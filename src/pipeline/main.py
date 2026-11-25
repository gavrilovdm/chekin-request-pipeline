"""Composition root: the only place where concrete adapters are created and wired to the core.

Entry points:
    uvicorn pipeline.main:api_app --factory   # intake API
"""

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import aio_pika
from fastapi import FastAPI

from pipeline.adapters.postgres import PgIntakeStore, create_pool
from pipeline.adapters.rabbitmq import RabbitPublisher, declare_topology, open_channel
from pipeline.api.app import ApiDeps, create_app
from pipeline.api.auth import TenantCache
from pipeline.api.rate_limit import TenantRateLimiter
from pipeline.api.shedding import LoadShedder
from pipeline.config import Settings


def api_app() -> FastAPI:
    """Intake API with Postgres, RabbitMQ and the load shedder opened for the app's lifetime."""
    settings = Settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        pool = await create_pool(settings.database_url, settings.db_pool_size, settings.db_command_timeout_s)
        connection = await aio_pika.connect_robust(settings.amqp_url)
        channel = await open_channel(connection)
        await declare_topology(channel)
        publisher = RabbitPublisher(channel, settings.publish_timeout_s)
        shedder = LoadShedder(publisher.queue_depth, settings.max_queue_depth)
        store = PgIntakeStore(pool)
        app.state.deps = ApiDeps(
            store=store,
            publisher=publisher,
            tenants=TenantCache(store, settings.tenant_cache_ttl_s),
            limiter=TenantRateLimiter(),
            shedder=shedder,
            publish_timeout=settings.publish_timeout_s,
        )
        shedding = asyncio.create_task(shedder.run())
        try:
            yield
        finally:
            shedding.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await shedding
            await connection.close()
            await pool.close()

    return create_app(lifespan=lifespan)
