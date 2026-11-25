"""Composition root: the only place where concrete adapters are created and wired to the core.

Entry points:
    uvicorn pipeline.main:api_app --factory   # intake API
    python -m pipeline worker                 # worker
"""

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

import aio_pika
import httpx
from fastapi import FastAPI

from pipeline.adapters.downstream import HttpProcessor
from pipeline.adapters.postgres import PgIntakeStore, PgRequestRepository, create_pool
from pipeline.adapters.rabbitmq import RabbitPublisher, declare_topology, open_channel
from pipeline.api.app import ApiDeps, create_app
from pipeline.api.auth import TenantCache
from pipeline.api.rate_limit import TenantRateLimiter
from pipeline.api.shedding import LoadShedder
from pipeline.config import Settings
from pipeline.domain.retry_policy import RetryPolicy
from pipeline.worker.consumer import Consumer
from pipeline.worker.handler import RequestHandler


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


async def run_worker(settings: Settings) -> None:
    """Worker process: consume request ids until SIGTERM, then drain gracefully."""
    pool = await create_pool(settings.database_url, settings.db_pool_size, settings.db_command_timeout_s)
    connection = await aio_pika.connect_robust(settings.amqp_url)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=settings.worker_concurrency)
    queue = await declare_topology(channel)
    client = httpx.AsyncClient(
        base_url=settings.downstream_url,
        timeout=settings.processing_timeout_s,
        limits=httpx.Limits(max_connections=settings.worker_concurrency),
    )
    handler = RequestHandler(
        PgRequestRepository(pool),
        HttpProcessor(client),
        RetryPolicy(settings.max_attempts, settings.retry_base_s, settings.retry_max_s),
        lease=timedelta(seconds=settings.lease_s),
        timeout=settings.processing_timeout_s,
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await Consumer(queue, handler, grace=settings.shutdown_grace_s).run(stop)
    finally:
        await connection.close()
        await client.aclose()
        await pool.close()
