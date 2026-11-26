"""Composition root: the only place where concrete adapters are created and wired to the core.

Entry points:
    uvicorn pipeline.main:api_app --factory   # intake API
    python -m pipeline worker                 # worker
    python -m pipeline dispatcher             # sweeper + webhooks
    python -m pipeline replay [--limit N] [--id UUID ...]   # operator: resume needs_human requests
"""

import asyncio
import contextlib
import signal
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import timedelta
from uuid import UUID

import aio_pika
import asyncpg
import httpx
import structlog
from fastapi import FastAPI
from prometheus_client import start_http_server

from pipeline.adapters.downstream import HttpProcessor
from pipeline.adapters.postgres import (
    PgDispatchStore,
    PgIntakeStore,
    PgOperatorStore,
    PgRequestRepository,
    create_pool,
)
from pipeline.adapters.rabbitmq import RabbitPublisher, declare_topology, open_channel
from pipeline.api.app import ApiDeps, create_app
from pipeline.api.auth import TenantCache
from pipeline.api.rate_limit import TenantRateLimiter
from pipeline.api.shedding import LoadShedder
from pipeline.config import Settings
from pipeline.dispatcher.gauges import refresh_gauges
from pipeline.dispatcher.sweeper import Sweeper
from pipeline.dispatcher.webhooks import WebhookSender
from pipeline.domain.retry_policy import RetryPolicy
from pipeline.observability import configure_logging
from pipeline.worker.consumer import Consumer
from pipeline.worker.handler import RequestHandler


def api_app() -> FastAPI:
    """Intake API with Postgres, RabbitMQ and the load shedder opened for the app's lifetime."""
    settings = Settings()
    configure_logging(settings.log_level)

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
            readiness=lambda: _readiness(pool, connection),
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


async def _readiness(pool: asyncpg.Pool, connection: aio_pika.abc.AbstractRobustConnection) -> dict[str, bool]:
    try:
        postgres = await asyncio.wait_for(pool.fetchval("SELECT 1"), timeout=1.0) == 1
    except Exception:
        postgres = False
    return {"postgres": postgres, "rabbitmq": not connection.is_closed}


async def run_worker(settings: Settings) -> None:
    """Worker process: consume request ids until SIGTERM, then drain gracefully."""
    configure_logging(settings.log_level)
    start_http_server(settings.metrics_port)
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
    stop = _stop_on_signals()
    try:
        await Consumer(queue, handler, grace=settings.shutdown_grace_s).run(stop)
    finally:
        await connection.close()
        await client.aclose()
        await pool.close()


async def run_dispatcher(settings: Settings) -> None:
    """Dispatcher process: sweeper, webhook sender and health gauges until SIGTERM."""
    configure_logging(settings.log_level)
    start_http_server(settings.metrics_port)
    pool = await create_pool(settings.database_url, settings.db_pool_size, settings.db_command_timeout_s)
    connection = await aio_pika.connect_robust(settings.amqp_url)
    channel = await open_channel(connection)
    await declare_topology(channel)
    store = PgDispatchStore(pool)
    publisher = RabbitPublisher(channel, settings.publish_timeout_s)
    sweeper = Sweeper(
        store,
        publisher,
        stale_after=timedelta(seconds=settings.stale_after_s),
        batch=settings.sweep_batch,
        queue_depth=publisher.queue_depth,
    )
    client = httpx.AsyncClient(limits=httpx.Limits(max_connections=200))
    sender = WebhookSender(
        store,
        client,
        RetryPolicy(settings.webhook_max_attempts, settings.retry_base_s, settings.retry_max_s),
        per_tenant=settings.webhook_per_tenant_concurrency,
        batch=settings.sweep_batch,
        visibility=timedelta(seconds=settings.webhook_timeout_s * 4),
        timeout=settings.webhook_timeout_s,
    )
    stop = _stop_on_signals()
    try:
        await asyncio.gather(
            _every(settings.sweep_interval_s, sweeper.sweep_once, settings.sweep_batch, stop, "sweeper"),
            sender.run(stop, idle_interval=settings.sweep_interval_s / 2),
            _every(10.0, lambda: refresh_gauges(store, publisher.queue_depth), 1, stop, "gauges"),
        )
    finally:
        await connection.close()
        await client.aclose()
        await pool.close()


def _stop_on_signals() -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    return stop


async def _every(
    interval: float, step: Callable[[], Awaitable[int]], batch: int, stop: asyncio.Event, name: str
) -> None:
    """Run ``step`` until ``stop``; a full batch means more is waiting, so go again without sleeping."""
    log = structlog.get_logger(name)
    while not stop.is_set():
        try:
            if await step() >= batch:
                continue
        except Exception:
            log.exception("loop.step_failed")
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval)


async def run_replay(settings: Settings, limit: int, ids: list[UUID] | None) -> list[UUID]:
    """Operator command: give ``needs_human`` requests a fresh retry budget; the sweeper publishes them."""
    pool = await create_pool(settings.database_url, 2, settings.db_command_timeout_s)
    try:
        return await PgOperatorStore(pool).replay_needs_human(limit, ids)
    finally:
        await pool.close()
