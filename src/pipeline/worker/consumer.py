"""RabbitMQ transport for the worker: turns deliveries into ``RequestHandler.handle`` calls, with graceful shutdown."""

import asyncio
from typing import Any, Protocol
from uuid import UUID

import structlog

from pipeline.worker.handler import HandleResult

log = structlog.get_logger(__name__)


class Handler(Protocol):
    """What the consumer needs from the handler."""

    async def handle(self, request_id: UUID) -> HandleResult:
        """Process one request id."""
        ...


class Consumer:
    """Consumes request ids. Concurrency is bounded by the channel's prefetch (unacked deliveries).

    Every delivery is acked after the handler returns, whatever the outcome, because the request's state lives
    in Postgres and the sweeper repairs anything a lost or acked message leaves behind. Unparseable bodies are
    rejected to the dead-letter queue.
    """

    def __init__(self, queue: Any, handler: Handler, grace: float) -> None:
        self._queue = queue
        self._handler = handler
        self._grace = grace
        self._tasks: set[asyncio.Task[None]] = set()

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until ``stop`` is set, then drain in-flight work for up to ``grace`` seconds."""
        tag = await self._queue.consume(self._on_message)
        log.info("consumer.started")
        await stop.wait()
        await self._queue.cancel(tag)
        log.info("consumer.draining", in_flight=len(self._tasks))
        if self._tasks:
            _done, pending = await asyncio.wait(self._tasks, timeout=self._grace)
            for task in pending:  # unacked: RabbitMQ re-queues them when the channel closes
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
        log.info("consumer.stopped")

    async def _on_message(self, message: Any) -> None:
        task = asyncio.create_task(self._process(message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _process(self, message: Any) -> None:
        try:
            request_id = UUID(message.body.decode())
        except (ValueError, UnicodeDecodeError):
            log.error("consumer.poison_message", body=message.body[:100])
            await message.reject(requeue=False)
            return
        try:
            await self._handler.handle(request_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("consumer.handler_failed", request_id=str(request_id))
        try:
            await message.ack()
        except Exception:  # e.g. the channel was re-opened after a broker restart; a redelivery is harmless
            log.exception("consumer.ack_failed", request_id=str(request_id))
