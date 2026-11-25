"""RabbitMQ topology and publisher. Messages carry only a request id; Postgres holds the state."""

import asyncio
from uuid import UUID

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractQueue

QUEUE = "requests"


async def open_channel(connection: aio_pika.abc.AbstractRobustConnection) -> AbstractChannel:
    """A channel where publishes are confirmed and an unroutable message raises instead of passing silently."""
    return await connection.channel(publisher_confirms=True, on_return_raises=True)


async def declare_topology(channel: AbstractChannel, queue: str = QUEUE) -> AbstractQueue:
    """Declare the work queue and its dead-letter queue. Idempotent; every service calls it on start.

    ``requests`` is a quorum queue: replicated across nodes in a cluster and durable on disk. Messages a consumer
    rejects (unparseable bodies) are routed to ``requests.dead`` for inspection instead of looping.
    """
    dead_exchange, dead_queue = dead_letter_names(queue)
    dlx = await channel.declare_exchange(dead_exchange, aio_pika.ExchangeType.FANOUT, durable=True)
    dead = await channel.declare_queue(dead_queue, durable=True, arguments={"x-queue-type": "quorum"})
    await dead.bind(dlx)
    return await channel.declare_queue(
        queue,
        durable=True,
        arguments={"x-queue-type": "quorum", "x-dead-letter-exchange": dead_exchange},
    )


def dead_letter_names(queue: str) -> tuple[str, str]:
    """Dead-letter exchange and queue for a work queue (``requests`` -> ``requests.dlx``, ``requests.dead``)."""
    return f"{queue}.dlx", f"{queue}.dead"


class RabbitPublisher:
    """``Publisher`` with persistent messages and publisher confirms.

    ``publish`` returns only after the broker confirmed the message; an unroutable message (``mandatory``) or a
    missing confirm within ``timeout`` raises, so the caller knows the sweeper must pick the request up later.
    """

    def __init__(self, channel: AbstractChannel, timeout: float, routing_key: str = QUEUE) -> None:
        self._channel = channel
        self._timeout = timeout
        self._routing_key = routing_key

    async def publish(self, request_id: UUID) -> None:
        """Publish one request id and wait for the confirm."""
        message = aio_pika.Message(
            str(request_id).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=str(request_id),
            content_type="text/plain",
        )
        await self._channel.default_exchange.publish(
            message, routing_key=self._routing_key, mandatory=True, timeout=self._timeout
        )

    async def queue_depth(self) -> int:
        """Messages ready in the work queue (not counting unacked ones in flight)."""
        # A raw passive declare: aio-pika's robust channel caches the declaration result, so its count goes stale.
        channel = await asyncio.wait_for(self._channel.get_underlay_channel(), timeout=self._timeout)
        result = await channel.queue_declare(self._routing_key, passive=True, timeout=self._timeout)
        return result.message_count or 0
