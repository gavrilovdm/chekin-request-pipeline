"""Topology and publisher against a real RabbitMQ."""

import asyncio
from uuid import uuid4

import aio_pika
import pytest

from pipeline.adapters.rabbitmq import DEAD_QUEUE, RabbitPublisher, declare_topology, open_channel
from tests.integration.conftest import AMQP_URL


@pytest.fixture
async def channel():
    connection = await aio_pika.connect_robust(AMQP_URL)
    channel = await open_channel(connection)
    yield channel
    await connection.close()


async def drain(queue):
    while await queue.get(fail=False, no_ack=True):
        pass


async def test_publish_then_consume_carries_only_the_id(channel):
    queue = await declare_topology(channel)
    await drain(queue)
    rid = uuid4()

    await RabbitPublisher(channel, timeout=2.0).publish(rid)
    message = await queue.get(timeout=5)

    assert message.body.decode() == str(rid)
    assert message.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    await message.ack()


async def test_queue_depth_is_reported(channel):
    queue = await declare_topology(channel)
    await drain(queue)
    publisher = RabbitPublisher(channel, timeout=2.0)
    for _ in range(3):
        await publisher.publish(uuid4())

    assert await publisher.queue_depth() == 3
    await drain(queue)


async def test_rejected_message_goes_to_dead_letter_queue(channel):
    queue = await declare_topology(channel)
    await drain(queue)
    dead = await channel.get_queue(DEAD_QUEUE)
    await drain(dead)

    await channel.default_exchange.publish(aio_pika.Message(b"not-a-uuid"), routing_key=queue.name)
    message = await queue.get(timeout=5)
    await message.reject(requeue=False)

    for _ in range(50):
        dead_message = await dead.get(fail=False, no_ack=True)
        if dead_message:
            break
        await asyncio.sleep(0.1)
    assert dead_message is not None and dead_message.body == b"not-a-uuid"


async def test_unroutable_publish_raises(channel):
    """If the queue is missing, publish must fail so the API leaves the request to the sweeper."""
    await declare_topology(channel)
    publisher = RabbitPublisher(channel, timeout=2.0, routing_key=f"missing-{uuid4()}")

    with pytest.raises(aio_pika.exceptions.DeliveryError):
        await publisher.publish(uuid4())
