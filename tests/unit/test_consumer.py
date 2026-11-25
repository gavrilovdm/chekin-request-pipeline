import asyncio
from uuid import uuid4

from pipeline.worker.consumer import Consumer
from pipeline.worker.handler import HandleResult


class FakeMessage:
    def __init__(self, body: bytes):
        self.body = body
        self.acked = self.rejected = False

    async def ack(self):
        self.acked = True

    async def reject(self, requeue: bool = False):
        assert requeue is False
        self.rejected = True


class FakeQueue:
    def __init__(self):
        self.callback = None
        self.cancelled = False

    async def consume(self, callback):
        self.callback = callback
        return "tag-1"

    async def cancel(self, tag):
        assert tag == "tag-1"
        self.cancelled = True


class FakeHandler:
    def __init__(self, delay=0.0, error=None):
        self.delay, self.error = delay, error
        self.seen = []

    async def handle(self, request_id):
        self.seen.append(request_id)
        await asyncio.sleep(self.delay)
        if self.error:
            raise self.error
        return HandleResult.DONE


async def start(handler, grace=1.0):
    queue, stop = FakeQueue(), asyncio.Event()
    consumer = Consumer(queue, handler, grace=grace)
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.sleep(0)
    return queue, stop, task


async def deliver(queue, message):
    await queue.callback(message)
    await asyncio.sleep(0.01)


async def test_valid_id_is_handled_and_acked():
    handler = FakeHandler()
    queue, stop, task = await start(handler)
    rid = uuid4()
    message = FakeMessage(str(rid).encode())

    await deliver(queue, message)
    stop.set()
    await task

    assert handler.seen == [rid] and message.acked


async def test_unparseable_body_is_rejected_to_dead_letter():
    handler = FakeHandler()
    queue, stop, task = await start(handler)
    message = FakeMessage(b"not-a-uuid")

    await deliver(queue, message)
    stop.set()
    await task

    assert message.rejected and not message.acked and handler.seen == []


async def test_handler_crash_still_acks_because_the_sweeper_owns_recovery():
    queue, stop, task = await start(FakeHandler(error=ConnectionError("db down")))
    message = FakeMessage(str(uuid4()).encode())

    await deliver(queue, message)
    stop.set()
    await task

    assert message.acked


async def test_shutdown_stops_consuming_and_waits_for_in_flight_work():
    queue, stop, task = await start(FakeHandler(delay=0.1), grace=1.0)
    message = FakeMessage(str(uuid4()).encode())

    await deliver(queue, message)
    stop.set()
    await task

    assert queue.cancelled and message.acked


async def test_work_past_the_grace_period_is_cancelled_and_left_unacked():
    queue, stop, task = await start(FakeHandler(delay=5), grace=0.05)
    message = FakeMessage(str(uuid4()).encode())

    await deliver(queue, message)
    stop.set()
    await task

    assert not message.acked  # RabbitMQ re-queues it when the channel closes
