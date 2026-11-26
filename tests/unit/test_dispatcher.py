import asyncio
import hashlib
import hmac
import json
from datetime import timedelta
from uuid import uuid4

import httpx

from pipeline.dispatcher.sweeper import Sweeper
from pipeline.dispatcher.webhooks import WebhookSender, sign
from pipeline.domain.retry_policy import RetryPolicy
from pipeline.ports import WebhookJob
from tests.fakes import FakeClock

# --- sweeper -----------------------------------------------------------------


class FakeSweepStore:
    def __init__(self, due):
        self.due = due
        self.calls = []

    async def take_due(self, stale_after, limit, recover_lost):
        self.calls.append((stale_after, limit, recover_lost))
        return self.due


class FlakyPublisher:
    def __init__(self, fail_ids=()):
        self.fail_ids = set(fail_ids)
        self.published = []

    async def publish(self, request_id):
        if request_id in self.fail_ids:
            raise TimeoutError("no confirm")
        self.published.append(request_id)


async def test_sweeper_publishes_every_due_id():
    ids = [uuid4() for _ in range(3)]
    store = FakeSweepStore([(i, "received") for i in ids])
    publisher = FlakyPublisher()

    assert await Sweeper(store, publisher, timedelta(seconds=60), batch=500, queue_depth=depth(0)).sweep_once() == 3
    assert sorted(publisher.published) == sorted(ids)
    assert store.calls == [(timedelta(seconds=60), 500, True)]


async def test_sweeper_survives_a_failed_publish():
    ids = [uuid4() for _ in range(3)]
    publisher = FlakyPublisher(fail_ids={ids[1]})

    published = await Sweeper(FakeSweepStore([(i, "retry_scheduled") for i in ids]), publisher,
                              timedelta(seconds=60), batch=500, queue_depth=depth(0)).sweep_once()

    assert published == 2 and ids[1] not in publisher.published  # picked again after stale_after


def depth(value=None, error=None):
    async def read():
        if error:
            raise error
        return value

    return read


async def test_no_lost_message_recovery_while_the_queue_has_a_backlog():
    """Under a backlog an unclaimed request is still queued; re-publishing it only adds duplicates."""
    store = FakeSweepStore([])

    await Sweeper(store, FlakyPublisher(), timedelta(seconds=60), batch=500, queue_depth=depth(20_000)).sweep_once()
    await Sweeper(store, FlakyPublisher(), timedelta(seconds=60), batch=500,
                  queue_depth=depth(error=ConnectionError())).sweep_once()

    assert [call[2] for call in store.calls] == [False, False]


# --- webhooks ----------------------------------------------------------------


class FakeWebhookStore:
    def __init__(self, jobs):
        self.jobs = jobs
        self.delivered = []
        self.failed = []  # (id, next_at)

    async def claim_due(self, limit, visibility, per_tenant, exclude_tenants):
        taken, kept, per = [], [], {}
        for job in self.jobs:
            n = per.get(job.tenant_id, 0)
            if len(taken) < limit and job.tenant_id not in exclude_tenants and n < per_tenant:
                taken.append(job)
                per[job.tenant_id] = n + 1
            else:
                kept.append(job)
        self.jobs = kept
        return taken

    async def mark_delivered(self, request_id, webhook_attempt):
        self.delivered.append(request_id)

    async def mark_failed_attempt(self, request_id, webhook_attempt, next_at):
        self.failed.append((request_id, next_at))


def job(tenant="tenant-a", webhook_attempt=0, secret="s3cret"):
    return WebhookJob(uuid4(), tenant, "http://sink/hook", secret, "done", {"ref": "R-1"}, None, 1, webhook_attempt)


def sender(store, handler, clock=None, per_tenant=4, max_attempts=3):
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return WebhookSender(store, client, RetryPolicy(max_attempts=max_attempts, base_delay=1, max_delay=60),
                         per_tenant=per_tenant, batch=100, visibility=timedelta(seconds=60), timeout=5.0,
                         clock=clock or FakeClock())


async def test_webhook_is_signed_and_marked_delivered():
    seen = {}

    def handler(request):
        seen["sig"] = request.headers["X-Signature"]
        seen["body"] = request.content
        seen["rid"] = request.headers["X-Request-Id"]
        return httpx.Response(200)

    j = job()
    store = FakeWebhookStore([j])

    s = sender(store, handler)
    await s.send_due_once()
    await s.wait_idle()

    expected = "sha256=" + hmac.new(b"s3cret", seen["body"], hashlib.sha256).hexdigest()
    assert seen["sig"] == expected == sign("s3cret", seen["body"])
    assert json.loads(seen["body"]) == {"id": str(j.request_id), "status": "done", "result": {"ref": "R-1"},
                                        "error": None, "attempt": 1}
    assert seen["rid"] == str(j.request_id)
    assert store.delivered == [j.request_id]


async def test_failed_delivery_backs_off():
    clock = FakeClock()
    store = FakeWebhookStore([job(webhook_attempt=0)])

    s = sender(store, lambda r: httpx.Response(500), clock=clock)
    await s.send_due_once()
    await s.wait_idle()

    (_, next_at), = store.failed
    assert clock.now <= next_at <= clock.now + timedelta(seconds=1)


async def test_unreachable_endpoint_counts_as_failure():
    def handler(request):
        raise httpx.ConnectError("refused")

    store = FakeWebhookStore([job()])
    s = sender(store, handler)
    await s.send_due_once()
    await s.wait_idle()

    assert len(store.failed) == 1 and store.failed[0][1] is not None


async def test_malformed_url_is_a_counted_failure_not_a_crash():
    store = FakeWebhookStore([WebhookJob(uuid4(), "t", "http://[::1", "s", "done", {}, None, 1, 0)])

    s = sender(store, lambda r: httpx.Response(200))
    await s.send_due_once()
    await s.wait_idle()

    assert len(store.failed) == 1


def test_visibility_must_exceed_timeout():
    import pytest

    with pytest.raises(ValueError):
        WebhookSender(FakeWebhookStore([]), httpx.AsyncClient(), RetryPolicy(), per_tenant=1, batch=1,
                      visibility=timedelta(seconds=5), timeout=5.0)


async def test_last_attempt_gives_up():
    store = FakeWebhookStore([job(webhook_attempt=2)])

    s = sender(store, lambda r: httpx.Response(503), max_attempts=3)
    await s.send_due_once()
    await s.wait_idle()

    assert store.failed[0][1] is None  # webhook_status = failed


async def test_per_tenant_concurrency_is_capped():
    in_flight = {"slow": 0, "max_slow": 0}

    async def handler(request):
        if b"slow" in request.headers.get("X-Tenant", "").encode():
            in_flight["slow"] += 1
            in_flight["max_slow"] = max(in_flight["max_slow"], in_flight["slow"])
            await asyncio.sleep(0.02)
            in_flight["slow"] -= 1
        return httpx.Response(200)

    jobs = [job(tenant="slow") for _ in range(10)] + [job(tenant="fast") for _ in range(3)]
    store = FakeWebhookStore(jobs)

    await run_until_empty(sender(store, handler, per_tenant=2), store)

    assert in_flight["max_slow"] == 2
    assert len(store.delivered) == 13



async def run_until_empty(s, store, limit_s=5.0):
    stop = asyncio.Event()
    task = asyncio.create_task(s.run(stop, idle_interval=0.5))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + limit_s
    while (store.jobs or s._tasks) and loop.time() < deadline:
        await asyncio.sleep(0.005)
    stop.set()
    await task
    return loop.time() - (deadline - limit_s)


async def test_healthy_tenant_throughput_is_not_capped_by_the_idle_interval():
    """T12 regression: 4 per claim with a 0.5 s pause capped one tenant at 8 webhooks/s."""
    store = FakeWebhookStore([job() for _ in range(200)])

    took = await run_until_empty(sender(store, lambda r: httpx.Response(200), per_tenant=4), store)

    assert len(store.delivered) == 200
    assert took < 2.0  # the old design needed 25 s


async def test_slow_tenant_does_not_hold_back_a_fast_one():
    fast_done_at = {}

    async def handler(request):
        if request.headers["X-Tenant"] == "slow":
            await asyncio.sleep(1.0)
        else:
            fast_done_at.setdefault("first", asyncio.get_running_loop().time())
        return httpx.Response(200)

    store = FakeWebhookStore([job(tenant="slow") for _ in range(8)] + [job(tenant="fast") for _ in range(40)])
    s = sender(store, handler, per_tenant=4)
    stop = asyncio.Event()
    started = asyncio.get_running_loop().time()
    task = asyncio.create_task(s.run(stop, idle_interval=0.5))
    deadline = started + 5
    while sum(1 for _ in store.delivered) < 40 and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    fast_finished = asyncio.get_running_loop().time() - started
    stop.set()
    await task

    assert fast_finished < 0.5  # all 40 fast deliveries done while the slow tenant's first round still runs


async def test_in_flight_deliveries_never_exceed_the_batch():
    """Review finding: the claim asked for a full batch on top of the running tasks (up to 2 x batch - 1)."""
    gate = asyncio.Event()
    peak = {"now": 0, "max": 0}

    async def handler(request):
        peak["now"] += 1
        peak["max"] = max(peak["max"], peak["now"])
        await gate.wait()
        peak["now"] -= 1
        return httpx.Response(200)

    store = FakeWebhookStore([job(tenant=f"t{i}") for i in range(30)])
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    s = WebhookSender(store, client, RetryPolicy(), per_tenant=4, batch=10, visibility=timedelta(seconds=60),
                      timeout=5.0, clock=FakeClock())
    await s.send_due_once()
    await s.send_due_once()
    await asyncio.sleep(0.05)

    assert len(s._tasks) == 10 and peak["max"] <= 10
    gate.set()
    await s.wait_idle()
