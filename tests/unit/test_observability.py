from uuid import uuid4

import httpx
from prometheus_client import REGISTRY

from pipeline.api.app import ApiDeps, create_app
from pipeline.api.auth import TenantCache
from pipeline.api.rate_limit import TenantRateLimiter
from tests.unit.test_api import FakePublisher, FakeStore, FixedShedder, post
from tests.unit.test_consumer import FakeHandler, FakeMessage, deliver, start


def value(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def client(shedder=None, readiness=None):
    store = FakeStore()
    deps = ApiDeps(store=store, publisher=FakePublisher(), tenants=TenantCache(store, ttl=60),
                   limiter=TenantRateLimiter(), shedder=shedder or FixedShedder(), readiness=readiness)
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(deps)), base_url="http://test")


async def test_intake_outcomes_are_counted():
    before = {o: value("pipeline_intake_total", outcome=o) for o in ("accepted", "replayed", "unauthorized")}
    async with client() as api:
        await post(api, idem="obs-1")
        await post(api, idem="obs-1")
        await post(api, key="wrong", idem="obs-2")

    assert value("pipeline_intake_total", outcome="accepted") == before["accepted"] + 1
    assert value("pipeline_intake_total", outcome="replayed") == before["replayed"] + 1
    assert value("pipeline_intake_total", outcome="unauthorized") == before["unauthorized"] + 1


async def test_shed_requests_are_counted():
    before = value("pipeline_intake_total", outcome="shed")
    async with client(shedder=FixedShedder(retry_after=5)) as api:
        await post(api, idem="obs-3")

    assert value("pipeline_intake_total", outcome="shed") == before + 1


async def test_metrics_endpoint_exposes_series():
    async with client() as api:
        await post(api, idem="obs-4")
        text = (await api.get("/metrics")).text

    assert "pipeline_intake_total" in text and "pipeline_intake_seconds_bucket" in text


async def test_readiness_reports_each_dependency():
    async def healthy():
        return {"postgres": True, "rabbitmq": True}

    async def db_down():
        return {"postgres": False, "rabbitmq": True}

    async with client(readiness=healthy) as api:
        ok = await api.get("/readyz")
    async with client(readiness=db_down) as api:
        bad = await api.get("/readyz")

    assert ok.status_code == 200
    assert bad.status_code == 503 and bad.json() == {"postgres": False, "rabbitmq": True}


async def test_consumer_counts_handled_by_result():
    before = value("pipeline_handled_total", result="done")
    queue, stop, task = await start(FakeHandler())

    await deliver(queue, FakeMessage(str(uuid4()).encode()))
    stop.set()
    await task

    assert value("pipeline_handled_total", result="done") == before + 1
    assert value("pipeline_in_flight") == 0
