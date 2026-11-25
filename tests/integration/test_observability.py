"""Metrics and readiness on the live stack."""

import asyncio
from uuid import uuid4

import httpx

from tests.integration.conftest import API_URL

DISPATCHER_METRICS = "http://localhost:9101/metrics"


async def test_api_exposes_intake_metrics_and_is_ready():
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as api:
        await api.post("/v1/requests", json={"type": "t", "data": {}},
                       headers={"Authorization": "Bearer demo-key-a", "Idempotency-Key": f"m-{uuid4()}"})
        metrics = (await api.get("/metrics")).text
        ready = await api.get("/readyz")

    assert 'pipeline_intake_total{outcome="accepted"}' in metrics
    assert ready.status_code == 200 and ready.json() == {"postgres": True, "rabbitmq": True}


async def test_dispatcher_exposes_health_gauges():
    async with httpx.AsyncClient(timeout=10) as client:
        for _ in range(30):  # gauges refresh every 10 s
            text = (await client.get(DISPATCHER_METRICS)).text
            if "pipeline_oldest_unfinished_seconds " in text and "pipeline_queue_depth " in text:
                break
            await asyncio.sleep(0.5)

    for series in ("pipeline_needs_human", "pipeline_oldest_unfinished_seconds", "pipeline_webhooks_pending",
                   "pipeline_queue_depth", "pipeline_republished_total"):
        assert series in text
