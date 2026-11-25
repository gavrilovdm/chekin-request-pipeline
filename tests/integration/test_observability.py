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


def gauge(text: str, name: str) -> float:
    line = next(line for line in text.splitlines() if line.startswith(name + " "))
    return float(line.split()[1])


async def test_dispatcher_gauges_reflect_the_database(pool):
    """Real values, not the default zeros: an hour-old waiting retry and a request that needs a human."""
    waiting, stuck = uuid4(), uuid4()
    rows = [(waiting, "retry_scheduled", "now() + interval '1 hour'"), (stuck, "needs_human", "NULL")]
    for rid, status, next_at in rows:
        await pool.execute(
            f"""INSERT INTO requests (id, tenant_id, idempotency_key, payload_hash, payload, status, attempt,
                                     next_attempt_at, created_at)
                VALUES ($1, 'tenant-b', $2, 'h', '{{}}', $3, 1, {next_at}, now() - interval '1 hour')""",
            rid, f"gauge-{rid}", status,
        )
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            for _ in range(60):  # gauges refresh every 10 s
                text = (await client.get(DISPATCHER_METRICS)).text
                oldest, needs_human = gauge(text, "pipeline_oldest_unfinished_seconds"), gauge(text, "pipeline_needs_human")
                if oldest >= 3500 and needs_human >= 1:
                    break
                await asyncio.sleep(0.5)
        assert gauge(text, "pipeline_oldest_unfinished_seconds") >= 3500
        assert gauge(text, "pipeline_needs_human") >= 1
    finally:
        await pool.execute("UPDATE requests SET status = 'done' WHERE id = ANY($1::uuid[])", [waiting, stuck])
