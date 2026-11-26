"""Invariant checker for one run window against the live stack (Postgres + downstream mock + Docker).

Used by loadtest/chaos_campaign.py after the faults stop and the backlog drains. Each check returns a list of
violations (empty = holds). See docs/tasks/T12-fault-campaign.md for the invariants I1-I6.
"""

import asyncio
import json
import subprocess
from datetime import datetime
from typing import Any

import asyncpg
import httpx

FINAL = ("done", "failed", "needs_human")


async def i1_every_request_is_final(conn: asyncpg.Connection, since: datetime) -> list[str]:
    rows = await conn.fetch(
        "SELECT id, status, attempt, updated_at FROM requests WHERE created_at >= $1 AND status <> ALL($2::text[])",
        since, list(FINAL),
    )
    return [f"I1 {r['id']} stuck in {r['status']} (attempt {r['attempt']}, since {r['updated_at']:%H:%M:%S})"
            for r in rows]


async def i2_one_effect_per_request(conn: asyncpg.Connection, since: datetime) -> list[str]:
    """The mock's result is derived from the Idempotency-Key, so a result from another key means a second effect."""
    rows = await conn.fetch("SELECT id, result FROM requests WHERE created_at >= $1 AND status = 'done'", since)
    return [f"I2 {r['id']} result {r['result']} was not produced under its own request id"
            for r in rows if (r["result"] or {}).get("ref") != f"R-{str(r['id'])[:8]}"]


async def i3_records_are_complete(conn: asyncpg.Connection, since: datetime) -> list[str]:
    rows = await conn.fetch(
        """SELECT id, status FROM requests WHERE created_at >= $1
             AND ((status = 'done' AND result IS NULL)
               OR (status IN ('failed', 'needs_human') AND last_error IS NULL))""",
        since,
    )
    return [f"I3 {r['id']} {r['status']} without its result/error" for r in rows]


async def i4_webhooks_settle(conn: asyncpg.Connection, since: datetime, mock: httpx.AsyncClient) -> list[str]:
    rows = await conn.fetch(
        """SELECT r.id, r.status, r.webhook_pending, r.webhook_status, t.webhook_url IS NOT NULL AS has_url
             FROM requests r JOIN tenants t ON t.id = r.tenant_id
            WHERE r.created_at >= $1 AND r.status = ANY($2::text[])""",
        since, list(FINAL),
    )
    problems = [f"I4 {r['id']} webhook still pending" for r in rows if r["webhook_pending"]]
    problems += [f"I4 {r['id']} final with a webhook URL but webhook_status={r['webhook_status']}"
                 for r in rows if r["has_url"] and not r["webhook_pending"] and r["webhook_status"] is None]
    received = {w["json"]["id"] for w in (await mock.get("/admin/webhooks")).json()}
    problems += [f"I4 {r['id']} marked delivered but the sink never received it"
                 for r in rows if r["webhook_status"] == "delivered" and str(r["id"]) not in received]
    return problems


async def i5_services_healthy(api: httpx.AsyncClient, amqp_api: str) -> list[str]:
    problems = []
    ready = await api.get("/readyz")
    if ready.status_code != 200:
        problems.append(f"I5 api not ready: {ready.text}")
    # "Drained" is eventual: right after a broker restart or a replay, fresh messages are still being consumed.
    async with httpx.AsyncClient(auth=("guest", "guest")) as mq:
        for _ in range(30):
            queue = (await mq.get(f"{amqp_api}/api/queues/%2F/requests")).json()
            if not queue.get("messages", 0) and queue.get("consumers", 0):
                break
            await asyncio.sleep(2)
    if queue.get("messages", 0) or not queue.get("consumers", 0):
        problems.append(f"I5 work queue not drained after 60 s: {queue.get('messages')} messages, "
                        f"{queue.get('consumers')} consumers")
    ps = ["docker", "compose", "ps", "-a", "--format", "json"]
    raw = (await asyncio.to_thread(subprocess.run, ps, capture_output=True, text=True)).stdout
    containers = json.loads(raw) if raw.strip().startswith("[") else [json.loads(x) for x in raw.splitlines() if x]
    problems += [f"I5 container not running: {c['Name']} {c['State']}" for c in containers if c["State"] != "running"]
    return problems


def i6_no_illegal_transitions(since: datetime) -> list[str]:
    logs = subprocess.run(["docker", "compose", "logs", "--since", since.strftime("%Y-%m-%dT%H:%M:%SZ")],
                          capture_output=True, text=True).stdout
    markers = ("IllegalTransition", "Task exception was never retrieved")
    return [f"I6 {line[:200]}" for line in logs.splitlines() if any(m in line for m in markers)]


async def check_all(dsn: str, api_url: str, mock_url: str, amqp_api: str, since: datetime) -> dict[str, Any]:
    """Run every invariant; returns {"violations": [...], "counts": {...}}."""
    conn = await asyncpg.connect(dsn)
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")
    try:
        async with httpx.AsyncClient(base_url=api_url, timeout=10) as api, \
                httpx.AsyncClient(base_url=mock_url, timeout=10) as mock:
            violations = (
                await i1_every_request_is_final(conn, since)
                + await i2_one_effect_per_request(conn, since)
                + await i3_records_are_complete(conn, since)
                + await i4_webhooks_settle(conn, since, mock)
                + await i5_services_healthy(api, amqp_api)
                + i6_no_illegal_transitions(since)
            )
        counts = dict(await conn.fetch(
            "SELECT status, count(*) FROM requests WHERE created_at >= $1 GROUP BY status", since))
        webhooks = dict(await conn.fetch(
            "SELECT coalesce(webhook_status, 'none'), count(*) FROM requests WHERE created_at >= $1 GROUP BY 1",
            since))
    finally:
        await conn.close()
    return {"violations": violations, "counts": counts, "webhooks": webhooks}
