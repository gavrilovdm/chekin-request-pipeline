"""Fake downstream service and webhook sink, tunable at runtime for failure and load scenarios.

POST /process            the "external system": honours Idempotency-Key, injects latency and errors
POST /webhooks/{tenant}  records webhooks the dispatcher sends, with their signature header
PUT  /admin/config       change latency, error rates, "down", "webhooks_down" and fail_first on the fly
GET  /admin/stats        calls, successes, duplicate calls after success
GET  /admin/calls/{key}  calls and success for one Idempotency-Key, plus this process's boot id
GET  /admin/webhooks     received webhooks (optionally ?request_id=)
POST /admin/reset        clear counters and stores
"""

import asyncio
import random
import uuid
from typing import Any

from fastapi import FastAPI, Header, Request, Response
from pydantic import BaseModel


class Config(BaseModel):
    """Behaviour of /process."""

    latency_ms: float = 200
    jitter_ms: float = 100
    error_rate: float = 0.0  # share of calls answered 503 (retryable)
    permanent_rate: float = 0.0  # share of calls answered 422 (permanent)
    down: bool = False  # answer every call with 503
    webhooks_down: bool = False  # the webhook sink answers 503 (a tenant endpoint that is down)
    fail_first: int = 0  # each Idempotency-Key gets 503 on its first N calls, then succeeds (deterministic flakiness)


app = FastAPI(title="downstream-mock")
config = Config()
results: dict[str, dict[str, Any]] = {}
stats = {"calls": 0, "successes": 0, "duplicate_calls_after_success": 0, "errors": 0}
webhooks: list[dict[str, Any]] = []
locks: dict[str, asyncio.Lock] = {}
calls_per_key: dict[str, int] = {}
BOOT_ID = str(uuid.uuid4())  # changes if the process restarts: tests can tell lost counters from real behaviour


@app.post("/process", response_model=None)
async def process(body: dict[str, Any], idempotency_key: str = Header(...)) -> Response | dict[str, Any]:
    """Pretend to do the work; the same Idempotency-Key always gets the same result."""
    stats["calls"] += 1
    calls_per_key[idempotency_key] = calls_per_key.get(idempotency_key, 0) + 1
    async with locks.setdefault(idempotency_key, asyncio.Lock()):  # concurrent calls with one key: one effect
        if idempotency_key in results:
            stats["duplicate_calls_after_success"] += 1
            return results[idempotency_key]
        return await _attempt(body, idempotency_key)


async def _attempt(body: dict[str, Any], idempotency_key: str) -> Response | dict[str, Any]:
    await asyncio.sleep(max(0.0, random.gauss(config.latency_ms, config.jitter_ms / 2)) / 1000)
    roll = random.random()
    if config.down or calls_per_key[idempotency_key] <= config.fail_first or roll < config.error_rate:
        stats["errors"] += 1
        return Response("temporarily unavailable", status_code=503)
    if roll < config.error_rate + config.permanent_rate:
        stats["errors"] += 1
        return Response("document number rejected", status_code=422)
    result = {"ref": f"R-{idempotency_key[:8]}", "tenant_id": body.get("tenant_id")}
    results[idempotency_key] = result
    stats["successes"] += 1
    return result


@app.post("/webhooks/{tenant}", response_model=None)
async def receive_webhook(tenant: str, request: Request) -> Response | dict[str, str]:
    """Record a webhook so tests can check delivery and signature."""
    if config.webhooks_down:
        return Response("tenant endpoint down", status_code=503)
    webhooks.append({
        "tenant": tenant,
        "signature": request.headers.get("X-Signature"),
        "body": (await request.body()).decode(),
        "json": await request.json(),
    })
    return {"status": "ok"}


@app.put("/admin/config")
async def set_config(new: Config) -> Config:
    """Replace the behaviour of /process."""
    global config
    config = new
    return config


@app.get("/admin/stats")
async def get_stats() -> dict[str, int]:
    """Counters since the last reset."""
    return stats


@app.get("/admin/calls/{key}")
async def calls_for(key: str) -> dict[str, Any]:
    """How many times one Idempotency-Key was called, and whether it has a stored result."""
    return {"calls": calls_per_key.get(key, 0), "succeeded": key in results, "boot_id": BOOT_ID}


@app.get("/admin/webhooks")
async def get_webhooks(request_id: str | None = None) -> list[dict[str, Any]]:
    """Webhooks received, optionally for one request."""
    return [w for w in webhooks if request_id is None or w["json"].get("id") == request_id]


@app.post("/admin/reset")
async def reset() -> dict[str, str]:
    """Clear counters, stored results and webhooks; restore the default config."""
    global config
    config = Config()
    results.clear()
    webhooks.clear()
    locks.clear()
    calls_per_key.clear()
    for key in stats:
        stats[key] = 0
    return {"status": "reset"}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    """Liveness."""
    return {"status": "ok"}
