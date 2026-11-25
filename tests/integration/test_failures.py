"""Failure scenarios against the live stack (`make test-chaos`). Each test restores what it breaks.

Invariant across all of them: every accepted request reaches a final state, and the downstream applies at most
one effect per request id.
"""

import asyncio
import subprocess
from uuid import uuid4

import httpx
import pytest

from pipeline.config import Settings
from tests.integration.conftest import MOCK_URL
from tests.integration.helpers import A, submit, wait_for

pytestmark = pytest.mark.chaos

MAX_ATTEMPTS = Settings().max_attempts  # PIPELINE_MAX_ATTEMPTS, same source as the services


def compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True, timeout=180)


async def calls_for(mock, rid: str) -> dict:
    return (await mock.get(f"/admin/calls/{rid}")).json()


async def wait_until(predicate, limit_s: float, step: float = 0.2):
    deadline = asyncio.get_running_loop().time() + limit_s
    while not await predicate():
        assert asyncio.get_running_loop().time() < deadline, "condition not reached in time"
        await asyncio.sleep(step)


async def test_worker_killed_mid_processing_still_finishes(api, mock, pool):
    await mock.put("/admin/config", json={"latency_ms": 3000, "jitter_ms": 0})
    rid = await submit(api)

    async def processing():
        return await pool.fetchval("SELECT status FROM requests WHERE id = $1::uuid", rid) == "processing"

    await wait_until(processing, 10)
    try:
        compose("kill", "worker")
        await mock.put("/admin/config", json={"latency_ms": 20, "jitter_ms": 0})
    finally:
        compose("up", "-d", "--wait", "worker")

    view = await wait_for(api, rid, limit_s=60)  # lease (15 s) expires, the sweeper re-publishes

    assert view["status"] == "done" and view["attempt"] == 2
    # Both attempts used the request id as Idempotency-Key (a per-attempt key would show calls == 1 here), so the
    # downstream stored one result for two calls. A global success count is not used: other tests' requests
    # recovered by the sweeper also reach the mock while this one runs.
    assert (await calls_for(mock, rid)) == {"calls": 2, "succeeded": True}


async def test_flaky_downstream_is_absorbed_by_retries(api, mock):
    await mock.put("/admin/config", json={"latency_ms": 20, "jitter_ms": 0, "fail_first": 2})
    rid = await submit(api)

    view = await wait_for(api, rid, limit_s=30)

    assert view["status"] == "done" and view["attempt"] == 3
    assert (await calls_for(mock, rid)) == {"calls": 3, "succeeded": True}


async def test_downstream_down_ends_in_needs_human_after_max_attempts(api, mock):
    await mock.put("/admin/config", json={"latency_ms": 5, "jitter_ms": 0, "down": True})
    rid = await submit(api)

    view = await wait_for(api, rid, limit_s=60)

    assert view["status"] == "needs_human"
    assert view["attempt"] == MAX_ATTEMPTS
    assert "503" in view["last_error"]
    assert (await calls_for(mock, rid))["calls"] == MAX_ATTEMPTS


async def test_broker_down_at_intake_is_recovered_without_client_retry(api, mock, pool):
    compose("stop", "rabbitmq")
    try:
        response = await api.post("/v1/requests", json={"type": "guest.register", "data": {}},
                                  headers=A | {"Idempotency-Key": f"chaos-{uuid4()}"})
        assert response.status_code == 202
        rid = response.json()["id"]
        assert await pool.fetchval("SELECT status FROM requests WHERE id = $1::uuid", rid) == "received"
    finally:
        compose("start", "rabbitmq")
        compose("up", "-d", "--wait", "rabbitmq")

    view = await wait_for(api, rid, limit_s=120)

    assert view["status"] == "done"
    enqueued = await pool.fetchval("SELECT enqueued_at FROM requests WHERE id = $1::uuid", rid)
    assert enqueued is not None  # it got to the queue through the sweeper, not the API


async def test_client_replay_creates_one_request_and_one_effect(api, mock, pool):
    key = f"replay-{uuid4()}"
    body = {"type": "guest.register", "data": {"name": "Ana"}}

    ids = {(await api.post("/v1/requests", json=body, headers=A | {"Idempotency-Key": key})).json()["id"]
           for _ in range(3)}
    (rid,) = ids
    await wait_for(api, rid)

    assert await pool.fetchval("SELECT count(*) FROM requests WHERE idempotency_key = $1", key) == 1
    assert (await calls_for(mock, rid)) == {"calls": 1, "succeeded": True}


@pytest.fixture(autouse=True)
async def _stack_is_whole():
    """If a scenario failed half-way, bring every service back before the next one.

    Note: `up` restores the compose file's scale (1 worker); re-run `make scale` after chaos tests if needed.
    """
    yield
    compose("up", "-d", "--wait")
    async with httpx.AsyncClient() as client:
        await client.post(f"{MOCK_URL}/admin/reset")
