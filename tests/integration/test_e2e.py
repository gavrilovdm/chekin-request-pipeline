"""End to end through the running stack: POST -> queue -> worker -> downstream -> done."""

import asyncio
import hashlib
import hmac
from uuid import uuid4

import httpx
import pytest

from tests.integration.conftest import API_URL, MOCK_URL

A = {"Authorization": "Bearer demo-key-a"}


@pytest.fixture
async def api():
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        yield client


@pytest.fixture
async def mock():
    async with httpx.AsyncClient(base_url=MOCK_URL, timeout=10) as client:
        await client.post("/admin/reset")
        await client.put("/admin/config", json={"latency_ms": 20, "jitter_ms": 10})
        yield client
        await client.post("/admin/reset")


async def submit(api, body=None) -> str:
    response = await api.post("/v1/requests", json=body or {"type": "guest.register", "data": {"name": "Ana"}},
                              headers=A | {"Idempotency-Key": f"e2e-{uuid4()}"})
    assert response.status_code == 202
    return response.json()["id"]


async def wait_for(api, rid, statuses=("done", "failed", "needs_human"), limit_s=20.0) -> dict:
    deadline = asyncio.get_running_loop().time() + limit_s
    while True:
        view = (await api.get(f"/v1/requests/{rid}", headers=A)).json()
        if view["status"] in statuses:
            return view
        assert asyncio.get_running_loop().time() < deadline, f"stuck in {view['status']}: {view}"
        await asyncio.sleep(0.2)


async def test_request_is_processed_to_done(api, mock):
    rid = await submit(api)

    view = await wait_for(api, rid)

    assert view["status"] == "done"
    assert view["attempt"] == 1
    assert view["result"]["ref"] == f"R-{rid[:8]}"


async def test_lost_publish_is_recovered_by_the_sweeper(api, mock, pool):
    """A row committed without any message (as if the publish failed) still reaches done."""
    rid = uuid4()
    await pool.execute(
        """INSERT INTO requests (id, tenant_id, idempotency_key, payload_hash, payload, status, created_at)
           VALUES ($1, 'tenant-a', $2, 'h', $3, 'received', now() - interval '2 minutes')""",
        rid, f"lost-{rid}", {"type": "guest.register", "data": {}},
    )

    view = await wait_for(api, str(rid))

    assert view["status"] == "done"


async def test_final_state_is_pushed_by_a_signed_webhook(api, mock):
    rid = await submit(api)
    await wait_for(api, rid)

    for _ in range(50):
        hooks = (await mock.get("/admin/webhooks", params={"request_id": rid})).json()
        if hooks:
            break
        await asyncio.sleep(0.2)
    assert len(hooks) == 1
    hook = hooks[0]
    expected = "sha256=" + hmac.new(b"demo-secret-a", hook["body"].encode(), hashlib.sha256).hexdigest()
    assert hook["signature"] == expected
    assert hook["json"]["status"] == "done" and hook["json"]["id"] == rid
