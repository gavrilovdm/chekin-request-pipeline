"""The intake API against the real stack: auth, idempotent accept, tenant-scoped read."""

from uuid import uuid4

import httpx
import pytest

from tests.integration.conftest import API_URL

A = {"Authorization": "Bearer demo-key-a"}
B = {"Authorization": "Bearer demo-key-b"}
BODY = {"type": "guest.register", "data": {"name": "Ana"}}


@pytest.fixture
async def api():
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        yield client


async def test_accept_then_replay_then_read(api, pool):
    key = f"it-{uuid4()}"

    first = await api.post("/v1/requests", json=BODY, headers=A | {"Idempotency-Key": key})
    replay = await api.post("/v1/requests", json=BODY, headers=A | {"Idempotency-Key": key})
    rid = first.json()["id"]

    assert first.status_code == 202 and replay.status_code == 202
    assert replay.json()["id"] == rid and replay.headers["Idempotent-Replayed"] == "true"
    row = await pool.fetchrow("SELECT tenant_id, payload FROM requests WHERE id = $1::uuid", rid)
    assert row["tenant_id"] == "tenant-a" and row["payload"] == BODY

    own = await api.get(f"/v1/requests/{rid}", headers=A)
    other = await api.get(f"/v1/requests/{rid}", headers=B)
    assert own.status_code == 200 and other.status_code == 404


async def test_unknown_key_is_rejected(api):
    response = await api.post("/v1/requests", json=BODY,
                              headers={"Authorization": "Bearer nope", "Idempotency-Key": "x"})

    assert response.status_code == 401
