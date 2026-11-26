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


async def test_malformed_input_never_breaks_the_connection(api):
    """Every malformed body gets a 422 on the same keep-alive connection, and a normal POST still works after."""
    bad = [b"{not json", b"", b"[1, 2]", b'{"type":"t","data":{},"evil":1}', b'{"type":"t","data":{"n":"A\\u0000"}}',
           b'{"type":"t","data":' + b'{"a":' * 2000 + b"1" + b"}" * 2001]
    for i, content in enumerate(bad):
        headers = A | {"Idempotency-Key": f"bad-{uuid4()}-{i}", "content-type": "application/json"}
        response = await api.post("/v1/requests", content=content, headers=headers)
        assert response.status_code == 422, (content[:40], response.status_code)
    too_long = await api.post("/v1/requests", json=BODY, headers=A | {"Idempotency-Key": "x" * 201})
    ok = await api.post("/v1/requests", json=BODY, headers=A | {"Idempotency-Key": f"ok-{uuid4()}"})

    assert too_long.status_code == 422
    assert ok.status_code == 202
