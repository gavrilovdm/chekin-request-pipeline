import hashlib
from datetime import UTC, datetime
from uuid import UUID, uuid4

import httpx
import pytest

from pipeline.api.app import ApiDeps, create_app
from pipeline.api.auth import TenantCache
from pipeline.api.rate_limit import TenantRateLimiter
from pipeline.ports import CreateResult, RequestView, TenantRecord

KEYS = {"key-a": TenantRecord("tenant-a", 1000, 1000), "key-b": TenantRecord("tenant-b", 1000, 1000),
        "key-noisy": TenantRecord("tenant-noisy", 1, 2)}


class FakeStore:
    def __init__(self):
        self.rows: dict[tuple[str, str], tuple[UUID, str]] = {}
        self.owner: dict[UUID, str] = {}

    async def tenant_by_key_hash(self, key_hash):
        for key, tenant in KEYS.items():
            if hashlib.sha256(key.encode()).hexdigest() == key_hash:
                return tenant
        return None

    async def create(self, tenant_id, idempotency_key, payload, payload_hash):
        existing = self.rows.get((tenant_id, idempotency_key))
        if existing:
            return CreateResult(existing[0], False, existing[1])
        rid = uuid4()
        self.rows[(tenant_id, idempotency_key)] = (rid, payload_hash)
        self.owner[rid] = tenant_id
        return CreateResult(rid, True, payload_hash)

    async def get(self, tenant_id, request_id):
        if self.owner.get(request_id) != tenant_id:
            return None
        now = datetime.now(UTC)
        return RequestView(request_id, "received", 0, None, None, now, now)


class FakePublisher:
    def __init__(self, fail=False):
        self.fail = fail
        self.published: list[UUID] = []

    async def publish(self, request_id):
        if self.fail:
            raise TimeoutError("no confirm")
        self.published.append(request_id)


class FixedShedder:
    def __init__(self, retry_after=None):
        self.value = retry_after

    def retry_after(self):
        return self.value


def make_client(*, publisher=None, shedder=None, store=None):
    store = store or FakeStore()
    deps = ApiDeps(
        store=store,
        publisher=publisher or FakePublisher(),
        tenants=TenantCache(store, ttl=60),
        limiter=TenantRateLimiter(),
        shedder=shedder or FixedShedder(),
    )
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(deps)), base_url="http://test")


def post(client, *, key="key-a", idem="idem-1", body=None):
    headers = {"Authorization": f"Bearer {key}"}
    if idem is not None:
        headers["Idempotency-Key"] = idem
    return client.post("/v1/requests", json=body or {"type": "guest.register", "data": {"name": "Ana"}},
                       headers=headers)


async def test_new_request_is_accepted_and_published():
    publisher = FakePublisher()
    async with make_client(publisher=publisher) as client:
        response = await post(client)

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "received"
    assert body["status_url"] == f"/v1/requests/{body['id']}"
    assert publisher.published == [UUID(body["id"])]


async def test_replay_returns_same_id_without_republishing():
    publisher = FakePublisher()
    async with make_client(publisher=publisher) as client:
        first = await post(client)
        replay = await post(client, body={"data": {"name": "Ana"}, "type": "guest.register"})  # key order differs

    assert replay.status_code == 202
    assert replay.json()["id"] == first.json()["id"]
    assert replay.headers["Idempotent-Replayed"] == "true"
    assert len(publisher.published) == 1


async def test_same_key_different_body_is_rejected():
    async with make_client() as client:
        await post(client)
        conflict = await post(client, body={"type": "guest.register", "data": {"name": "Bob"}})

    assert conflict.status_code == 422
    assert "Idempotency-Key" in conflict.json()["detail"]


async def test_missing_idempotency_key_is_rejected():
    async with make_client() as client:
        response = await post(client, idem=None)

    assert response.status_code == 400


@pytest.mark.parametrize("auth", [None, "Bearer wrong", "Basic key-a"])
async def test_bad_credentials_are_rejected(auth):
    async with make_client() as client:
        headers = {"Idempotency-Key": "k"} | ({"Authorization": auth} if auth else {})
        response = await client.post("/v1/requests", json={"type": "t", "data": {}}, headers=headers)

    assert response.status_code == 401


async def test_noisy_tenant_is_limited_without_affecting_others():
    async with make_client() as client:
        codes = [(await post(client, key="key-noisy", idem=f"n{i}")).status_code for i in range(3)]
        limited = await post(client, key="key-noisy", idem="n-last")
        other = await post(client, key="key-b", idem="b1")

    assert codes[:2] == [202, 202] and codes[2] == 429
    assert int(limited.headers["Retry-After"]) >= 1
    assert other.status_code == 202


async def test_deep_queue_sheds_load_before_writing():
    store = FakeStore()
    async with make_client(store=store, shedder=FixedShedder(retry_after=5)) as client:
        response = await post(client)

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
    assert store.rows == {}


async def test_publish_failure_still_accepts_because_the_row_is_committed():
    store = FakeStore()
    async with make_client(store=store, publisher=FakePublisher(fail=True)) as client:
        response = await post(client)

    assert response.status_code == 202
    assert len(store.rows) == 1  # the sweeper will publish it later


async def test_get_is_tenant_scoped():
    async with make_client() as client:
        rid = (await post(client)).json()["id"]
        own = await client.get(f"/v1/requests/{rid}", headers={"Authorization": "Bearer key-a"})
        other = await client.get(f"/v1/requests/{rid}", headers={"Authorization": "Bearer key-b"})

    assert own.status_code == 200 and own.json()["status"] == "received"
    assert other.status_code == 404


async def test_oversized_body_is_rejected():
    async with make_client() as client:
        response = await post(client, body={"type": "t", "data": {"blob": "x" * 70_000}})

    assert response.status_code == 413


async def test_oversized_declared_length_is_rejected_before_reading():
    async with make_client() as client:
        response = await client.post("/v1/requests", content=b"{}", headers={
            "Authorization": "Bearer key-a", "Idempotency-Key": "k", "Content-Length": str(10**9)})

    assert response.status_code == 413


async def test_invalid_body_is_rejected():
    async with make_client() as client:
        response = await post(client, body={"data": {}})

    assert response.status_code == 422



@pytest.mark.parametrize(
    ("name", "content"),
    [
        ("invalid json", b"{not json"),
        ("empty body", b""),
        ("json array", b"[1, 2]"),
        ("deep nesting", b'{"type":"t","data":' + b'{"a":' * 2000 + b"1" + b"}" * 2001),
        ("unknown field", b'{"type":"t","data":{},"evil":1}'),
        ("nul character", b'{"type":"t","data":{"name":"A\\u0000"}}'),
    ],
)
async def test_malformed_input_is_a_422_never_a_500(name, content):
    async with make_client() as client:
        response = await client.post("/v1/requests", content=content, headers={
            "Authorization": "Bearer key-a", "Idempotency-Key": f"bad-{name}", "content-type": "application/json"})
        after = await post(client, idem=f"ok-after-{name}")

    assert response.status_code == 422, response.text
    assert after.status_code == 202  # the connection and the app are still fine


async def test_too_long_idempotency_key_is_a_422():
    async with make_client() as client:
        response = await post(client, idem="x" * 201)

    assert response.status_code == 422


async def test_store_unavailable_is_a_503_with_retry_after():
    from pipeline.ports import StoreUnavailable

    class DownStore(FakeStore):
        async def create(self, *args):
            raise StoreUnavailable("postgres unreachable")

    async with make_client(store=DownStore()) as client:
        response = await post(client, idem="down-1")

    assert response.status_code == 503
    assert response.headers["Retry-After"] == "5"
