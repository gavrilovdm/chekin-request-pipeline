"""The same claim / fencing scenarios as tests/fakes.py, against real Postgres."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pipeline.adapters.postgres import PgIntakeStore, PgRequestRepository
from pipeline.domain.request import Status, Transition
from tests.integration.conftest import set_row

LEASE = timedelta(seconds=30)


@pytest.fixture
def repo(pool):
    return PgRequestRepository(pool)


@pytest.fixture
def store(pool):
    return PgIntakeStore(pool)


async def new_request(store, tenant="tenant-a", payload=None):
    result = await store.create(tenant, f"k-{uuid4()}", payload or {"guest": "Ana"}, "hash-1")
    assert result.created
    return result.id


async def status_of(pool, rid):
    return await pool.fetchrow("SELECT * FROM requests WHERE id = $1", rid)


async def test_claim_once_then_none(store, repo):
    rid = await new_request(store)

    first = await repo.claim(rid, LEASE)
    second = await repo.claim(rid, LEASE)

    assert first is not None and first.attempt == 1 and first.payload == {"guest": "Ana"}
    assert first.tenant_id == "tenant-a"
    assert second is None


async def test_claim_unknown_id_is_none(repo):
    assert await repo.claim(uuid4(), LEASE) is None


@pytest.mark.parametrize("final", ["done", "failed", "needs_human"])
async def test_final_request_is_not_claimable(store, repo, pool, final):
    rid = await new_request(store)
    await set_row(pool, rid, status=final)

    assert await repo.claim(rid, LEASE) is None


async def test_expired_lease_is_reclaimed(store, repo, pool):
    rid = await new_request(store)
    await repo.claim(rid, LEASE)
    await set_row(pool, rid, locked_until=datetime.now(UTC) - timedelta(seconds=1))

    again = await repo.claim(rid, LEASE)

    assert again is not None and again.attempt == 2


async def test_due_retry_is_claimable(store, repo, pool):
    rid = await new_request(store)
    past = datetime.now(UTC) - timedelta(seconds=1)
    await set_row(pool, rid, status="retry_scheduled", attempt=1, next_attempt_at=past)

    claimed = await repo.claim(rid, LEASE)

    assert claimed is not None and claimed.attempt == 2


async def test_retry_not_yet_due_is_not_claimable(store, repo, pool):
    """A duplicate message must not skip the backoff (same rule as tests/fakes.py)."""
    rid = await new_request(store)
    future = datetime.now(UTC) + timedelta(seconds=30)
    await set_row(pool, rid, status="retry_scheduled", attempt=1, next_attempt_at=future)

    assert await repo.claim(rid, LEASE) is None


async def test_complete_records_done_and_flags_webhook_for_tenant_with_url(store, repo, pool):
    rid = await new_request(store, tenant="tenant-a")
    claimed = await repo.claim(rid, LEASE)

    assert await repo.complete(rid, claimed.attempt, Transition(Status.DONE, result={"ref": "R-1"}))

    row = await status_of(pool, rid)
    assert row["status"] == "done"
    assert row["result"] == {"ref": "R-1"}
    assert row["locked_until"] is None
    assert row["webhook_pending"] is True and row["webhook_next_at"] is not None


async def test_no_webhook_flag_for_tenant_without_url(store, repo, pool):
    rid = await new_request(store, tenant="tenant-b")
    claimed = await repo.claim(rid, LEASE)

    await repo.complete(rid, claimed.attempt, Transition(Status.DONE, result={}))

    assert (await status_of(pool, rid))["webhook_pending"] is False


async def test_retry_is_not_reported_to_webhook(store, repo, pool):
    rid = await new_request(store, tenant="tenant-a")
    claimed = await repo.claim(rid, LEASE)
    due = datetime.now(UTC) + timedelta(seconds=4)

    await repo.complete(rid, claimed.attempt, Transition(Status.RETRY_SCHEDULED, error="503", next_attempt_at=due))

    row = await status_of(pool, rid)
    assert row["status"] == "retry_scheduled" and row["next_attempt_at"] == due
    assert row["last_error"] == "503" and row["webhook_pending"] is False


async def test_stale_attempt_cannot_complete(store, repo, pool):
    rid = await new_request(store)
    stale = await repo.claim(rid, LEASE)
    await set_row(pool, rid, locked_until=datetime.now(UTC) - timedelta(seconds=1))
    fresh = await repo.claim(rid, LEASE)

    assert not await repo.complete(rid, stale.attempt, Transition(Status.DONE, result={"by": "stale"}))
    assert await repo.complete(rid, fresh.attempt, Transition(Status.DONE, result={"by": "fresh"}))
    assert (await status_of(pool, rid))["result"] == {"by": "fresh"}


async def test_idempotent_create_returns_existing_id_and_hash(store):
    key = f"k-{uuid4()}"
    first = await store.create("tenant-a", key, {"n": 1}, "hash-1")
    replay = await store.create("tenant-a", key, {"n": 2}, "hash-2")
    other_tenant = await store.create("tenant-b", key, {"n": 1}, "hash-1")

    assert first.created and not replay.created
    assert replay.id == first.id and replay.payload_hash == "hash-1"
    assert other_tenant.created and other_tenant.id != first.id


async def test_get_is_tenant_scoped(store):
    rid = await new_request(store, tenant="tenant-a")

    view = await store.get("tenant-a", rid)

    assert view is not None and view.status == "received" and view.attempt == 0
    assert await store.get("tenant-b", rid) is None


async def test_tenant_lookup_by_key_hash(store):
    tenant = await store.tenant_by_key_hash("f15424e984f66fb993b9d8e6444ade02adcc714a58b3b50106e98750e4244bfe")

    assert tenant is not None and tenant.id == "tenant-a"
    assert await store.tenant_by_key_hash("nope") is None
