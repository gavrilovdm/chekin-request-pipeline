"""Due selection for the sweeper and the webhook outbox against real Postgres."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from pipeline.adapters.postgres import PgDispatchStore, PgIntakeStore
from tests.integration.conftest import set_row

STALE = timedelta(seconds=60)


@pytest.fixture
def dispatch(pool):
    return PgDispatchStore(pool)


@pytest.fixture
def store(pool):
    return PgIntakeStore(pool)


async def new_row(store, pool, tenant="tenant-a", **fields):
    rid = (await store.create(tenant, f"d-{uuid4()}", {"n": 1}, "h")).id
    if fields:
        await set_row(pool, rid, **fields)
    return rid


async def due_ids(dispatch, recover_lost=True):
    return {rid for rid, _ in await dispatch.take_due(STALE, 10_000, recover_lost)}


def ago(seconds):
    return datetime.now(UTC) - timedelta(seconds=seconds)


async def test_due_selection_per_row_type(store, pool, dispatch):
    await due_ids(dispatch)  # flush rows left by earlier tests
    lost = await new_row(store, pool, created_at=ago(120))
    fresh = await new_row(store, pool)
    retry_due = await new_row(store, pool, status="retry_scheduled", attempt=1, next_attempt_at=ago(1))
    retry_later = await new_row(store, pool, status="retry_scheduled", attempt=1,
                                next_attempt_at=datetime.now(UTC) + timedelta(seconds=30))
    lease_expired = await new_row(store, pool, status="processing", attempt=1, locked_until=ago(1))
    lease_alive = await new_row(store, pool, status="processing", attempt=1,
                                locked_until=datetime.now(UTC) + timedelta(seconds=30))
    done = await new_row(store, pool, status="done", attempt=1, created_at=ago(120))

    due = await due_ids(dispatch)

    assert {lost, retry_due, lease_expired} <= due
    assert not {fresh, retry_later, lease_alive, done} & due


async def test_a_stamped_row_is_not_picked_again_until_stale(store, pool, dispatch):
    rid = await new_row(store, pool, status="retry_scheduled", attempt=1, next_attempt_at=ago(1))

    assert rid in await due_ids(dispatch)
    assert rid not in await due_ids(dispatch)

    await set_row(pool, rid, enqueued_at=ago(120))  # that message was lost
    assert rid in await due_ids(dispatch)


async def test_webhook_claim_hides_rows_and_records_outcomes(store, pool, dispatch):
    rid = await new_row(store, pool, status="done", attempt=1, result={"ref": "R"}, webhook_pending=True,
                        webhook_next_at=ago(1))

    jobs = [j for j in await dispatch.claim_due(10_000, timedelta(seconds=60), 10_000) if j.request_id == rid]
    again = [j for j in await dispatch.claim_due(10_000, timedelta(seconds=60), 10_000) if j.request_id == rid]

    assert len(jobs) == 1 and again == []
    job = jobs[0]
    assert job.url.endswith("/webhooks/tenant-a") and job.secret == "demo-secret-a" and job.result == {"ref": "R"}

    await dispatch.mark_failed_attempt(rid, 0, datetime.now(UTC) + timedelta(seconds=5))
    await dispatch.mark_failed_attempt(rid, 0, datetime.now(UTC))  # a second sender with the same job: fenced
    row = await pool.fetchrow("SELECT * FROM requests WHERE id = $1", rid)
    assert row["webhook_pending"] and row["webhook_attempt"] == 1 and row["webhook_status"] is None

    await dispatch.mark_failed_attempt(rid, 1, None)
    row = await pool.fetchrow("SELECT * FROM requests WHERE id = $1", rid)
    assert not row["webhook_pending"] and row["webhook_status"] == "failed" and row["webhook_attempt"] == 2


async def test_delivered_clears_the_outbox_flag(store, pool, dispatch):
    rid = await new_row(store, pool, status="done", attempt=1, webhook_pending=True, webhook_next_at=ago(1))

    await dispatch.mark_delivered(rid, 0)

    row = await pool.fetchrow("SELECT * FROM requests WHERE id = $1", rid)
    assert not row["webhook_pending"] and row["webhook_status"] == "delivered"


async def test_webhook_claim_caps_jobs_per_tenant(store, pool, dispatch):
    """A tenant with many due webhooks cannot fill the whole batch (head-of-line blocking)."""
    await dispatch.claim_due(10_000, timedelta(seconds=60), 10_000)  # flush earlier rows
    ids = [await new_row(store, pool, status="done", attempt=1, webhook_pending=True, webhook_next_at=ago(1))
           for _ in range(6)]

    jobs = [j for j in await dispatch.claim_due(100, timedelta(seconds=60), 2) if j.request_id in ids]

    assert len(jobs) == 2


async def test_backlog_mode_skips_lost_message_recovery_but_not_due_work(store, pool, dispatch):
    await due_ids(dispatch)
    lost = await new_row(store, pool, created_at=ago(120))
    retry_due = await new_row(store, pool, status="retry_scheduled", attempt=1, next_attempt_at=ago(1))

    due = await due_ids(dispatch, recover_lost=False)

    assert retry_due in due and lost not in due
    assert lost in await due_ids(dispatch, recover_lost=True)
