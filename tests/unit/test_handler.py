import asyncio
from datetime import timedelta

import pytest

from pipeline.domain.errors import PermanentError, RetryableError
from pipeline.domain.request import Status
from pipeline.domain.retry_policy import RetryPolicy
from pipeline.worker.handler import HandleResult, RequestHandler
from tests.fakes import FakeClock, InMemoryRepository, ScriptedProcessor

LEASE = timedelta(seconds=30)
POLICY = RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=60.0)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def repo(clock):
    return InMemoryRepository(clock)


def make_handler(repo, clock, processor, *, timeout=5.0):
    return RequestHandler(repo, processor, POLICY, lease=LEASE, timeout=timeout, clock=clock)


async def test_success_marks_done_and_stores_result(repo, clock):
    rid = repo.add()
    processor = ScriptedProcessor({"ref": "R-1"})

    assert await make_handler(repo, clock, processor).handle(rid) is HandleResult.DONE
    row = repo.rows[rid]
    assert row.status is Status.DONE
    assert row.result == {"ref": "R-1"}
    assert row.attempt == 1
    assert row.locked_until is None
    assert processor.calls[0].attempt == 1


async def test_retryable_error_schedules_retry(repo, clock):
    rid = repo.add()
    handler = make_handler(repo, clock, ScriptedProcessor(RetryableError("503 from downstream")))

    assert await handler.handle(rid) is HandleResult.RETRY_SCHEDULED
    row = repo.rows[rid]
    assert row.status is Status.RETRY_SCHEDULED
    assert row.last_error == "503 from downstream"
    assert clock.now <= row.next_attempt_at <= clock.now + timedelta(seconds=1)


async def test_last_attempt_goes_to_needs_human(repo, clock):
    rid = repo.add(status=Status.RETRY_SCHEDULED, attempt=2)
    handler = make_handler(repo, clock, ScriptedProcessor(RetryableError("503")))

    assert await handler.handle(rid) is HandleResult.NEEDS_HUMAN
    assert repo.rows[rid].status is Status.NEEDS_HUMAN
    assert repo.rows[rid].last_error == "503"


async def test_permanent_error_fails_without_retry(repo, clock):
    rid = repo.add()
    handler = make_handler(repo, clock, ScriptedProcessor(PermanentError("422 invalid document")))

    assert await handler.handle(rid) is HandleResult.FAILED
    assert repo.rows[rid].status is Status.FAILED
    assert repo.rows[rid].next_attempt_at is None


@pytest.mark.parametrize("final", [Status.DONE, Status.FAILED, Status.NEEDS_HUMAN])
async def test_final_request_is_skipped_without_processing(repo, clock, final):
    rid = repo.add(status=final, attempt=1)
    processor = ScriptedProcessor({"ref": "x"})

    assert await make_handler(repo, clock, processor).handle(rid) is HandleResult.SKIPPED
    assert processor.calls == []


async def test_request_held_by_live_lease_is_skipped(repo, clock):
    rid = repo.add(status=Status.PROCESSING, attempt=1, locked_until=clock.now + timedelta(seconds=10))
    processor = ScriptedProcessor({"ref": "x"})

    assert await make_handler(repo, clock, processor).handle(rid) is HandleResult.SKIPPED
    assert processor.calls == []


async def test_expired_lease_is_reclaimed(repo, clock):
    rid = repo.add(status=Status.PROCESSING, attempt=1, locked_until=clock.now - timedelta(seconds=1))

    assert await make_handler(repo, clock, ScriptedProcessor({"ok": True})).handle(rid) is HandleResult.DONE
    assert repo.rows[rid].attempt == 2


async def test_stale_worker_cannot_overwrite_after_losing_lease(repo, clock):
    """Worker A is slow; its lease expires; worker B re-claims and finishes. A's late result must be dropped."""
    rid = repo.add()
    worker_b = make_handler(repo, clock, ScriptedProcessor({"by": "B"}))

    async def slow_then_b_takes_over(_request):
        clock.advance(LEASE.total_seconds() + 1)
        assert await worker_b.handle(rid) is HandleResult.DONE
        return {"by": "A"}

    worker_a = make_handler(repo, clock, ScriptedProcessor(slow_then_b_takes_over))

    assert await worker_a.handle(rid) is HandleResult.LEASE_LOST
    assert repo.rows[rid].status is Status.DONE
    assert repo.rows[rid].result == {"by": "B"}


async def test_timeout_is_retryable(repo, clock):
    rid = repo.add()

    async def hang(_request):
        await asyncio.sleep(1)

    handler = make_handler(repo, clock, ScriptedProcessor(hang), timeout=0.01)

    assert await handler.handle(rid) is HandleResult.RETRY_SCHEDULED
    assert "timeout" in repo.rows[rid].last_error


async def test_crash_loop_goes_to_needs_human_without_processing(repo, clock):
    """Three earlier attempts died mid-processing (no outcome recorded); the 4th claim must not process again."""
    rid = repo.add(status=Status.PROCESSING, attempt=3, locked_until=clock.now - timedelta(seconds=1))
    processor = ScriptedProcessor({"ref": "x"})

    assert await make_handler(repo, clock, processor).handle(rid) is HandleResult.NEEDS_HUMAN
    assert processor.calls == []
    assert "crash loop" in repo.rows[rid].last_error


async def test_unexpected_exception_is_retried_and_kept_for_humans(repo, clock):
    rid = repo.add()
    handler = make_handler(repo, clock, ScriptedProcessor(KeyError("guest_id")))

    assert await handler.handle(rid) is HandleResult.RETRY_SCHEDULED
    assert "KeyError" in repo.rows[rid].last_error


async def test_duplicate_delivery_processes_once(repo, clock):
    rid = repo.add()
    processor = ScriptedProcessor({"ref": "R-1"})
    handler = make_handler(repo, clock, processor)

    first, second = await handler.handle(rid), await handler.handle(rid)

    assert (first, second) == (HandleResult.DONE, HandleResult.SKIPPED)
    assert len(processor.calls) == 1


async def test_cancellation_propagates_for_graceful_shutdown(repo, clock):
    rid = repo.add()
    handler = make_handler(repo, clock, ScriptedProcessor(asyncio.CancelledError()))

    with pytest.raises(asyncio.CancelledError):
        await handler.handle(rid)
    assert repo.rows[rid].status is Status.PROCESSING  # lease will expire; the sweeper re-enqueues it


def test_lease_must_outlive_timeout(repo, clock):
    with pytest.raises(ValueError):
        RequestHandler(repo, ScriptedProcessor({}), POLICY, lease=timedelta(seconds=5), timeout=5.0, clock=clock)
