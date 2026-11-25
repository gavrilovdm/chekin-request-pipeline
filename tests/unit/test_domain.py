from datetime import UTC, datetime, timedelta

import pytest

from pipeline.domain.errors import IllegalTransition
from pipeline.domain.request import (
    CLAIMABLE,
    TERMINAL,
    Outcome,
    Status,
    Transition,
    decide,
    ensure_transition,
    exhausted_before_processing,
)
from pipeline.domain.retry_policy import RetryPolicy

NOW = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)
POLICY = RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=60.0)


def always(value: float):
    return lambda: value


# --- transitions -------------------------------------------------------------


@pytest.mark.parametrize(
    ("src", "dst"),
    [
        (Status.RECEIVED, Status.PROCESSING),
        (Status.RETRY_SCHEDULED, Status.PROCESSING),
        (Status.PROCESSING, Status.PROCESSING),  # lease expired, re-claimed
        (Status.PROCESSING, Status.DONE),
        (Status.PROCESSING, Status.FAILED),
        (Status.PROCESSING, Status.RETRY_SCHEDULED),
        (Status.PROCESSING, Status.NEEDS_HUMAN),
    ],
)
def test_allowed_transitions(src, dst):
    ensure_transition(src, dst)


@pytest.mark.parametrize("terminal", sorted(TERMINAL))
def test_terminal_states_are_final(terminal):
    for dst in Status:
        with pytest.raises(IllegalTransition):
            ensure_transition(terminal, dst)


ALLOWED_EDGES = {
    (Status.RECEIVED, Status.PROCESSING),
    (Status.RETRY_SCHEDULED, Status.PROCESSING),
    (Status.PROCESSING, Status.PROCESSING),
    (Status.PROCESSING, Status.DONE),
    (Status.PROCESSING, Status.FAILED),
    (Status.PROCESSING, Status.RETRY_SCHEDULED),
    (Status.PROCESSING, Status.NEEDS_HUMAN),
}


@pytest.mark.parametrize(
    ("src", "dst"), [(s, d) for s in Status for d in Status if (s, d) not in ALLOWED_EDGES]
)
def test_every_other_transition_is_rejected(src, dst):
    """The complement of the lifecycle diagram: any edge added by accident fails here."""
    with pytest.raises(IllegalTransition):
        ensure_transition(src, dst)


def test_claimable_and_terminal_sets():
    assert {Status.RECEIVED, Status.RETRY_SCHEDULED} == CLAIMABLE
    assert {Status.DONE, Status.FAILED, Status.NEEDS_HUMAN} == TERMINAL


# --- decide ------------------------------------------------------------------


def test_success_is_done_with_result():
    t = decide(1, Outcome.succeeded({"ref": "abc"}), POLICY, NOW)
    assert t.status is Status.DONE
    assert t.result == {"ref": "abc"}
    assert t.error is None and t.next_attempt_at is None
    assert t.notify


def test_permanent_error_fails_without_retry():
    t = decide(1, Outcome.permanent("422 invalid guest"), POLICY, NOW)
    assert t.status is Status.FAILED
    assert t.error == "422 invalid guest"
    assert t.next_attempt_at is None
    assert t.notify


def test_retryable_error_schedules_retry_within_backoff_ceiling():
    t = decide(2, Outcome.retryable("503"), POLICY, NOW, rand=always(1.0))
    assert t.status is Status.RETRY_SCHEDULED
    assert t.next_attempt_at == NOW + timedelta(seconds=2)  # ceiling for attempt 2
    assert t.error == "503"
    assert not t.notify


@pytest.mark.parametrize(
    ("attempt", "expected"),
    [(1, Status.RETRY_SCHEDULED), (2, Status.RETRY_SCHEDULED), (3, Status.NEEDS_HUMAN)],
)
def test_max_attempts_boundary(attempt, expected):
    assert decide(attempt, Outcome.retryable("timeout"), POLICY, NOW).status is expected


def test_needs_human_keeps_last_error_and_notifies():
    t = decide(3, Outcome.retryable("timeout"), POLICY, NOW)
    assert t.error == "timeout"
    assert t.notify


def test_attempt_must_be_positive():
    with pytest.raises(ValueError):
        decide(0, Outcome.succeeded({}), POLICY, NOW)


def test_crash_loop_guard():
    assert not exhausted_before_processing(3, POLICY)
    assert exhausted_before_processing(4, POLICY)


# --- retry policy ------------------------------------------------------------


@pytest.mark.parametrize(("attempt", "ceiling"), [(1, 1), (2, 2), (3, 4), (4, 8), (7, 60), (30, 60)])
def test_backoff_ceiling_doubles_and_caps(attempt, ceiling):
    policy = RetryPolicy(max_attempts=50, base_delay=1.0, max_delay=60.0)
    assert policy.ceiling(attempt) == ceiling
    assert policy.delay(attempt, rand=always(1.0)) == timedelta(seconds=ceiling)
    assert policy.delay(attempt, rand=always(0.0)) == timedelta(0)


def test_default_jitter_stays_in_range():
    policy = RetryPolicy(max_attempts=10, base_delay=1.0, max_delay=60.0)
    delays = {policy.delay(4) for _ in range(200)}
    assert all(timedelta(0) <= d <= timedelta(seconds=8) for d in delays)
    assert len(delays) > 1  # actually jittered


@pytest.mark.parametrize("kwargs", [{"max_attempts": 0}, {"base_delay": 0}, {"base_delay": 10, "max_delay": 5}])
def test_policy_rejects_nonsense(kwargs):
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


@pytest.mark.parametrize("status", [Status.PROCESSING, Status.RECEIVED])
def test_an_attempt_cannot_end_in_a_non_final_waiting_state(status):
    with pytest.raises(IllegalTransition):
        Transition(status)
