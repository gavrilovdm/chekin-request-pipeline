"""Request lifecycle: statuses, allowed transitions, and the decision taken after each processing attempt.

This module is the single source of truth for state rules. The worker, the repositories and the tests all go
through it; no other code decides what status a request moves to.
"""

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any

from pipeline.domain.errors import IllegalTransition
from pipeline.domain.retry_policy import RetryPolicy


class Status(StrEnum):
    """Where a request is in its lifecycle. Stored as text in Postgres."""

    RECEIVED = "received"  # accepted by the API; maybe published to the queue
    PROCESSING = "processing"  # claimed by a worker under a lease
    RETRY_SCHEDULED = "retry_scheduled"  # failed with a retryable error; due at next_attempt_at
    DONE = "done"
    FAILED = "failed"  # permanent error, no retry
    NEEDS_HUMAN = "needs_human"  # attempts exhausted; an operator must look


CLAIMABLE = frozenset({Status.RECEIVED, Status.RETRY_SCHEDULED})
TERMINAL = frozenset({Status.DONE, Status.FAILED, Status.NEEDS_HUMAN})  # final for the pipeline; reported to tenants

_ALLOWED: dict[Status, frozenset[Status]] = {
    Status.RECEIVED: frozenset({Status.PROCESSING}),
    Status.RETRY_SCHEDULED: frozenset({Status.PROCESSING}),
    # PROCESSING -> PROCESSING is a re-claim after the previous worker's lease expired.
    Status.PROCESSING: frozenset(
        {Status.PROCESSING, Status.DONE, Status.FAILED, Status.RETRY_SCHEDULED, Status.NEEDS_HUMAN}
    ),
    Status.DONE: frozenset(),
    Status.FAILED: frozenset(),
    # Operator replay after the cause is fixed (python -m pipeline replay). The only way out of a final state.
    Status.NEEDS_HUMAN: frozenset({Status.RETRY_SCHEDULED}),
}


def ensure_transition(src: Status, dst: Status) -> None:
    """Raise ``IllegalTransition`` unless the lifecycle allows moving from ``src`` to ``dst``."""
    if dst not in _ALLOWED[src]:
        raise IllegalTransition(f"{src} -> {dst}")


MAX_ERROR_LENGTH = 2000  # error text is stored and logged on every attempt; a 2 MB HTML error page must not be


def _clip(error: str) -> str:
    return error if len(error) <= MAX_ERROR_LENGTH else error[: MAX_ERROR_LENGTH - 1] + "…"


class OutcomeKind(StrEnum):
    """How one processing attempt ended."""

    SUCCEEDED = "succeeded"
    RETRYABLE = "retryable"
    PERMANENT = "permanent"


@dataclass(frozen=True)
class Outcome:
    """The result of one processing attempt, before any state decision."""

    kind: OutcomeKind
    result: dict[str, Any] | None = None
    error: str | None = None

    @classmethod
    def succeeded(cls, result: dict[str, Any]) -> "Outcome":
        """The downstream accepted the request."""
        return cls(OutcomeKind.SUCCEEDED, result=result)

    @classmethod
    def retryable(cls, error: str) -> "Outcome":
        """The attempt failed but may succeed later."""
        return cls(OutcomeKind.RETRYABLE, error=_clip(error))

    @classmethod
    def permanent(cls, error: str) -> "Outcome":
        """The attempt failed and will fail again."""
        return cls(OutcomeKind.PERMANENT, error=_clip(error))


@dataclass(frozen=True)
class Transition:
    """The state a request moves to after an attempt, with what to store alongside it."""

    status: Status
    result: dict[str, Any] | None = None
    error: str | None = None
    next_attempt_at: datetime | None = None

    def __post_init__(self) -> None:
        # An attempt always ends somewhere else: writing 'processing' with no lease would strand the request.
        if self.status in (Status.PROCESSING, Status.RECEIVED):
            raise IllegalTransition(f"an attempt cannot end in {self.status}")

    @property
    def notify(self) -> bool:
        """Whether the tenant should be told (a webhook): only final states are reported."""
        return self.status in TERMINAL


def decide(
    attempt: int,
    outcome: Outcome,
    policy: RetryPolicy,
    now: datetime,
    rand: Callable[[], float] = random.random,
) -> Transition:
    """Map the outcome of attempt number ``attempt`` (1-based) to the next state."""
    if attempt < 1:
        raise ValueError("attempt is 1-based")
    match outcome.kind:
        case OutcomeKind.SUCCEEDED:
            return Transition(Status.DONE, result=outcome.result)
        case OutcomeKind.PERMANENT:
            return Transition(Status.FAILED, error=outcome.error)
        case OutcomeKind.RETRYABLE if attempt >= policy.max_attempts:
            return Transition(Status.NEEDS_HUMAN, error=outcome.error)
        case OutcomeKind.RETRYABLE:
            return Transition(
                Status.RETRY_SCHEDULED,
                error=outcome.error,
                next_attempt_at=now + policy.delay(attempt, rand),
            )
    raise AssertionError(f"unhandled outcome {outcome.kind}")


def exhausted_before_processing(attempt: int, policy: RetryPolicy) -> bool:
    """True when a claim's attempt number is already past the limit.

    That happens only when earlier attempts never recorded an outcome, i.e. the worker died while processing:
    a crash loop. Such a request goes to ``needs_human`` instead of being processed again.
    """
    return attempt > policy.max_attempts
