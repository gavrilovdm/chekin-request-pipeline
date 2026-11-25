"""What a worker does with one request id taken off the queue: claim, process, classify, record.

The handler knows nothing about RabbitMQ. The consumer calls ``handle`` and acks the message whatever the result,
because the request's state lives in Postgres: a message is only a hint that there may be work for this id.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from uuid import UUID

import structlog

from pipeline.domain.errors import PermanentError, RetryableError
from pipeline.domain.request import Outcome, Status, Transition, decide, exhausted_before_processing
from pipeline.domain.retry_policy import RetryPolicy
from pipeline.ports import ClaimedRequest, Clock, Processor, RequestRepository

log = structlog.get_logger(__name__)


def utcnow() -> datetime:
    """Current UTC time; the default clock."""
    return datetime.now(UTC)


class HandleResult(StrEnum):
    """What happened to one delivery. Final statuses plus the two ways a delivery does nothing."""

    DONE = Status.DONE.value
    FAILED = Status.FAILED.value
    RETRY_SCHEDULED = Status.RETRY_SCHEDULED.value
    NEEDS_HUMAN = Status.NEEDS_HUMAN.value
    SKIPPED = "skipped"  # already final, or held by another worker's live lease
    LEASE_LOST = "lease_lost"  # this attempt took too long; another worker owns the request now


class RequestHandler:
    """Runs one processing attempt for a request id, safely under at-least-once delivery."""

    def __init__(
        self,
        repo: RequestRepository,
        processor: Processor,
        policy: RetryPolicy,
        *,
        lease: timedelta,
        timeout: float,
        clock: Clock = utcnow,
    ) -> None:
        if timeout <= 0:
            raise ValueError("processing timeout must be positive")
        if lease.total_seconds() <= timeout:
            raise ValueError("lease must be longer than the processing timeout, or healthy attempts lose it")
        self._repo = repo
        self._processor = processor
        self._policy = policy
        self._lease = lease
        self._timeout = timeout
        self._clock = clock

    async def handle(self, request_id: UUID) -> HandleResult:
        """Claim the request, run one attempt and record its outcome. Safe to call twice for the same id."""
        claimed = await self._repo.claim(request_id, self._lease)
        if claimed is None:
            log.info("request.skipped", request_id=str(request_id))
            return HandleResult.SKIPPED

        if exhausted_before_processing(claimed.attempt, self._policy):
            transition = Transition(
                Status.NEEDS_HUMAN,
                error=f"attempt {claimed.attempt} > max {self._policy.max_attempts} with no recorded outcome "
                "(crash loop: the worker died while processing)",
            )
        else:
            outcome = await self._attempt(claimed)
            transition = decide(claimed.attempt, outcome, self._policy, self._clock())

        if not await self._repo.complete(request_id, claimed.attempt, transition):
            log.warning("request.lease_lost", request_id=str(request_id), attempt=claimed.attempt)
            return HandleResult.LEASE_LOST

        log.info(
            "request.attempted",
            request_id=str(request_id),
            attempt=claimed.attempt,
            status=transition.status.value,
            error=transition.error,
        )
        return HandleResult(transition.status.value)

    async def _attempt(self, claimed: ClaimedRequest) -> Outcome:
        try:
            async with asyncio.timeout(self._timeout):
                result = await self._processor.process(claimed)
        except TimeoutError:
            return Outcome.retryable(f"timeout after {self._timeout}s")
        except RetryableError as exc:
            return Outcome.retryable(str(exc) or type(exc).__name__)
        except PermanentError as exc:
            return Outcome.permanent(str(exc) or type(exc).__name__)
        except Exception as exc:  # unknown failure: retry, bounded by max_attempts; keep the text for a human
            log.exception("request.unexpected_error", request_id=str(claimed.id))
            return Outcome.retryable(f"unexpected {type(exc).__name__}: {exc}")
        return Outcome.succeeded(result)
