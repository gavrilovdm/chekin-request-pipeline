"""Interfaces the pipeline core depends on. Concrete adapters (asyncpg, aio-pika, httpx) implement them."""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import UUID

from pipeline.domain.request import Transition

Clock = Callable[[], datetime]


@dataclass(frozen=True)
class ClaimedRequest:
    """A request a worker now holds under a lease. ``attempt`` is 1-based and doubles as the fencing token."""

    id: UUID
    tenant_id: str
    payload: dict[str, Any]
    attempt: int


class RequestRepository(Protocol):
    """Request state as the worker needs it."""

    async def claim(self, request_id: UUID, lease: timedelta) -> ClaimedRequest | None:
        """Atomically take the request; ``None`` if it is not claimable right now.

        Claimable: ``received``; ``retry_scheduled`` whose ``next_attempt_at`` has passed (a duplicate message must
        not skip the backoff); ``processing`` whose lease expired. The lease is set to ``now + lease``.
        """
        ...

    async def complete(self, request_id: UUID, attempt: int, transition: Transition) -> bool:
        """Record the transition if this attempt still holds the lease; ``False`` if it was lost."""
        ...


class Processor(Protocol):
    """Does the actual work for a request. Raises ``RetryableError`` or ``PermanentError`` on failure."""

    async def process(self, request: ClaimedRequest) -> dict[str, Any]:
        """Process the request and return a JSON-serialisable result."""
        ...
