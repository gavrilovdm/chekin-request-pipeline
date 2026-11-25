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


@dataclass(frozen=True)
class TenantRecord:
    """What the API needs to know about the caller."""

    id: str
    rate_per_sec: float
    burst: int


@dataclass(frozen=True)
class CreateResult:
    """Outcome of an idempotent insert. ``created`` is False on a replay of an existing Idempotency-Key."""

    id: UUID
    created: bool
    payload_hash: str


@dataclass(frozen=True)
class RequestView:
    """A request as its tenant sees it."""

    id: UUID
    status: str
    attempt: int
    result: dict[str, Any] | None
    last_error: str | None
    created_at: datetime
    updated_at: datetime


class IntakeStore(Protocol):
    """Request state as the intake API needs it."""

    async def tenant_by_key_hash(self, key_hash: str) -> TenantRecord | None:
        """Look up the tenant owning an API key, by the key's sha256."""
        ...

    async def create(self, tenant_id: str, idempotency_key: str, payload: dict[str, Any], payload_hash: str
                     ) -> CreateResult:
        """Insert a new request, or return the existing one for the same (tenant, idempotency key)."""
        ...

    async def get(self, tenant_id: str, request_id: UUID) -> RequestView | None:
        """Fetch a request; ``None`` if it does not exist or belongs to another tenant."""
        ...


class Publisher(Protocol):
    """Puts a request id on the work queue."""

    async def publish(self, request_id: UUID) -> None:
        """Publish and wait for the broker's confirm; raise on failure or timeout."""
        ...


class Processor(Protocol):
    """Does the actual work for a request. Raises ``RetryableError`` or ``PermanentError`` on failure."""

    async def process(self, request: ClaimedRequest) -> dict[str, Any]:
        """Process the request and return a JSON-serialisable result."""
        ...
