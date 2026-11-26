"""In-memory fakes for unit tests. Semantics mirror the SQL in adapters/postgres.py (checked in T3)."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from pipeline.domain.request import CLAIMABLE, Status, Transition, ensure_transition
from pipeline.ports import ClaimedRequest


class FakeClock:
    def __init__(self, start: datetime = datetime(2026, 11, 26, 12, 0, tzinfo=UTC)):
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass
class Row:
    id: UUID
    tenant_id: str
    payload: dict[str, Any]
    status: Status = Status.RECEIVED
    attempt: int = 0
    attempt_offset: int = 0
    locked_until: datetime | None = None
    next_attempt_at: datetime | None = None
    result: dict[str, Any] | None = None
    last_error: str | None = None
    history: list[Status] = field(default_factory=list)


class InMemoryRepository:
    def __init__(self, clock: FakeClock):
        self.clock = clock
        self.rows: dict[UUID, Row] = {}

    def add(self, *, status: Status = Status.RECEIVED, attempt: int = 0, locked_until: datetime | None = None,
            next_attempt_at: datetime | None = None, attempt_offset: int = 0) -> UUID:
        rid = uuid4()
        self.rows[rid] = Row(rid, "tenant-a", {"n": 1}, status=status, attempt=attempt, locked_until=locked_until,
                             next_attempt_at=next_attempt_at, attempt_offset=attempt_offset)
        return rid

    async def claim(self, request_id: UUID, lease: timedelta) -> ClaimedRequest | None:
        row = self.rows.get(request_id)
        if row is None:
            return None
        now = self.clock()
        expired = row.status is Status.PROCESSING and row.locked_until is not None and row.locked_until < now
        retry_not_due = (
            row.status is Status.RETRY_SCHEDULED and row.next_attempt_at is not None and row.next_attempt_at > now
        )
        if (row.status not in CLAIMABLE and not expired) or retry_not_due:
            return None
        ensure_transition(row.status, Status.PROCESSING)
        row.status = Status.PROCESSING
        row.attempt += 1
        row.locked_until = self.clock() + lease
        row.history.append(Status.PROCESSING)
        return ClaimedRequest(row.id, row.tenant_id, row.payload, row.attempt, row.attempt - row.attempt_offset)

    async def complete(self, request_id: UUID, attempt: int, transition: Transition) -> bool:
        row = self.rows[request_id]
        if row.status is not Status.PROCESSING or row.attempt != attempt:
            return False
        ensure_transition(row.status, transition.status)
        row.status = transition.status
        row.result = transition.result
        row.last_error = transition.error
        row.next_attempt_at = transition.next_attempt_at
        row.locked_until = None
        row.history.append(transition.status)
        return True


class ScriptedProcessor:
    """Returns or raises the scripted items in order; an item may be a callable run before returning."""

    def __init__(self, *script: Any):
        self.script = list(script)
        self.calls: list[ClaimedRequest] = []

    async def process(self, request: ClaimedRequest) -> dict[str, Any]:
        self.calls.append(request)
        item = self.script.pop(0) if len(self.script) > 1 else self.script[0]
        if callable(item):
            item = await item(request)
        if isinstance(item, BaseException):
            raise item
        return item

