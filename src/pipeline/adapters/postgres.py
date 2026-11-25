"""Postgres implementations of the request ports. Every state change is one conditional statement."""

import json
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

import asyncpg

from pipeline.domain.request import CLAIMABLE, Status, Transition, ensure_transition
from pipeline.ports import ClaimedRequest, CreateResult, RequestView, TenantRecord, WebhookJob

_CLAIMABLE = [s.value for s in CLAIMABLE]


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec("jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog")


async def create_pool(dsn: str, size: int = 10, command_timeout: float = 5.0) -> asyncpg.Pool:
    """Connection pool with jsonb mapped to Python dicts.

    ``command_timeout`` bounds every query: if Postgres stalls, callers get an error instead of hanging forever
    (a hung worker would hold its prefetched deliveries unacked and stop consuming).
    """
    return await asyncpg.create_pool(
        dsn, min_size=1, max_size=size, init=_init_connection, command_timeout=command_timeout
    )


class PgRequestRepository:
    """``RequestRepository`` for workers: claim under a lease, complete fenced by the attempt number."""

    _CLAIM = """
        UPDATE requests
           SET status = 'processing', attempt = attempt + 1,
               locked_until = now() + $2::interval, updated_at = now()
         WHERE id = $1
           AND (   (status = ANY($3::text[])
                    AND (status <> 'retry_scheduled' OR COALESCE(next_attempt_at, now()) <= now()))
                OR (status = 'processing' AND locked_until < now()))
     RETURNING id, tenant_id, payload, attempt
    """

    # Fencing: only the attempt that holds the lease may record an outcome. The webhook flag is set in the same
    # statement (the requests table is the outbox), and only for final states of tenants that have a webhook URL.
    _COMPLETE = """
        UPDATE requests r
           SET status = $3, result = $4, last_error = $5, next_attempt_at = $6, locked_until = NULL,
               webhook_pending = ($7 AND t.webhook_url IS NOT NULL),
               webhook_next_at = CASE WHEN $7 AND t.webhook_url IS NOT NULL THEN now() END,
               updated_at = now()
          FROM tenants t
         WHERE r.id = $1 AND r.status = 'processing' AND r.attempt = $2 AND t.id = r.tenant_id
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def claim(self, request_id: UUID, lease: timedelta) -> ClaimedRequest | None:
        """Take the request if it is claimable now (see ``ports.RequestRepository.claim``)."""
        row = await self._pool.fetchrow(self._CLAIM, request_id, lease, _CLAIMABLE)
        if row is None:
            return None
        return ClaimedRequest(row["id"], row["tenant_id"], row["payload"], row["attempt"])

    async def complete(self, request_id: UUID, attempt: int, transition: Transition) -> bool:
        """Record the outcome if ``attempt`` still holds the lease."""
        ensure_transition(Status.PROCESSING, transition.status)
        result = await self._pool.execute(
            self._COMPLETE,
            request_id,
            attempt,
            transition.status.value,
            transition.result,
            transition.error,
            transition.next_attempt_at,
            transition.notify,
        )
        return result == "UPDATE 1"


class PgIntakeStore:
    """``IntakeStore`` for the API: tenant lookup, idempotent create, tenant-scoped read."""

    _CREATE = """
        INSERT INTO requests (id, tenant_id, idempotency_key, payload_hash, payload, status)
        VALUES ($1, $2, $3, $4, $5, 'received')
        ON CONFLICT (tenant_id, idempotency_key) DO NOTHING
        RETURNING id
    """
    _EXISTING = "SELECT id, payload_hash FROM requests WHERE tenant_id = $1 AND idempotency_key = $2"
    _GET = """
        SELECT id, status, attempt, result, last_error, created_at, updated_at
          FROM requests WHERE id = $1 AND tenant_id = $2
    """
    _TENANT = "SELECT id, rate_per_sec, burst FROM tenants WHERE api_key_hash = $1"

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def tenant_by_key_hash(self, key_hash: str) -> TenantRecord | None:
        """Tenant owning the API key whose sha256 is ``key_hash``."""
        row = await self._pool.fetchrow(self._TENANT, key_hash)
        return TenantRecord(row["id"], row["rate_per_sec"], row["burst"]) if row else None

    async def create(
        self, tenant_id: str, idempotency_key: str, payload: dict[str, Any], payload_hash: str
    ) -> CreateResult:
        """One insert on the happy path; a replay costs one extra read."""
        new_id = uuid4()
        inserted = await self._pool.fetchval(self._CREATE, new_id, tenant_id, idempotency_key, payload_hash, payload)
        if inserted is not None:
            return CreateResult(inserted, True, payload_hash)
        row = await self._pool.fetchrow(self._EXISTING, tenant_id, idempotency_key)
        return CreateResult(row["id"], False, row["payload_hash"])

    async def get(self, tenant_id: str, request_id: UUID) -> RequestView | None:
        """The request if it belongs to ``tenant_id``."""
        row = await self._pool.fetchrow(self._GET, request_id, tenant_id)
        return RequestView(**dict(row)) if row else None


class PgDispatchStore:
    """``SweepStore`` and ``WebhookStore``. Rows are handed out with ``FOR UPDATE SKIP LOCKED``.

    Any number of dispatcher replicas can run: each gets a disjoint batch, no leader election needed.
    """

    # A row is due when its trigger time has passed and it was not re-published since that trigger
    # (or that re-publish is older than stale_after, i.e. the message itself may have been lost).
    _TAKE_DUE = """
        WITH due AS (
            SELECT id FROM requests
             WHERE (status = 'received'
                    AND COALESCE(enqueued_at, created_at) < now() - $1::interval)
                OR (status = 'retry_scheduled' AND next_attempt_at <= now()
                    AND (enqueued_at IS NULL OR enqueued_at < next_attempt_at
                         OR enqueued_at < now() - $1::interval))
                OR (status = 'processing' AND locked_until < now()
                    AND (enqueued_at IS NULL OR enqueued_at < locked_until
                         OR enqueued_at < now() - $1::interval))
             LIMIT $2
               FOR UPDATE SKIP LOCKED
        )
        UPDATE requests r SET enqueued_at = now()
          FROM due WHERE r.id = due.id
        RETURNING r.id, r.status
    """

    # At most $3 jobs per tenant per claim: a tenant whose endpoint hangs can hold one round of its own
    # deliveries (<= the HTTP timeout), never the whole batch. Candidates are over-fetched so other tenants
    # still fill the batch.
    _CLAIM_WEBHOOKS = """
        WITH candidates AS (
            SELECT id, tenant_id FROM requests
             WHERE webhook_pending AND webhook_next_at <= now()
             ORDER BY webhook_next_at
             LIMIT $1 * 4
               FOR UPDATE SKIP LOCKED
        ), ranked AS (
            SELECT id, row_number() OVER (PARTITION BY tenant_id ORDER BY id) AS n FROM candidates
        ), due AS (
            SELECT id FROM ranked WHERE n <= $3 LIMIT $1
        )
        UPDATE requests r SET webhook_next_at = now() + $2::interval
          FROM due, tenants t
         WHERE r.id = due.id AND t.id = r.tenant_id
        RETURNING r.id, r.tenant_id, t.webhook_url, t.webhook_secret, r.status, r.result, r.last_error,
                  r.attempt, r.webhook_attempt
    """

    _DELIVERED = """
        UPDATE requests SET webhook_pending = false, webhook_status = 'delivered',
                            webhook_attempt = webhook_attempt + 1, webhook_next_at = NULL
         WHERE id = $1 AND webhook_pending AND webhook_attempt = $2
    """

    _FAILED = """
        UPDATE requests SET webhook_attempt = webhook_attempt + 1,
                            webhook_next_at = $3::timestamptz,
                            webhook_pending = ($3::timestamptz IS NOT NULL),
                            webhook_status = CASE WHEN $3::timestamptz IS NULL THEN 'failed' END
         WHERE id = $1 AND webhook_pending AND webhook_attempt = $2
    """

    _HEALTH = """
        SELECT (SELECT count(*) FROM requests WHERE status = 'needs_human') AS needs_human,
               (SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0) FROM requests
                 WHERE status IN ('received', 'processing', 'retry_scheduled')) AS oldest_unfinished_s,
               (SELECT count(*) FROM requests WHERE webhook_pending) AS webhooks_pending
    """

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def health_snapshot(self) -> dict[str, float]:
        """Numbers behind the dispatcher's gauges; each subquery is served by a partial index."""
        row = await self._pool.fetchrow(self._HEALTH)
        return {key: float(row[key]) for key in ("needs_human", "oldest_unfinished_s", "webhooks_pending")}

    async def take_due(self, stale_after: timedelta, limit: int) -> list[tuple[UUID, str]]:
        """Stamp and return requests that need publishing."""
        rows = await self._pool.fetch(self._TAKE_DUE, stale_after, limit)
        return [(row["id"], row["status"]) for row in rows]

    async def claim_due(self, limit: int, visibility: timedelta, per_tenant: int) -> list[WebhookJob]:
        """Webhooks due now (at most ``per_tenant`` each), hidden from other senders for ``visibility``."""
        rows = await self._pool.fetch(self._CLAIM_WEBHOOKS, limit, visibility, per_tenant)
        return [
            WebhookJob(r["id"], r["tenant_id"], r["webhook_url"], r["webhook_secret"], r["status"], r["result"],
                       r["last_error"], r["attempt"], r["webhook_attempt"])
            for r in rows
        ]

    async def mark_delivered(self, request_id: UUID, webhook_attempt: int) -> None:
        """Delivered: clear the outbox flag (only if no other sender recorded this attempt first)."""
        await self._pool.execute(self._DELIVERED, request_id, webhook_attempt)

    async def mark_failed_attempt(self, request_id: UUID, webhook_attempt: int, next_at: datetime | None) -> None:
        """Failed: schedule the next try, or give up when ``next_at`` is None. Fenced like ``mark_delivered``."""
        await self._pool.execute(self._FAILED, request_id, webhook_attempt, next_at)
