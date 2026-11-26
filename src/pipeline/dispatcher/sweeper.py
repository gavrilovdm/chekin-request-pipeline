"""The safety net: finds requests the queue lost or that are due again, and publishes their ids."""

import asyncio
from collections.abc import Awaitable, Callable
from datetime import timedelta

import structlog

from pipeline.observability import REPUBLISH_FAILURES, REPUBLISHED
from pipeline.ports import Publisher, SweepStore

log = structlog.get_logger(__name__)


class Sweeper:
    """Re-publishes due requests. Safe to run in many replicas: the store hands out rows with SKIP LOCKED.

    A publish that fails here is not retried in-process: the row was stamped, so it is picked again after
    ``stale_after``. That keeps the sweep loop simple and its cost bounded.

    "Lost message" recovery is gated on the queue being nearly empty. Under a backlog an unclaimed request is
    almost certainly still queued; re-publishing it would add a duplicate per sweep, and the duplicates feed the
    backlog that caused them (found in the T9 load test: 131k duplicates in one run). Due retries and expired
    leases are never in the queue, so they are always re-published.
    """

    def __init__(
        self,
        store: SweepStore,
        publisher: Publisher,
        stale_after: timedelta,
        batch: int,
        queue_depth: Callable[[], Awaitable[int]],
    ) -> None:
        self._store = store
        self._publisher = publisher
        self._stale_after = stale_after
        self._batch = batch
        self._queue_depth = queue_depth

    async def _recover_lost(self) -> bool:
        try:
            return await self._queue_depth() < self._batch
        except Exception:  # broker unreachable: publishing would fail anyway
            return False

    async def sweep_once(self) -> int:
        """One pass; returns how many ids were published."""
        due = await self._store.take_due(self._stale_after, self._batch, await self._recover_lost())
        if not due:
            return 0
        results = await asyncio.gather(*(self._publisher.publish(rid) for rid, _ in due), return_exceptions=True)
        failed = [(rid, r) for (rid, _), r in zip(due, results, strict=True) if isinstance(r, BaseException)]
        for rid, exc in failed:
            log.warning("sweeper.publish_failed", request_id=str(rid), error=repr(exc))
        reasons: dict[str, int] = {}
        for (_, status), outcome in zip(due, results, strict=True):
            if not isinstance(outcome, BaseException):
                REPUBLISHED.labels(status).inc()
            reasons[status] = reasons.get(status, 0) + 1
        REPUBLISH_FAILURES.inc(len(failed))
        log.info("sweeper.republished", total=len(due), failed=len(failed), by_status=reasons)
        return len(due) - len(failed)
