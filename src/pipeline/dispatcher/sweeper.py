"""The safety net: finds requests the queue lost or that are due again, and publishes their ids."""

import asyncio
from datetime import timedelta

import structlog

from pipeline.ports import Publisher, SweepStore

log = structlog.get_logger(__name__)


class Sweeper:
    """Re-publishes due requests. Safe to run in many replicas: the store hands out rows with SKIP LOCKED.

    A publish that fails here is not retried in-process: the row was stamped, so it is picked again after
    ``stale_after``. That keeps the sweep loop simple and its cost bounded.
    """

    def __init__(self, store: SweepStore, publisher: Publisher, stale_after: timedelta, batch: int) -> None:
        self._store = store
        self._publisher = publisher
        self._stale_after = stale_after
        self._batch = batch

    async def sweep_once(self) -> int:
        """One pass; returns how many ids were published."""
        due = await self._store.take_due(self._stale_after, self._batch)
        if not due:
            return 0
        results = await asyncio.gather(*(self._publisher.publish(rid) for rid, _ in due), return_exceptions=True)
        failed = [(rid, r) for (rid, _), r in zip(due, results, strict=True) if isinstance(r, BaseException)]
        for rid, exc in failed:
            log.warning("sweeper.publish_failed", request_id=str(rid), error=repr(exc))
        reasons: dict[str, int] = {}
        for _, status in due:
            reasons[status] = reasons.get(status, 0) + 1
        log.info("sweeper.republished", total=len(due), failed=len(failed), by_status=reasons)
        return len(due) - len(failed)
