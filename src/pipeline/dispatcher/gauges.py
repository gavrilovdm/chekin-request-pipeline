"""Pipeline-wide health gauges, refreshed by the dispatcher (one query, not per request)."""

from collections.abc import Awaitable, Callable
from typing import Protocol

from pipeline.observability import NEEDS_HUMAN, OLDEST_UNFINISHED, QUEUE_DEPTH, WEBHOOKS_PENDING


class HealthSource(Protocol):
    """Where the numbers come from."""

    async def health_snapshot(self) -> dict[str, float]:
        """needs_human, oldest_unfinished_s, webhooks_pending."""
        ...


async def refresh_gauges(source: HealthSource, read_depth: Callable[[], Awaitable[int]]) -> int:
    """Set the gauges once; returns 0 so the caller's loop always sleeps between refreshes."""
    snapshot = await source.health_snapshot()
    NEEDS_HUMAN.set(snapshot["needs_human"])
    OLDEST_UNFINISHED.set(snapshot["oldest_unfinished_s"])
    WEBHOOKS_PENDING.set(snapshot["webhooks_pending"])
    QUEUE_DEPTH.set(await read_depth())
    return 0
