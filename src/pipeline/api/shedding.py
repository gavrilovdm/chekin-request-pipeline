"""Load shedding on queue depth: refuse new work early instead of building hours of backlog."""

import asyncio
from collections.abc import Awaitable, Callable

import structlog

log = structlog.get_logger(__name__)


class LoadShedder:
    """Refreshes the queue depth in the background; the request path only reads a number.

    Fails open: when the depth is unknown (RabbitMQ down), requests are still accepted. They are committed to
    Postgres, which is the source of truth, and the sweeper publishes them once the broker is back.
    """

    def __init__(
        self,
        read_depth: Callable[[], Awaitable[int]],
        max_depth: int,
        interval: float = 1.0,
        retry_after_s: int = 5,
    ) -> None:
        self._read_depth = read_depth
        self._max_depth = max_depth
        self._interval = interval
        self._retry_after_s = retry_after_s
        self.depth: int | None = None

    def retry_after(self) -> int | None:
        """Seconds the client should wait if the queue is too deep; ``None`` to accept."""
        if self.depth is not None and self.depth > self._max_depth:
            return self._retry_after_s
        return None

    async def refresh(self) -> None:
        """Read the depth once; on failure forget it (fail open)."""
        try:
            self.depth = await asyncio.wait_for(self._read_depth(), timeout=self._interval * 2)
        except Exception as exc:  # includes TimeoutError: a stalled broker must not freeze a stale depth
            self.depth = None
            log.warning("shedder.depth_unknown", error=str(exc))

    async def run(self) -> None:
        """Refresh forever; cancelled on shutdown."""
        while True:
            await self.refresh()
            await asyncio.sleep(self._interval)
