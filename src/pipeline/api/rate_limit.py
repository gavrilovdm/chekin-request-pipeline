"""Per-tenant token buckets: one noisy tenant cannot starve the others at intake."""

import time
from collections.abc import Callable

from pipeline.ports import TenantRecord


class TokenBucket:
    """Classic token bucket: ``burst`` tokens, refilled at ``rate`` per second. Time is passed in, not read."""

    def __init__(self, rate: float, burst: int, now: float) -> None:
        self._rate = rate
        self._burst = burst
        self._tokens = float(burst)
        self._last = now

    def take(self, now: float) -> float | None:
        """Take one token. ``None`` if allowed, otherwise the seconds until a token is available."""
        self._tokens = min(self._burst, self._tokens + (now - self._last) * self._rate)
        self._last = now
        if self._tokens >= 1:
            self._tokens -= 1
            return None
        return (1 - self._tokens) / self._rate


class TenantRateLimiter:
    """One bucket per tenant, in this process.

    With N api replicas each enforces the limit separately, so the effective limit is up to N times higher.
    Accepted for the prototype (ADR-0006); a shared limiter (Redis, or the API gateway) is the production answer.
    """

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._buckets: dict[str, TokenBucket] = {}

    def check(self, tenant: TenantRecord) -> float | None:
        """``None`` if the tenant may proceed, otherwise seconds to wait."""
        now = self._clock()
        bucket = self._buckets.get(tenant.id)
        if bucket is None:
            bucket = self._buckets[tenant.id] = TokenBucket(tenant.rate_per_sec, tenant.burst, now)
        return bucket.take(now)
