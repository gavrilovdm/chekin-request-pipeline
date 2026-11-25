"""API-key authentication with a short-lived cache, so the hot path does not hit Postgres per request."""

import hashlib
import time
from collections import OrderedDict
from collections.abc import Callable

from pipeline.ports import IntakeStore, TenantRecord


def hash_key(api_key: str) -> str:
    """sha256 of the key; only the hash is stored."""
    return hashlib.sha256(api_key.encode()).hexdigest()


class TenantCache:
    """Resolves a bearer key to its tenant through a bounded LRU cache with a TTL.

    Misses are cached too, so repeating a wrong key does not reach the database. The cache is bounded, so random
    guessed keys cannot grow memory without limit; a flood of distinct guesses still costs one indexed lookup each,
    which in production belongs to the API gateway / WAF to throttle per client IP.
    """

    def __init__(
        self,
        store: IntakeStore,
        ttl: float,
        clock: Callable[[], float] = time.monotonic,
        max_entries: int = 10_000,
    ) -> None:
        self._store = store
        self._ttl = ttl
        self._clock = clock
        self._max_entries = max_entries
        self._cache: OrderedDict[str, tuple[TenantRecord | None, float]] = OrderedDict()

    async def resolve(self, api_key: str) -> TenantRecord | None:
        """Tenant for ``api_key``, or ``None`` if the key is unknown."""
        key_hash = hash_key(api_key)
        cached = self._cache.get(key_hash)
        if cached and cached[1] > self._clock():
            self._cache.move_to_end(key_hash)
            return cached[0]
        tenant = await self._store.tenant_by_key_hash(key_hash)
        self._cache[key_hash] = (tenant, self._clock() + self._ttl)
        self._cache.move_to_end(key_hash)
        while len(self._cache) > self._max_entries:
            self._cache.popitem(last=False)
        return tenant

    def __len__(self) -> int:
        return len(self._cache)
