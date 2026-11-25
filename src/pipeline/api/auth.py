"""API-key authentication with a short-lived cache, so the hot path does not hit Postgres per request."""

import hashlib
import time
from collections.abc import Callable

from pipeline.ports import IntakeStore, TenantRecord


def hash_key(api_key: str) -> str:
    """sha256 of the key; only the hash is stored."""
    return hashlib.sha256(api_key.encode()).hexdigest()


class TenantCache:
    """Resolves a bearer key to its tenant. Misses are cached too, so key guessing cannot hammer the database."""

    def __init__(self, store: IntakeStore, ttl: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._store = store
        self._ttl = ttl
        self._clock = clock
        self._cache: dict[str, tuple[TenantRecord | None, float]] = {}

    async def resolve(self, api_key: str) -> TenantRecord | None:
        """Tenant for ``api_key``, or ``None`` if the key is unknown."""
        key_hash = hash_key(api_key)
        cached = self._cache.get(key_hash)
        if cached and cached[1] > self._clock():
            return cached[0]
        tenant = await self._store.tenant_by_key_hash(key_hash)
        self._cache[key_hash] = (tenant, self._clock() + self._ttl)
        return tenant
