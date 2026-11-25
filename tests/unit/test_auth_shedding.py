import asyncio

from pipeline.api.auth import TenantCache
from pipeline.api.shedding import LoadShedder
from pipeline.ports import TenantRecord


class CountingStore:
    def __init__(self):
        self.lookups = 0

    async def tenant_by_key_hash(self, key_hash):
        self.lookups += 1
        return TenantRecord("t", 1, 1) if key_hash.startswith("0") else None


async def test_cache_is_bounded_under_key_guessing():
    cache = TenantCache(CountingStore(), ttl=60, max_entries=100)

    for i in range(1000):
        await cache.resolve(f"guess-{i}")

    assert len(cache) == 100


async def test_repeated_key_is_served_from_cache():
    store = CountingStore()
    cache = TenantCache(store, ttl=60)

    for _ in range(5):
        await cache.resolve("same-key")

    assert store.lookups == 1


async def test_shedder_refuses_above_threshold():
    async def depth():
        return 300

    shedder = LoadShedder(depth, max_depth=200, interval=0.05)
    await shedder.refresh()

    assert shedder.retry_after() == 5


async def test_shedder_fails_open_when_broker_errors():
    async def depth():
        raise ConnectionError("broker down")

    shedder = LoadShedder(depth, max_depth=200, interval=0.05)
    shedder.depth = 300  # was overloaded before the outage
    await shedder.refresh()

    assert shedder.depth is None and shedder.retry_after() is None


async def test_shedder_fails_open_when_broker_hangs():
    async def depth():
        await asyncio.sleep(10)

    shedder = LoadShedder(depth, max_depth=200, interval=0.05)
    shedder.depth = 300
    await shedder.refresh()

    assert shedder.retry_after() is None
