import pytest

from pipeline.api.rate_limit import TenantRateLimiter, TokenBucket
from pipeline.ports import TenantRecord


def test_bucket_allows_burst_then_reports_wait():
    bucket = TokenBucket(rate=2.0, burst=3, now=0.0)

    assert [bucket.take(0.0) for _ in range(3)] == [None, None, None]
    assert bucket.take(0.0) == pytest.approx(0.5)  # one token every 0.5 s


def test_bucket_refills_over_time_up_to_burst():
    bucket = TokenBucket(rate=2.0, burst=3, now=0.0)
    for _ in range(3):
        bucket.take(0.0)

    assert bucket.take(0.5) is None
    assert bucket.take(0.5) is not None
    assert [bucket.take(100.0) for _ in range(3)] == [None, None, None]  # capped at burst
    assert bucket.take(100.0) is not None


def test_tenants_have_separate_buckets():
    clock = [0.0]
    limiter = TenantRateLimiter(clock=lambda: clock[0])
    noisy = TenantRecord("noisy", rate_per_sec=1, burst=1)
    quiet = TenantRecord("quiet", rate_per_sec=1, burst=1)

    assert limiter.check(noisy) is None
    assert limiter.check(noisy) is not None
    assert limiter.check(quiet) is None
