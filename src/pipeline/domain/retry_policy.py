"""Exponential backoff with full jitter and a bounded number of attempts."""

import random
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class RetryPolicy:
    """How many times a request is attempted and how long to wait between attempts.

    The delay after attempt ``n`` is ``uniform(0, min(max_delay, base_delay * 2**(n-1)))`` ("full jitter").
    Jitter spreads retries in time, so a downstream that recovers from an outage is not hit by a synchronized wave.
    """

    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 60.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if self.base_delay <= 0:
            raise ValueError("base_delay must be positive")
        if self.max_delay < self.base_delay:
            raise ValueError("max_delay must not be below base_delay")

    def ceiling(self, attempt: int) -> float:
        """Upper bound of the delay after ``attempt`` (1-based), in seconds."""
        return min(self.max_delay, self.base_delay * 2 ** (attempt - 1))

    def delay(self, attempt: int, rand: Callable[[], float] = random.random) -> timedelta:
        """Jittered delay after ``attempt``. ``rand`` returns a float in [0, 1]; injectable for tests."""
        return timedelta(seconds=rand() * self.ceiling(attempt))
