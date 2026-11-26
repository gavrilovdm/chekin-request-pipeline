"""Property-based tests: random interleavings of everything that can happen to one request.

Hypothesis drives a state machine over the real RequestHandler and the in-memory repository (same claim/fencing
semantics as the SQL, checked in tests/integration/test_postgres.py). After every step the invariants must hold;
at the end, liveness: with the faults gone, the request reaches a final state within a bounded number of deliveries.
"""

import asyncio
import contextlib
import os
from datetime import timedelta

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, initialize, invariant, rule

from pipeline.domain.errors import PermanentError, RetryableError
from pipeline.domain.request import TERMINAL, Status, Transition
from pipeline.domain.retry_policy import RetryPolicy
from pipeline.worker.handler import RequestHandler
from tests.fakes import FakeClock, InMemoryRepository, ScriptedProcessor

LEASE = timedelta(seconds=30)
POLICY = RetryPolicy(max_attempts=3, base_delay=1.0, max_delay=8.0)
OUTCOMES = st.sampled_from(["ok", "retryable", "permanent", "unexpected", "crash"])


def outcome_item(kind: str, attempt_tag: str):
    return {
        "ok": {"by": attempt_tag},
        "retryable": RetryableError("503"),
        "permanent": PermanentError("422"),
        "unexpected": KeyError("bug"),
        "crash": asyncio.CancelledError(),  # the worker dies mid-attempt: nothing is recorded
    }[kind]


class OneRequestLifecycle(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.clock = FakeClock()
        self.repo = InMemoryRepository(self.clock)
        self.rid = self.repo.add()
        self.zombies: list[tuple[int, int]] = []  # (attempt, round) claimed by workers that froze
        self.done_writes = 0
        self.final_seen: Status | None = None
        self.replays = 0

    @initialize(after_faults=st.sampled_from(["ok", "crash"]))
    def choose_what_happens_after_the_faults(self, after_faults):
        """Liveness is checked both when the downstream recovers and when every attempt keeps crashing."""
        self.after_faults = after_faults

    @property
    def row(self):
        return self.repo.rows[self.rid]

    def check_claim_was_allowed(self, before_status, before_attempt, before_lease, before_next):
        """Safety: a new attempt may start only if nobody holds a live lease and a scheduled retry is due."""
        if self.row.attempt == before_attempt:
            return
        now = self.clock()
        if before_status is Status.PROCESSING:
            assert before_lease is not None and before_lease < now, "claimed while another worker's lease was live"
        if before_status is Status.RETRY_SCHEDULED and before_next is not None:
            assert before_next <= now, "retry started before it was due (backoff skipped)"

    def snapshot(self):
        row = self.row
        return row.status, row.attempt, row.locked_until, row.next_attempt_at

    def handle(self, kind: str) -> None:
        handler = RequestHandler(self.repo, ScriptedProcessor(outcome_item(kind, f"a{self.row.attempt + 1}")),
                                 POLICY, lease=LEASE, timeout=5.0, clock=self.clock)
        before = self.snapshot()
        with contextlib.suppress(asyncio.CancelledError):
            asyncio.run(handler.handle(self.rid))
        self.check_claim_was_allowed(*before)
        if self.row.status is Status.DONE and before[0] is not Status.DONE:
            self.done_writes += 1

    @rule(kind=OUTCOMES)
    def deliver(self, kind):
        """A message for this id arrives (first, duplicate, redelivered or re-published by the sweeper)."""
        self.handle(kind)

    @rule(seconds=st.sampled_from([0.5, 2, 10, 31, 120]))
    def time_passes(self, seconds):
        """Backoffs become due, leases expire."""
        self.clock.advance(seconds)

    @rule()
    def worker_freezes_after_claim(self):
        """A worker claims and then freezes (GC pause, docker pause) before recording anything."""
        before = self.snapshot()
        claimed = asyncio.run(self.repo.claim(self.rid, LEASE))
        self.check_claim_was_allowed(*before)
        if claimed is not None:
            self.zombies.append((claimed.attempt, self.replays))

    @rule(data=st.data())
    def zombie_wakes_and_completes(self, data):
        """A frozen worker resumes and tries to record a success with its old fencing token."""
        if not self.zombies:
            return
        attempt, zombie_round = data.draw(st.sampled_from(self.zombies))
        before_status, before_attempt = self.row.status, self.row.attempt
        applied = asyncio.run(self.repo.complete(self.rid, attempt,
                                                 Transition(Status.DONE, result={"by": f"a{attempt}"})))
        if applied:
            # only legal for the current lease holder: same attempt, still processing
            assert before_status is Status.PROCESSING and attempt == before_attempt, "stale worker overwrote state"
            assert zombie_round == self.replays, "a worker from before the replay matched the new round's token"
            self.done_writes += 1

    @rule()
    def operator_replays(self):
        """An operator replays the request if it ended in needs_human."""
        if self.row.status is Status.NEEDS_HUMAN:
            asyncio.run(self.repo.replay(self.rid))
            self.replays += 1
            self.final_seen = None

    @invariant()
    def final_states_stick(self):
        status = self.row.status
        if self.final_seen in (Status.DONE, Status.FAILED):
            assert status is self.final_seen, f"{self.final_seen} changed to {status}"
        if status in TERMINAL:
            self.final_seen = status

    @invariant()
    def at_most_one_success_is_recorded(self):
        assert self.done_writes <= 1

    @invariant()
    def records_are_complete(self):
        row = self.row
        if row.status is Status.DONE:
            assert row.result is not None
            assert row.result["by"] == f"a{row.attempt}", "result written by an attempt that did not hold the lease"
        if row.status in (Status.FAILED, Status.NEEDS_HUMAN):
            assert row.last_error
        if row.status is Status.PROCESSING:
            assert row.locked_until is not None

    def teardown(self):
        """Liveness: within a bounded number of deliveries the request is final, whether the downstream recovers
        ("ok") or every further attempt crashes the worker ("crash": the crash-loop guard must end it)."""
        for _ in range(POLICY.max_attempts + 3):
            if self.row.status in TERMINAL:
                break
            self.clock.advance(120)  # leases expired, retries due
            self.handle(self.after_faults)
        assert self.row.status in TERMINAL, f"stuck in {self.row.status} ({self.after_faults} after faults)"


# 300 examples locally; CI (CI=true) runs 3000, which is what it takes to hit the rare multi-round interleavings.
OneRequestLifecycle.TestCase.settings = settings(
    max_examples=3000 if os.environ.get("CI") else 300,
    stateful_step_count=40,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)
TestOneRequestLifecycle = OneRequestLifecycle.TestCase
