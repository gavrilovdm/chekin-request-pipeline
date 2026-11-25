# T1 — Domain: state machine, retry policy, error types

Roadmap: [T1](../sdlc/roadmap.md) · Spec: [request lifecycle](../sdlc/spec.md#request-lifecycle)

## Files that change

- `src/pipeline/domain/request.py`:
  - `Status`
  - `CLAIMABLE` and `TERMINAL`
  - `ensure_transition`
  - `Outcome`
  - `Transition`
  - `decide`
  - `exhausted_before_processing`
- `src/pipeline/domain/retry_policy.py` — `RetryPolicy` with full-jitter exponential backoff.
- `src/pipeline/domain/errors.py` — `RetryableError`, `PermanentError`, `IllegalTransition`.
- `tests/unit/test_domain.py`

## Order of work

1. Write the tests first, from the spec's lifecycle diagram. Confirm they fail on import.
2. Implement `RetryPolicy`.
3. Implement statuses and transitions.
4. Implement `decide`: the single place that maps an attempt's outcome to the next state.

## Design notes

- `decide` is pure. It takes `now` and the policy, and returns a `Transition`. It never reads a clock or a random
  source by itself (the jitter source is injectable). This lets the worker, the repository and the tests share one
  rule.
- Backoff is full jitter: `uniform(0, min(cap, base·2^(attempt-1)))`. The ceilings 1, 2, 4, 8 s… match the
  interview diagram. Jitter spreads the retries, so a downstream recovering from an outage is not hit by a
  synchronized wave.
- `exhausted_before_processing(attempt)` guards against crash loops. A request whose processing kills the worker
  is re-claimed after every lease expiry. Once `attempt > max_attempts` it goes to `needs_human` without being
  processed again.

## Risks

- An off-by-one on `max_attempts`. Pinned by tests: with `max_attempts=3`, attempts 1 and 2 retry, and attempt 3
  goes to `needs_human`.

## Proof

- `make check` passes.
- Tests cover:
  - every allowed transition, and a rejected one out of each terminal state;
  - all three outcome kinds;
  - the `max_attempts` boundary;
  - the backoff ceiling, the cap and the jitter range;
  - the crash-loop guard.
