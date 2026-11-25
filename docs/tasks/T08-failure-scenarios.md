# T8 — Failure scenarios

Roadmap: [T8](../sdlc/roadmap.md) · Target: the "Failure modes" table on the [architecture page](../architecture.html)

## Files that change

- `tests/integration/test_failures.py`: five scenarios against the live stack. They use `docker compose` to kill and stop
  services and the mock's `/admin/config` to inject faults.
- `mock_downstream/app.py`: `fail_first` config. Each `Idempotency-Key` fails with 503 for its first N calls, then succeeds.
  This makes the flaky-downstream scenario deterministic: 50% random errors with `max_attempts=5` would flake about 3% of runs.
- `docker-compose.yml`: demo timings for the dispatcher and workers.
  - `stale_after_s=15` and `lease_s=15`, so recovery happens within a test's patience.
  - Each override carries a comment. The code defaults (60 s and 30 s) stay as the production values.
- `tests/integration/conftest.py` gets the shared `api` and `mock` fixtures, and `tests/integration/helpers.py` gets `A`, `submit` and `wait_for`. Before this they lived in `test_e2e.py`, and importing fixtures across test modules tripped ruff F811.
- `Makefile`: `make test-chaos` runs only these tests. `make test-integration` still runs everything.
- `pyproject.toml`: a `chaos` marker.

## Order of work

1. Add `fail_first` to the mock.
2. Write the scenarios one by one. Each restores the stack in a `finally`, so one failure does not cascade into the others.

## Scenarios and invariants

| # | Fault | Expected | Invariant checked |
|---|---|---|---|
| 1 | `docker compose kill worker` while a request is being processed (mock latency 3 s), then restart | `done` | at most one successful downstream effect for the id (`Idempotency-Key`) |
| 2 | downstream fails the first 2 calls per request | `done` at `attempt = 3` | no duplicate effect |
| 3 | downstream down | `needs_human` after `max_attempts`; `last_error` kept | the downstream saw exactly `max_attempts` calls |
| 4 | RabbitMQ stopped at intake | `202`; once RabbitMQ is back, `done` without any client retry | the sweeper re-published it |
| 5 | client replays a POST (same key and body) | same id, one row, one downstream effect | — |

## Risks

- **Slow tests.** Scenarios 1 and 4 wait for a lease or the staleness window. With the demo timings the suite takes about 1–2 minutes.
- **Flakiness.** The scenarios poll with deadlines instead of sleeping fixed times.

## Proof

- `make check` passes.
- `make test-chaos` passes all five scenarios.
- `make test-integration` passes in full.
