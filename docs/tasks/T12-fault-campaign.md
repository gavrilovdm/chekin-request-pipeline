# T12 — Fault campaign: every step, every failure point, until clean

Roadmap: [T12](../sdlc/roadmap.md)

## Goal and "done"

"Fault tolerant" here means one thing: these invariants hold **after any sequence of the faults below, under load**, with no human action except the documented replay of `needs_human`.

| # | Invariant |
|---|---|
| I1 | Every accepted request reaches a final state (`done`, `failed`, `needs_human`) within a bounded time after the faults stop |
| I2 | No request completes with two different results. The downstream stores at most one effect per request id |
| I3 | Every `done` request has a result, and every `failed` or `needs_human` request has a `last_error` |
| I4 | Every final state of a tenant with a webhook is eventually delivered or marked `failed`. No pending webhook is left behind |
| I5 | The services keep running: no crash loops, `/readyz` green, the queue drained |
| I6 | The state machine is never violated: no `IllegalTransition` in any log |

The task is done when the campaign passes **3 consecutive runs** with zero invariant violations.

## Failure points per step

| Step | Fault | Expected |
|---|---|---|
| Intake insert | DB down or stopping | `503` (or a reset connection), nothing committed; the client retries with the same key |
| Intake publish | broker down, confirm lost | `202`; the sweeper publishes later |
| Deliver | duplicate, poison, queue deleted | claim dedupes; poison goes to the DLQ; the queue is re-declared |
| Claim | DB down | delivery acked; the row is untouched; the sweeper re-publishes |
| Process | downstream slow, 5xx, down, hangs | timeout, retry with backoff, `needs_human` |
| Process | worker killed (SIGKILL) | lease expires, re-claimed, idempotency key dedupes |
| Process | worker **frozen** (`docker pause`) past the lease, then resumed | the zombie's completion is fenced off |
| Complete | DB down after downstream success | re-processed, downstream returns the stored result |
| Sweep | dispatcher killed or frozen | another run picks up (`SKIP LOCKED`); stamps expire |
| Webhook | tenant endpoint down | backoff, then `failed`; other tenants unaffected |
| Everything | API, worker, dispatcher, Postgres or RabbitMQ restart under load | I1–I6 |

## Files that change

- `tests/unit/test_handler_properties.py`: Hypothesis.
  - Random interleavings of deliveries, duplicate deliveries, crashes, lease expiries, zombie completions and downstream outcomes, run against the handler and the in-memory repo.
  - Invariants: the request reaches a final state, the transitions are legal, at most one accepted completion per attempt, and the result is never overwritten.
- `tests/invariants.py`: the I1–I4 checker over Postgres and the mock, for one run window. It prints the violations and exits non-zero.
- `loadtest/chaos_campaign.py`: steady load plus a random fault every *N* seconds from the table above. When the load stops, it waits for the drain, then runs the invariant checker and a log scan (I5, I6). Writes a report to `loadtest/results/chaos-<n>.md`.
- `Makefile`: `make chaos-campaign`.
- `pyproject.toml`: `hypothesis` in the dev group.
- Fixes for whatever the campaign finds, each with a regression test, and the living docs.

## Proof

- `make check` passes, including the property tests.
- 3 consecutive campaign reports with zero violations are committed.
- Every bug found has a regression test and a line in this plan.
