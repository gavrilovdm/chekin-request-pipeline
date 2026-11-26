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
- `pyproject.toml`: `hypothesis` in the dev group; ruff `ASYNC110` ignored in `tests/**` (tests poll observable state on purpose).
- `mock_downstream/app.py`: `webhooks_down`, a fault for the campaign (the tenant endpoint answers 503).
- `src/pipeline/config.py`: `webhook_max_in_flight`, which bounds both the in-flight deliveries and the HTTP pool (review fix).
- Fixes for whatever the campaign finds, each with a regression test, and the living docs.

## Proof

- `make check` passes, including the property tests.
- 3 consecutive campaign reports with zero violations are committed.
- Every bug found has a regression test and a line in this plan.

## What the campaign found

| Found by | Finding | Fix and regression test |
|---|---|---|
| Property tests | None in the code. The first version of the test itself was too weak: all five injected mutations passed | Safety invariants added (claim under a live lease, retry before due, stale or other-round writer); `test_zombie_from_before_a_replay_cannot_complete_the_new_round` |
| Campaigns 1, 3 | Webhooks capped at ~8/s per tenant | `WebhookSender.run`: continuous, tenant-aware claims (`exclude_tenants`); `test_healthy_tenant_throughput_is_not_capped_by_the_idle_interval`, `test_slow_tenant_does_not_hold_back_a_fast_one`, `test_webhook_claim_skips_saturated_tenants` |
| Campaign 2 | Harness: restart command failed for non-worker services | `kill_restart` scales only the worker |
| Campaign 4 | Queue check not eventual | I5 polls for 60 s |
| Campaigns 6, 7 | Full jitter made the retry window random and sometimes ~2 s | Equal jitter in `RetryPolicy`; `test_retry_window_has_a_guaranteed_minimum` |

The campaigns ran with the docs-check hook live. It flagged 10 missing docstrings in the new campaign script, which were fixed before committing.

Result: campaigns 8, 9 and 10 passed in a row with 0 violations. Reports are in `loadtest/results/`.
