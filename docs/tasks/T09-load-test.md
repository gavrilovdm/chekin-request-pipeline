# T9 — Load test and capacity report

Roadmap: [T9](../sdlc/roadmap.md) · Spec: [NFR intake latency](../sdlc/spec.md#non-functional-requirements), [capacity math](../architecture.html#scaling)

## Files that change

- `loadtest/locustfile.py`: tenants POST at a constant rate (`constant_throughput(1)` per user), plus an occasional GET. Each POST has a unique `Idempotency-Key`. The tenant mix includes `tenant-a`, which has a webhook, so the dispatcher's load is real too.
- `loadtest/run.sh`: one scenario.
  1. Scale the workers.
  2. Reset the mock to 200 ± 50 ms latency.
  3. Run locust headless.
  4. Wait for the backlog to drain.
  5. Write the report.
- `loadtest/report.py`: reads locust's CSV for intake numbers and Postgres for processing numbers. Writes `loadtest/results/<label>.md`.
  - Intake: rps, p50/p95/p99 latency, failures.
  - Processing: done/s, end-to-end p50/p95, statuses, time to drain.
- `loadtest/results/*.md`: the committed reports. Locust's raw CSV and HTML are ignored.
- `Makefile`: `make load WORKERS=1 USERS=300`.

## Scenarios

| Label | Workers | Target intake | Question it answers |
|---|---|---|---|
| `w1-u300` | 1 | ~300 rps (1M/hour) | Does one worker (concurrency 50) keep up with average load? |
| `w4-u300` | 4 | ~300 rps | Does end-to-end latency drop when workers are added? |
| `w4-u900` | 4 | ~900 rps | Where does a laptop stack saturate, and what saturates first? |

With 200 ms downstream latency and 50 in flight per worker, one worker can process at most ~250 rps. That is below 300, so scenario 1 should build a backlog and scenario 2 should not. The run checks this prediction.

## Found while running (see `loadtest/results/README.md`)

- **The sweeper caused a duplicate storm under backlog.** This is a design fix to T6, kept in this PR because the load test is what found it.
  - Files: `ports.SweepStore.take_due(…, recover_lost)`, `PgDispatchStore._TAKE_DUE`, and `Sweeper(queue_depth=…)`, with unit and SQL tests.
  - Lost-message recovery now runs only while the queue is nearly empty.
- **The worker is CPU-bound** at about 121 req/s per process. The prediction of ~250 was wrong: the work is CPU-bound, not I/O-bound.
- **The API runs 2 uvicorn processes** (`docker-compose.yml`). One process saturated a core at about 230 POST/s.
- `tests/integration/test_failures.py`: scenario 1 asserts per key, not on the global success count. Rows that the sweeper recovers from other tests reach the mock during the scenario.
- `loadtest/results/README.md` holds the summary table and the conclusions.
- `docs/sdlc/spec.md`: "Areas of concern" item 2 (sweeper duplicates) is updated with the finding and the fix. Item 3 now says rate limits are per API process.
- The `docker-compose.yml` comment refers to ADR-0006. The ADRs are written in T10, which is stacked on this PR.

## Risks

- **Laptop numbers are not production numbers.** Docker has 4 vCPUs. Locust, the API, workers, Postgres and RabbitMQ all share them. The API runs 2 uvicorn processes after the first run. The report says this and draws conclusions about **shape** (what scales, what saturates first), not absolute capacity.
- **Shared database.** Rows from earlier runs share the database. The report filters by the run's start time.

## Proof

- `make check` passes.
- All three reports are committed with the numbers, along with a summary in `loadtest/results/README.md`. Quoting them in the README is T10's proof.
