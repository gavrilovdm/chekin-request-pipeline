# Load test results

Laptop runs: Docker Desktop with **4 vCPU / 8 GB** shared by every service; locust on the host. The downstream
mock answers in 200 ± 50 ms. Each run: 60 s of load, then wait until every accepted request is final.
Raw per-run reports: [w1-u300](w1-u300.md) · [w4-u300](w4-u300.md) · [w4-u900](w4-u900.md).

| Run | Workers | Offered | Intake rps | Intake p50 / p95 | Done/s | End-to-end p50 / p95 | Lost | Drain |
|---|---|---|---|---|---|---|---|---|
| w1-u300 | 1 | ~300 rps | 267 | 430 / 840 ms | 121 | 48.3 / 69.5 s | 0 | 73 s |
| w4-u300 | 4 | ~300 rps | 173 | 1.3 / 2.9 s | 144 | **1.9** / 19.0 s | 0 | 14 s |
| w4-u900 | 4 | ~900 rps | 168 | 2.7 / 12 s | 136 | 20.4 / 31.7 s | 0 | 19 s |

## What the numbers say

1. **Nothing is lost or duplicated under overload.** Every accepted request reached `done`, `avg attempts` = 1.00,
   no `needs_human`. Backlogs drain after the load stops.
2. **A worker process is CPU-bound, not I/O-bound.** The capacity math assumed ~250/s per worker (50 in flight ×
   200 ms). One worker reached 121/s at 100% of one core: asyncpg, aio-pika, httpx and JSON logging cost ~8 ms
   of CPU per request. That is the GIL limit of one process, which is why the design scales by processes
   (containers), not threads.
3. **Adding workers removes the backlog**: end-to-end p50 drops from 48 s to 1.9 s at the same offered load.
4. **The ceiling here is the laptop's 4 vCPU**, shared by 4 workers, 2 API processes, Postgres and RabbitMQ: with
   4 workers the API loses CPU (intake p50 0.4 s → 1.3 s), and at ~900 rps offered the whole stack tops out
   at ~170 POST/s and ~140 done/s. In production api and workers run on separate nodes and scale independently
   (KEDA on queue depth); the measured per-process cost (~120 req/s per worker core) gives the sizing:
   1M/hour (280 rps) ≈ 3 worker cores, peak ×5 ≈ 12, before any optimisation.

## Found by the load test

- **Sweeper duplicate storm.** With a backlog older than `stale_after`, the sweeper treated queued `received`
  requests as lost and re-published them every sweep: 131k duplicates in one 60 s run, which the worker then
  skipped at full CPU cost, growing the backlog that caused them. Fix: "lost message" recovery runs only when
  the queue is nearly empty (depth < sweep batch); due retries and expired leases are always re-published.
  After the fix: 464 re-publishes in the next run (all genuine), done/s 77 → 121.
- **One API process saturates one core at ~230 POST/s**, so the API runs 2 uvicorn processes.

## Chaos under load (T12)

`loadtest/chaos_campaign.py`: 40 rps of well-behaved clients for 4 minutes, a random fault every 20 s
(kill or freeze a worker or the dispatcher, kill the API, restart RabbitMQ or Postgres, downstream flaky / slow /
down, tenant webhook endpoint down), then heal, drain, replay `needs_human`, and check invariants I1–I6
([`tests/invariants.py`](../../tests/invariants.py)). The final three consecutive runs:
[chaos-8](chaos-8.md) · [chaos-9](chaos-9.md) · [chaos-10](chaos-10.md).

| Run | Accepted | Client gave up | Final | Webhooks failed | Drain | Violations |
|---|---|---|---|---|---|---|
| 8 | 8983 | 0 | all `done` | 0 | 0 s | **0** |
| 9 | 8986 | 0 | all `done` | 0 | 0 s | **0** |
| 10 | 8799 | 0 | all `done` | 0 | 0 s | **0** |

What earlier runs found (each fixed with a regression test):

| Run | Finding | Fix |
|---|---|---|
| 1, 3 | Webhook delivery capped at ~8/s per tenant (4 per claim, 0.5 s pause): a 3k backlog took 7 min | Continuous delivery: claim only for tenants with free slots, wake as soon as a slot frees |
| 2 | Harness bug: killed API/dispatcher were never restarted | The harness plays the orchestrator (compose does not restart a hand-killed container) |
| 4 | Queue check ran while a just-restarted broker was still consuming | "Drained" is checked as eventual (60 s) |
| 6, 7 | Full jitter could fit all retries into ~2 s, so a short blip gave up a webhook or sent a request to `needs_human` | Equal jitter: guaranteed minimum window (7.5 s for processing, ~60 s for webhooks) |
