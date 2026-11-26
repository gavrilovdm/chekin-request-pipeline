# Spec: request pipeline

Stage: Design · Input: [intent.md](intent.md) · Decisions in detail: [`docs/adr/`](../adr/)

## Components

| Component | Job | Scales by |
|---|---|---|
| `api` | Auth, validation, idempotent insert, publish. Answers `202`. Runs 2 processes per replica | Replicas behind a load balancer |
| `worker` | Claims a request, calls the processor, classifies the outcome, records the result | `--scale worker=N` (KEDA on queue lag in production) |
| `dispatcher` | Sweeper: re-enqueues due retries and expired leases, and lost messages while the queue is nearly empty. Sends webhooks and refreshes the health gauges | Replicas. Coordinated by `SKIP LOCKED`, no leader election |
| `postgres` | Source of truth for request state | Vertical, HA, partitioning (out of scope) |
| `rabbitmq` | Work distribution. Quorum queue, DLX for poison messages | 3-node cluster (out of scope) |
| `downstream-mock` | Fake external service with tunable latency and error rate | — |

## Request lifecycle

```
received ──claim──▶ processing ──ok──────────▶ done
   ▲                    │ ──permanent error──▶ failed
   │                    │ ──retryable error──▶ retry_scheduled ──due──▶ (claim again)
   │                    │ ──attempts exhausted─▶ needs_human
   └── sweeper re-enqueues: lost publish, due retry, expired lease
```

## Functional requirements

1. `POST /v1/requests` takes a bearer API key and an `Idempotency-Key` header.
   - A new request returns `202 {id, status, status_url}`.
   - A replay of the same key and body returns the same `id`.
   - The same key with a different body returns `422`.
2. `GET /v1/requests/{id}` returns status, attempt, result and last error. A tenant sees only its own requests.
3. A terminal state triggers a webhook to the tenant's URL, if one is set.
   - The body is signed: `X-Signature: sha256=<hmac>`.
   - Delivery is retried with backoff and stops after a maximum.
4. Requests that exhaust their attempts go to `needs_human` and are counted in a metric.

## Non-functional requirements

| Requirement | How |
|---|---|
| No loss | Postgres is the source of truth. The queue only carries ids. The sweeper repairs a lost publish or a lost message |
| No duplicate effects | Conditional claim, plus a fencing token (`attempt`) on completion, plus an idempotency key to the downstream |
| Bounded retries | Full-jitter exponential backoff, `max_attempts`, then `needs_human` |
| Crash safety | Lease (`locked_until`). An expired lease is re-claimable. Graceful shutdown on SIGTERM |
| Overload | Per-tenant token bucket (`429`). Load shedding on queue depth (`503` + `Retry-After`) |
| Observability | Prometheus metrics, structured logs with `request_id`, `/healthz` and `/readyz`, alert rules |
| Intake latency | Target: p95 < 50 ms. The request path does one insert and one publish. **Measured (T9):** 11–35 ms unloaded; under ~300 rps on a 4-vCPU laptop shared by every service, p95 is 0.84 s, because the API competes for CPU with the workers and the databases. The target needs the API on its own cores ([load results](../../loadtest/results/README.md)) |

## Areas of concern

1. **Postgres is the bottleneck and a single point of failure.** It takes about 3 writes per request, which is ≈1k writes/s on average and ≈5k at peak. The prototype handles this. Production needs HA, partitioning and retention.
2. **Under backlog the sweeper could re-enqueue duplicates.** The T9 load test confirmed this: 131k duplicates in one run fed the backlog that caused them.
   - Fixed: lost-message recovery runs only when the queue is nearly empty.
   - Due retries and expired leases are never in the queue, so they are always re-published.
3. **The rate limit is per API process.** Without shared state it is approximate: the effective limit is roughly the configured limit × the number of API processes (2 per replica in compose). This is accepted for the prototype; see ADR-0006.
4. **A downstream success can be repeated** if a worker dies after the call but before it records the result. This is at-least-once by nature, so the downstream must accept an idempotency key.

## Out of scope

- Schema migrations tool (the schema only reaches an empty database)
- Postgres-down, fault-under-load and soak tests
- HA Postgres and partitioning
- RabbitMQ cluster
- KEDA
- Fair per-tenant queues
- Circuit breaker
- PII and GDPR retention
- Real authentication (OAuth)

Each is described as a next step in the README.
