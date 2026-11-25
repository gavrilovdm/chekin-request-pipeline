# T7 — Observability

Roadmap: [T7](../sdlc/roadmap.md) · Spec: "operators can see when something needs a human" ([intent](../sdlc/intent.md))

## Files that change

- `src/pipeline/observability.py`
  - `configure_logging()`: structlog JSON with ISO timestamps and log level. `request_id` is bound by callers.
  - One module that defines every Prometheus series, so each name exists in exactly one place.
- `src/pipeline/api/app.py`
  - Intake counters by outcome and an intake latency histogram.
  - `/metrics`.
  - `/readyz`: Postgres and RabbitMQ checks. It answers `503` when either fails.
- `src/pipeline/worker/consumer.py`: handled-by-result counter, attempt duration histogram, in-flight gauge.
- `src/pipeline/dispatcher/sweeper.py`, `src/pipeline/dispatcher/webhooks.py`: republished-by-status counter, webhook-outcome counter.
- `src/pipeline/dispatcher/gauges.py` and `PgDispatchStore.health_snapshot()`: every 10 s, the dispatcher sets four gauges:
  - the `needs_human` count;
  - the age of the oldest unfinished request (lag that the user can feel);
  - the number of pending webhooks;
  - the queue depth.
- `docker-compose.yml`: the dispatcher's `/metrics` is published on host port 9101.
- `src/pipeline/main.py`: logging setup. Worker and dispatcher expose `/metrics` on `metrics_port` (9100), and the dispatcher's is published on host port 9101.
- `migrations/003_unfinished_index.sql`: a partial index on `created_at` for unfinished requests, which keeps the oldest-unfinished query cheap.
- `src/pipeline/config.py`: `log_level`. The `httpx` logger is set to WARNING.
- `/metrics` on the API is a plain route, not a mounted app. A mount redirects `/metrics` to `/metrics/`.
- `docs/alerts.yml`: Prometheus alert rules with their thresholds and the reason for each.
- Tests:
  - `tests/unit/test_observability.py`: API metrics and readiness on fakes, consumer metrics;
  - `tests/integration/test_observability.py`: API `/metrics` and dispatcher gauges on the live stack.

## Order of work

1. Write the series definitions and the logging setup.
2. Write the unit tests for the counters at each call site, then instrument.
3. Add readiness, then write the gauges' SQL with an integration test.
4. Write the alert rules.

## Design notes

- **Labels have bounded cardinality.** Statuses and outcomes only. There are no tenant or request ids in labels: thousands of tenants would explode the series count. Ids go to logs.
- **The best single health signal is the age of the oldest unfinished request.**
  - It rises when workers die, when the queue is lost, when the downstream is down, or when the sweeper stops.
  - It holds whatever the volume, unlike queue depth, which is normal during a spike.
- **`/healthz` and `/readyz` answer different questions.**
  - `/healthz` is liveness: is the process up? A restart fixes a failure here.
  - `/readyz` is readiness: can the process serve now? The load balancer drains a replica that fails it.

## Series added beyond the first draft

- `pipeline_publish_failures_total` (API): publishes that the sweeper will recover.
- `pipeline_republish_failures_total` (sweeper).
- `pipeline_poison_messages_total` (worker): messages sent to the DLQ.
- `pipeline_intake_total{outcome="conflict"}`: an Idempotency-Key reused with a different body, kept apart from schema errors (`invalid`).
- `pipeline_intake_total{outcome="error"}`: unhandled 500s, counted in the middleware.

## Risks

- **The default Prometheus registry is shared across tests.** Tests compare deltas, not absolute values.

## Proof

- `make check` passes, and the unit tests cover:
  - an accepted, a rate-limited and a shed request each move their counter;
  - `/metrics` exposes `pipeline_intake_total`;
  - `/readyz` answers `200` when healthy and `503` when the DB check fails;
  - the consumer counts a handled delivery by result.
- `make test-integration` passes, and shows:
  - the API `/metrics` has intake series after a POST;
  - the dispatcher `/metrics` has the `needs_human` and oldest-unfinished gauges.
