# T6 — Dispatcher: sweeper and webhooks

Roadmap: [T6](../sdlc/roadmap.md) · Spec: [no loss, webhooks](../sdlc/spec.md#non-functional-requirements)

## Files that change

- `src/pipeline/ports.py`: `SweepStore`, `WebhookStore`, `WebhookJob`.
- `src/pipeline/adapters/postgres.py`: `PgDispatchStore`. Due rows are selected with `FOR UPDATE SKIP LOCKED`, so any number of dispatchers can run.
- `src/pipeline/dispatcher/sweeper.py`: `Sweeper.sweep_once()`. It re-publishes ids that are lost, due, or whose lease expired.
- `src/pipeline/dispatcher/webhooks.py`: `WebhookSender.send_due_once()`. It signs with HMAC-SHA256, applies a per-tenant concurrency cap and a timeout, and retries with backoff up to a maximum.
- `src/pipeline/main.py`: `run_dispatcher`. `__main__.py`: `python -m pipeline dispatcher`. `docker-compose.yml`: a `dispatcher` service.
- Tests:
  - `tests/unit/test_sweeper.py` and `tests/unit/test_webhooks.py` (fakes);
  - `tests/integration/test_dispatch_sql.py`;
  - `tests/integration/test_e2e.py`, extended with a lost publish and a signed webhook.

## Order of work

1. Write unit tests for the sweeper and the webhook sender on fakes, then implement both.
2. Write SQL integration tests for due selection, then implement `PgDispatchStore`.
3. Wire the dispatcher service. Write e2e tests for recovery of a lost publish and for webhook delivery.

## Design notes

### Sweeper

One statement picks up to `sweep_batch` ids and stamps `enqueued_at = now()`. After the commit, it publishes them. A row is due when one of these holds:

| Status | Trigger | Not yet re-published when |
|---|---|---|
| `received` | `coalesce(enqueued_at, created_at) < now() − stale_after` | (the trigger covers it) |
| `retry_scheduled` | `next_attempt_at <= now()` | `enqueued_at` is null, or before `next_attempt_at`, or older than `stale_after` |
| `processing` | `locked_until < now()` | `enqueued_at` is null, or before `locked_until`, or older than `stale_after` |

The "not yet re-published" condition prevents re-publishing the same row on every tick. It still re-publishes if that message was lost (older than `stale_after`).

If a publish fails after the stamp, the row is picked again after `stale_after`. Nothing is lost.

This is the only path that publishes retries. A retry's latency is therefore its backoff plus at most one sweep interval (1 s).

### Webhooks

`webhook_pending` on `requests` is the outbox. T3's `complete` sets it in the same statement as the final status.

The sender claims due rows (`SKIP LOCKED`). It also pushes `webhook_next_at` forward by a visibility window, so a crashed sender's rows come back on their own. Then it POSTs each job:
- headers: `X-Signature: sha256=<hmac(secret, body)>`, `X-Request-Id`, `X-Webhook-Attempt`;
- timeout: `webhook_timeout_s`;
- at most `webhook_per_tenant_concurrency` in flight per tenant, so one slow endpoint cannot hold everyone's deliveries.

| Result | What is stored |
|---|---|
| 2xx | `webhook_status = delivered` |
| anything else | backoff with the domain `RetryPolicy`; after `webhook_max_attempts`, `webhook_status = failed` |

Delivery is at least once. The tenant deduplicates by `X-Request-Id`.

## Risks

- **Re-publishing during a backlog.** If the queue lag exceeds `stale_after`, the sweeper re-publishes `received` rows that are still queued.
  - Mitigations: `stale_after` (60 s) is set well above normal lag, load shedding bounds the lag, and a duplicate costs one failed `UPDATE`.
  - Recorded in the spec's areas of concern.

## Proof

- `make check` passes, and the unit tests cover:
  - sweeper: due ids are published; a publish failure does not stop the batch;
  - webhooks: a valid signature; per-tenant concurrency is capped; failure → backoff; max attempts → failed.
- `make test-integration` passes, and covers:
  - due selection for each row type;
  - a not-due retry is excluded;
  - a fresh stamp is not picked twice;
  - e2e: a row with no publish (inserted directly) reaches `done`;
  - e2e: a `done` request's webhook arrives at the mock with a valid HMAC.
