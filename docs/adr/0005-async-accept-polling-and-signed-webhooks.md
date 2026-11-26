# ADR-0005: `202 Accepted`, then polling or a signed webhook from a table outbox

**Status:** accepted · T4/T6

## Context
Processing depends on a downstream with seconds of latency and outages. Thousands of tenants integrate
differently: some can receive webhooks, some can only poll.

## Options
| Option | Against |
|---|---|
| Synchronous response | Client holds a connection for the whole processing; downstream outages become API outages |
| Polling only | Wasteful for clients and for us at 1M/h |
| **`202 {id, status_url}` + `GET` + webhook (if configured)** | Webhook delivery is another at-least-once flow to run |

## Decision
- `POST` requires `Idempotency-Key`; replays return the same id, a reused key with another body → `422`.
- Final states set `webhook_pending` **in the same statement** as the status (the `requests` table is the outbox).
- The dispatcher signs the body (`X-Signature: sha256=HMAC(secret, body)`), retries with backoff, and fences updates
  on `webhook_attempt`. Delivery is continuous: at most `per_tenant` in flight per tenant, new jobs claimed only for
  tenants with a free slot, the loop woken when a delivery finishes. One hanging endpoint holds only its own slots
  (T6), and a healthy tenant is bounded by its endpoint latency, not a polling interval (T12: was 8/s).

## Consequences
Tenants deduplicate webhooks by `X-Request-Id`. A tenant whose endpoint is down still has results via `GET`.
