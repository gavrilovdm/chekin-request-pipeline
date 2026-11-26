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
- The dispatcher signs the body (`X-Signature: sha256=HMAC(secret, body)`), retries with backoff, caps in-flight
  deliveries **and claims** per tenant (one hanging endpoint cannot stall others, verifier finding T6), and fences
  updates on `webhook_attempt`.

## Consequences
Tenants deduplicate webhooks by `X-Request-Id`. A tenant whose endpoint is down still has results via `GET`.
