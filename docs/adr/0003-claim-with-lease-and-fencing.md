# ADR-0003: Claim with a lease, complete with a fencing token

**Status:** accepted · T2/T3

## Context
Delivery is at least once. A message can arrive twice, a worker can stall past its lease while another worker
takes over, and a worker can die after the downstream succeeded but before recording it.

## Options
| Option | Against |
|---|---|
| Dedupe table of processed message ids | Does not stop two concurrent workers on the same request; grows forever |
| Distributed lock (Redis) | Extra system; a lock without fencing still lets a stale holder write |
| **Conditional `UPDATE … RETURNING` claim + lease + `attempt` as fencing token** | — |

## Decision
- **Claim:** `status → processing, attempt += 1, locked_until = now() + lease` only if the row is `received`, a
  *due* `retry_scheduled`, or `processing` with an expired lease. Lease uses the DB clock.
- **Complete:** applies only `WHERE status = 'processing' AND attempt = $n`; a stale worker's write is dropped
  (`LEASE_LOST`).
- **Downstream:** `Idempotency-Key = request_id`, so a repeat after a crash returns the original result.
- **Crash loop guard:** a claim whose round attempt (`attempt − attempt_offset`) exceeds `max_attempts` goes to
  `needs_human` without processing.
- **Operator replay (T11):** `needs_human → retry_scheduled` sets `attempt_offset = attempt`. `attempt` itself
  never resets, so a zombie worker from an earlier round can never match the fencing token.

## Consequences
- Lease must exceed the worst-case attempt: processing timeout + 2 × DB command timeout (claim and complete),
  enforced by `Settings` at start-up.
- A retry is claimable only when due: a duplicate message cannot skip the backoff (verifier finding, T2).
