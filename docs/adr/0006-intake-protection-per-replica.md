# ADR-0006: Intake protection: per-tenant token bucket and queue-depth shedding, per replica

**Status:** accepted for the prototype · T4

## Context
One tenant's burst (a PMS re-syncing a season) must not starve the others, and the API must not accept work faster
than it can ever be processed.

## Options
| Option | For | Against |
|---|---|---|
| **In-memory token bucket per tenant, per API process** | No extra hop or system | Effective limit × number of processes |
| Redis token bucket | Exact global limit | Extra system and a network hop on every request |
| API gateway / WAF rate limiting | Also handles per-IP abuse | Outside the service |

## Decision
Per-process buckets (limits from the `tenants` table), `429 + Retry-After`. Load shedding: if the queue depth is
above `max_queue_depth`, `503 + Retry-After` before any write; depth refreshed in the background with a timeout
and **fail-open** when unknown (Postgres is the truth; the sweeper catches up).

## Consequences
Limits are approximate with N API processes (N = 2 in compose). Production: move tenant limits to the gateway or
Redis, keep shedding local.
