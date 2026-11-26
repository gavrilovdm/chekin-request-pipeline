# ADR-0004: Retries are scheduled in the database; a sweeper re-publishes

**Status:** accepted · T6, revised in T9

## Context
The interview diagram retried the publish 1-2-4-8 s inside the API request. Processing failures also need
backoff. Something must also repair lost messages and expired leases.

## Options
| Option | Against |
|---|---|
| Retry inside the HTTP request (1-2-4-8 s) | Holds the client up to 15 s and still needs a fallback when all retries fail |
| RabbitMQ delay queues (TTL + DLX per backoff step) | Several extra queues; state split between broker and DB; no answer for lost messages |
| **`retry_scheduled` + `next_attempt_at` in the row; one sweeper re-publishes due, lost and expired-lease rows** | Retry latency = backoff + ≤1 sweep interval |

## Decision
Full-jitter exponential backoff (`uniform(0, min(60, 2^(n-1)))` s), `max_attempts` then `needs_human`. The sweeper
selects due rows with `FOR UPDATE SKIP LOCKED` (any number of replicas, no leader), stamps `enqueued_at`, then
publishes. **T9 revision:** "lost message" recovery (staleness) runs only while the queue is nearly empty; under a
backlog it re-published queued requests as duplicates (131k in one run) and fed the backlog.

## Consequences
One mechanism covers retries, lost publishes and dead workers. Recovery of a genuinely lost message waits until a
backlog drains, which is acceptable because losses are rare (broker outage) and are counted in metrics.
