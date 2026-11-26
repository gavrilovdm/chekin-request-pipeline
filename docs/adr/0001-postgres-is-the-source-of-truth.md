# ADR-0001: Postgres is the source of truth; the queue carries only request ids

**Status:** accepted · T2/T3

## Context
At ~1M requests/hour, every component will fail at some point: the broker restarts, a worker dies mid-request, a
publish times out after the insert committed. With at-least-once delivery the system must reach a correct final
state for every accepted request anyway.

## Options
| Option | For | Against |
|---|---|---|
| Queue is the truth (payload in the message, state in the consumer) | One write path | A lost or duplicated message is a lost or duplicated request; no global view of state; replays need the broker's history |
| Kafka log as the truth | Replayable, ordered | Per-request state machine (retries, leases) still needs a store; heavier to operate for a work queue |
| **Postgres row is the truth, message = id** | One place to ask "what state is X in"; duplicates and losses repairable from the table | Postgres becomes the hot path and the single point of failure (≈3 writes per request) |

## Decision
Every request is a row with a status. RabbitMQ messages carry only the id and are treated as a hint that there may
be work. Workers claim the row with a conditional `UPDATE`, and the sweeper re-publishes anything the queue lost.

## Consequences
- Duplicate deliveries are harmless (the claim lets one through) and lost messages are recoverable.
- Postgres throughput bounds the system: ~1k writes/s average, ~5k at peak. Production needs HA (Patroni/managed),
  partitioning `requests` by day, and retention.
