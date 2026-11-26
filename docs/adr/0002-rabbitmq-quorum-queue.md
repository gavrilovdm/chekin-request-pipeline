# ADR-0002: RabbitMQ quorum queue for work distribution

**Status:** accepted (agreed in the interview) · T3

## Context
Workers need a push-based work queue with acknowledgements, back-pressure (prefetch) and a dead-letter path.

## Options
| Option | For | Against |
|---|---|---|
| **RabbitMQ quorum queue** | Acks + prefetch = natural concurrency control; DLX; replicated (Raft) | Another stateful system to run |
| AWS SQS | Fully managed, visibility timeout built in | Cloud-specific; 120k in-flight limit; no local parity |
| Kafka | Huge throughput, replay | Partition-level parallelism makes per-message retries and acks awkward |
| Postgres `SKIP LOCKED` as the queue | One system fewer | Polling load on the hot table; mixes queueing with state at 1M/h |

## Decision
Quorum queue `requests` (durable, replicated), publisher confirms + `mandatory` so a publish that is not stored or
not routed raises, manual acks, prefetch = worker concurrency, DLX to `requests.dead` for unparseable messages.

## Consequences
- A publish failure is visible to the API immediately; the request stays `received` and the sweeper recovers it.
- Ordering is not guaranteed and not needed: requests are independent.
