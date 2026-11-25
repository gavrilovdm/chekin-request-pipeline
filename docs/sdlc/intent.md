# Intent: reliable processing of requests from the public API

Stage: Plan · Source: system design interview with Chekin, follow-up task.

## Problem

Customers send requests to Chekin's public API. There are thousands of customers and about 1M requests per hour.
The volume spikes hard in travel season.

In the interview we agreed on the intake path: auth → validate → persist → enqueue, then workers. The open question
is what happens **after a worker takes a request off the queue**, and how that part scales and survives failures.

## Proposed outcome

A runnable prototype of the processing side. It should show:

- every accepted request reaches a final state, with no silent loss;
- no duplicate side effects, even though delivery is at least once;
- failures are classified: retried with backoff, failed fast, or handed to a human;
- throughput scales by adding stateless workers;
- operators can see when something needs a human.

## Users and systems affected

| Who | How |
|---|---|
| API customers (PMS vendors, property managers) | Send requests. Get a final result by polling or by webhook |
| Operations | Watch metrics and alerts. Handle `needs_human` requests |
| Downstream service | Receives the processed request. Abstract here, mocked in the prototype |

## Constraints

- About 1M requests/hour (≈280 rps average), up to ×5 in peaks.
- Thousands of tenants. One noisy tenant must not starve the others.
- RabbitMQ as the queue (decided in the interview).
- A prototype for a take-home: it must run with `docker compose up` on a laptop.
- Due by Sunday 2026-11-29.

## Open questions (resolved)

| Question | Answer |
|---|---|
| What is a "request"? | An abstract job from a tenant. The domain does not matter, the pipeline does |
| What happens after processing? | Our call. We store the result and notify the tenant by webhook or polling |
| Per-country adapters? | No. One `Processor` interface and one mocked downstream |
