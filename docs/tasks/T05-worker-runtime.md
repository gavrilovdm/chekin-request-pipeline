# T5 — Worker runtime and downstream mock

Roadmap: [T5](../sdlc/roadmap.md) · Builds on: [T2 handler](T02-worker-core.md), [T3 adapters](T03-persistence.md)

## Files that change

- `src/pipeline/worker/consumer.py`: `Consumer`. It is the RabbitMQ transport around `RequestHandler`, with graceful shutdown.
- `src/pipeline/adapters/downstream.py`: `HttpProcessor`, which implements `Processor`. It maps HTTP results to
  `RetryableError` and `PermanentError`.
- `src/pipeline/main.py`: `run_worker`. `src/pipeline/__main__.py`: `python -m pipeline worker`.
- `mock_downstream/app.py`: the fake external service.
  - `POST /process` honours `Idempotency-Key`.
  - Latency and error rates can be changed at runtime through `/admin/config`.
  - `/admin/stats` counts calls and duplicates.
  - It also works as a webhook sink (`POST /webhooks/{tenant}`) for T6.
- `docker-compose.yml`: `worker` (scalable, `stop_grace_period`) and `downstream-mock` on port 8081.
- `src/pipeline/adapters/rabbitmq.py`: the queue name is a parameter of `declare_topology` and `RabbitPublisher` (dead-letter names derived), and `tests/integration/test_rabbitmq.py` uses a private queue per test. With a live worker in compose, the shared `requests` queue is consumed, so the T3 broker tests stole each other's messages.
- `tests/unit/test_consumer.py`, `tests/unit/test_downstream.py`, `tests/integration/test_e2e.py`.

## Order of work

1. Write the consumer tests with fake messages, then implement the consumer.
2. Write the processor tests with `httpx.MockTransport`, then implement the processor.
3. Write the mock service. Wire the worker into compose.
4. Write the end-to-end test: POST → `done` with the downstream result.

## Design notes

**Ack policy: every delivery is acked once the handler returns, whatever the result.** The message is only a hint; Postgres holds the state.
- If the handler raises because of an infrastructure problem (for example the DB is down), the consumer still acks. The row keeps its claimable or leased state, and the sweeper re-publishes it.
- The alternative, nack with requeue, would spin hot on every prefetched message while the DB is down.

**Poison messages.** A body that is not a UUID is rejected without requeue and lands in `requests.dead`.

**Concurrency.** Prefetch is `worker_concurrency`, and each delivery runs as its own task. The number of unacked deliveries, and so the number in flight, cannot exceed the prefetch. The httpx pool size equals the concurrency.

**Graceful shutdown on SIGTERM.**
1. Stop consuming.
2. Wait up to `shutdown_grace_s` for in-flight attempts.
3. Cancel whatever is left. Those deliveries stay unacked, so RabbitMQ re-queues them when the channel closes.

A redelivered message may be skipped while the old lease is still alive. Once the lease expires, the sweeper (T6) re-publishes the request.

**HTTP mapping.**

| Downstream response | Error |
|---|---|
| timeout, connection error, `429`, `5xx` | `RetryableError` |
| `3xx`, other `4xx`, a `2xx` with a non-JSON body | `PermanentError` |
| `2xx` | success; the JSON body is the result, an empty body is `{}` |

`Idempotency-Key` is set to the request id, so a repeat after a crash between "downstream succeeded" and "complete" is deduplicated on the downstream side.

## Risks

- **Until T6 (sweeper) lands, two paths leave a request stuck with no message.**
  - Shutdown cancels an attempt. RabbitMQ redelivers at once, while the lease is still alive, so the redelivery is skipped and acked. The row stays `processing`.
  - The handler raises (for example the DB is down) and the delivery is acked. The row stays `received` or `retry_scheduled`.

  Both are by design: the message is only a hint. The T6 sweeper is what recovers them.

## Proof

- `make check` passes, and the unit tests cover the consumer and the HTTP mapping.
  - Consumer: a valid id → handled and acked; a bad body → rejected; a handler crash → acked; shutdown waits for in-flight work; cancellation leaves the delivery unacked.
  - HTTP mapping: 200, 503, 429, 422, timeout, connection error, and the idempotency header.
- `make up && make test-integration` passes, including e2e: POST → `done`, with the result stored and visible via GET.
