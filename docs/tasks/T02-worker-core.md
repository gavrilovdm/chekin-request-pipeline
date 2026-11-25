# T2 — Worker core: ports and `RequestHandler`

Roadmap: [T2](../sdlc/roadmap.md) · Spec: [non-functional requirements](../sdlc/spec.md#non-functional-requirements)

This is the heart of the take-home: what happens after a worker takes a request id off the queue.

## Files that change

- `src/pipeline/ports.py`
  - `ClaimedRequest`
  - `RequestRepository` (`claim`, `complete`)
  - `Processor` (`process`)
  - `Clock`
- `src/pipeline/worker/handler.py`
  - `RequestHandler.handle(request_id) -> HandleResult`
  - It has no imports of aio-pika, asyncpg or httpx.
- `tests/fakes.py`
  - `InMemoryRepository`, which has the same claim and fencing semantics as the SQL planned for T3
  - `ScriptedProcessor`
  - `FakeClock`
- `tests/unit/test_handler.py`

## Order of work

1. Write the ports: only the methods the handler calls.
2. Write the fakes and the tests for each outcome. Confirm they fail.
3. Implement the handler.

## Design notes

- **Claim → process → classify → complete.**
  - The claim is a conditional update. It returns the request or `None`.
  - `None` means another worker holds it or it is already final. The handler returns `SKIPPED`, and the caller acks.
- **A retry is claimable only when due.** `retry_scheduled` is claimed only once `next_attempt_at <= now`. Otherwise a
  duplicate message (redelivery, sweeper re-publish) would skip the backoff and burn attempts.
- **Error text is clipped** to 2,000 characters in `Outcome` (domain), because it is stored and logged on every attempt.
- **Fencing.** `complete(id, attempt, transition)` applies only if the row is still `processing` with the same `attempt`.
  - Scenario: a slow worker whose lease expired, while another worker re-claimed the row.
  - The slow worker gets `LEASE_LOST` and its write is dropped.
- **Classification.**

  | What happened | How it is classified |
  |---|---|
  | `RetryableError`, timeout | retryable |
  | `PermanentError` | permanent |
  | any other exception | retryable |

  An unknown error is treated as transient, because `max_attempts` bounds it and the error text is kept for the human.
- `asyncio.CancelledError` is not caught, so graceful shutdown can cancel an attempt. The lease then expires and the
  request is re-claimed.
- **Crash-loop guard.** If the claim returns `attempt > max_attempts`, the request goes to `needs_human` without being
  processed.
- `lease` must be longer than the processing `timeout`, otherwise a healthy slow attempt loses its lease. The
  constructor rejects such a config.

## Risks

- The fakes can drift from the SQL. Mitigation: T3 runs the same claim and fencing scenarios against real Postgres.

## Proof

- `make check` passes.
- `test_handler.py` covers each of these:
  1. done
  2. retry scheduled
  3. needs_human on the last attempt
  4. failed
  5. skipped (already final; held by a live lease)
  6. lease lost (a stale worker cannot overwrite)
  7. timeout
  8. crash-loop guard
  9. unexpected exception
  10. a duplicate delivery calls the processor once
  11. config validation
