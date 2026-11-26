# ADR-0007: One asyncio process per container; scale by containers

**Status:** accepted · T5, measured in T9

## Context
The interview asked: worker vs container vs thread vs process, and their limits.

| Unit | Limit |
|---|---|
| Coroutine | Blocked by any synchronous call; bounded here by prefetch |
| Thread | Shares the GIL: no CPU parallelism in CPython; adds memory and context switches |
| Process | One core of CPU-bound Python (GIL); own memory and connection pool |
| Container | CPU/memory quota; DB connections are the shared ceiling across all of them |

## Decision
Each worker container runs one asyncio process with `prefetch = concurrency` (50) in flight. Horizontal scaling is
by container count (`--scale`, KEDA on queue depth in production). The API runs 2 uvicorn processes.

## Consequences
T9 measured one worker process at **~120 requests/s at 100% of a core**: the per-request CPU cost (asyncpg,
aio-pika, httpx, JSON logs) dominates, not I/O waiting. Sizing therefore follows CPU: 1M/h ≈ 3 worker cores,
peak ×5 ≈ 12. Threads would not help; more processes do.
