# chekin-request-pipeline

Parent: [hub/projects/work](https://github.com/gavrilovdm/hub/blob/main/domains/projects/work/AGENTS.md)

Links: none

## What it is

Take-home prototype for Chekin: a fault-tolerant pipeline behind a public API that accepts requests from
thousands of tenants (~1M/hour), queues them in RabbitMQ and processes them with stateless asyncio workers.
The focus is what happens after a worker takes a request off the queue. Decisions live in `docs/adr/`.

## Commands

```bash
uv sync                      # install
uv run pytest tests/unit     # fast tests, no Docker
make up                      # postgres, rabbitmq, api, worker, dispatcher, downstream-mock
make test-integration        # end-to-end and failure scenarios against the running stack
make load                    # locust load test
uvx ruff check .             # lint
```

## Structure

- `src/pipeline/domain/` — request state machine, error types, retry policy. No I/O.
- `src/pipeline/ports.py` — small `Protocol` interfaces the core depends on.
- `src/pipeline/worker/handler.py` — claim → process → classify → record. Transport-agnostic.
- `src/pipeline/api/`, `src/pipeline/dispatcher/`, `src/pipeline/adapters/` — FastAPI, sweeper and webhooks,
  asyncpg / aio-pika / httpx implementations of the ports.
- `src/pipeline/main.py` — composition root: the only place concrete adapters are wired.
- `mock_downstream/` — fake external service with tunable latency and error rate.
- `migrations/`, `docker-compose.yml`, `loadtest/`, `docs/adr/`.

## Conventions and pitfalls

- All repo content is in English: it is read by the hiring team.
- State changes go through `domain/request.py`; never `UPDATE status` around it.
- Every handler must stay idempotent: messages are delivered at least once.
