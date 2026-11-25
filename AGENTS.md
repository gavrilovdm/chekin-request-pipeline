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
make check                   # lint + docs rule + unit tests; the definition of "done" for every task
uv run pytest tests/unit     # fast tests, no Docker
make up                      # postgres, rabbitmq, api, worker, dispatcher, downstream-mock
make test-integration        # end-to-end and failure scenarios against the running stack
make load                    # locust load test
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

## How work is done here

Every change is one roadmap task (`docs/sdlc/roadmap.md`) and one PR:
1. Plan: write `docs/tasks/TNN-*.md` (files that change, order, risks, proof).
2. Build: write tests first where the code is pure.
3. Test: run `make check`, plus `make test-integration` from T3 on.
4. Review: run the `verifier` agent against the plan and `REVIEW.md`.
5. Ship: open a PR with real command output.

If the implementation diverges from the task plan, update the plan in the same commit.

## Verifying your work

Do not report a task done without pasting the tail of `make check` output (and `make test-integration` when the
task touches Docker services) into the PR under "How checked".

## Things Claude gets wrong

- Plan drift (T0, T1): a commit changed files or settings its task plan did not list. Before committing, diff the
  plan's "Files that change" against `git status` and update the plan in the same commit.
