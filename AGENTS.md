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
make test-integration        # end-to-end and failure scenarios against the running stack (~1 min)
make test-chaos              # only the six failure scenarios
docker compose exec dispatcher python -m pipeline replay [--id UUID]   # operator: resume needs_human
make scale N=4               # run 4 workers
make load WORKERS=4 USERS=300   # locust run + markdown report in loadtest/results/
make down                    # stop and delete volumes
scripts/verify.sh docs/tasks/TNN-*.md <branch>   # fresh-context verifier for one task
```

## Structure

- `src/pipeline/domain/` — request state machine, error types, retry policy. No I/O.
- `src/pipeline/ports.py` — small `Protocol` interfaces the core depends on.
- `src/pipeline/worker/handler.py` — claim → process → classify → record. Transport-agnostic.
- `src/pipeline/api/`, `src/pipeline/dispatcher/`, `src/pipeline/adapters/` — FastAPI, sweeper and webhooks,
  asyncpg / aio-pika / httpx implementations of the ports.
- `src/pipeline/main.py` — composition root: the only place concrete adapters are wired; `__main__.py` runs
  `worker` / `dispatcher`; `config.py` — every setting (`PIPELINE_*` env vars).
- `src/pipeline/observability.py` — logging setup and every Prometheus series; `dispatcher/gauges.py` — health gauges.
- `mock_downstream/` — fake external service and webhook sink, faults injectable at runtime via `/admin/config`.
- `migrations/`, `docker-compose.yml` (demo timings: stale_after 15 s, lease 21 s), `loadtest/`, `docs/alerts.yml`.
- `docs/sdlc/` (intent, spec, roadmap), `docs/tasks/` (one plan per task; historical, not updated after merge),
  `docs/adr/`, `docs/architecture.html` (target vs as-built), `REVIEW.md`, `.claude/agents/verifier.md`.

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

Required CI checks (`check`, `integration`) must be green before merge: they are the evidence for `make check` and
`make test-integration`. In "How checked", paste verbatim only what CI does not run (load runs, manual steps), plus
the verifier verdict. Never chain a commit after a check with `;`; use `&&`, so a failed check stops the commit.

## Things Claude gets wrong

- Committed after a failed check (T7): `make check ...; git commit` ran the commit anyway. Chain with `&&`.
- Plan drift (T0, T1): a commit changed files or settings its task plan did not list. Before committing, diff the
  plan's "Files that change" against `git status` and update the plan in the same commit.
