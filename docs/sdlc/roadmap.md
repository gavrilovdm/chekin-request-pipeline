# Roadmap

Each task runs through one AI-native SDLC cycle and lands as one PR:

| Step | What happens | Artifact |
|---|---|---|
| Plan | A task plan lists the files that change, the order of work, the risks and the proof | `docs/tasks/TNN-*.md` |
| Build | Tests first where the code is pure, then the implementation | code + tests |
| Test | `make check` (lint + unit) and, from T3 on, `make test-integration` | command output in the PR |
| Review | A fresh-context verifier agent checks the diff against the task plan and [REVIEW.md](../../REVIEW.md) | findings in the PR |
| Ship | PR with Why / What changed / How checked; CI green; merge | merged PR |
| Learn | A mistake made twice goes into `AGENTS.md` → "Things Claude gets wrong" | commit |

## Tasks

| # | Task | Proof (as planned) | Depends on | Status |
|---|---|---|---|---|
| T0 | Foundation: intent, spec, roadmap, tooling, CI, review rules | `make check` passes on an empty package; CI green | — | merged ([#1](https://github.com/gavrilovdm/chekin-request-pipeline/pull/1)) |
| T1 | Domain: state machine, retry policy, error types | unit tests for every transition and the backoff bounds | T0 | merged ([#2](https://github.com/gavrilovdm/chekin-request-pipeline/pull/2)) |
| T2 | Worker core: ports and `RequestHandler` | unit tests on fakes: success, retry, permanent, exhausted, skipped, lease lost, timeout, poison | T1 | merged ([#3](https://github.com/gavrilovdm/chekin-request-pipeline/pull/3)) |
| T3 | Persistence and messaging: schema, asyncpg repo, RabbitMQ topology and publisher, compose with postgres and rabbitmq | integration tests for claim, fencing and idempotent insert against real Postgres | T2 | merged ([#4](https://github.com/gavrilovdm/chekin-request-pipeline/pull/4)) |
| T4 | Intake API: auth, idempotency, rate limit, load shedding, publish | API tests on fakes: 202, replay, 422, 401, 429, 503, publish failure still 202 | T3 | merged ([#5](https://github.com/gavrilovdm/chekin-request-pipeline/pull/5)) |
| T5 | Worker runtime and downstream mock: consumer, HTTP processor, graceful shutdown; full compose | e2e: POST → `done` | T4 | merged ([#6](https://github.com/gavrilovdm/chekin-request-pipeline/pull/6)) |
| T6 | Dispatcher: sweeper and webhooks | e2e: a lost publish is recovered; a webhook arrives with a valid signature | T5 | merged ([#7](https://github.com/gavrilovdm/chekin-request-pipeline/pull/7)) |
| T7 | Observability: metrics, structured logs, health, alert rules | `/metrics` exposes the documented series; `/readyz` fails when the DB is down | T6 | merged ([#8](https://github.com/gavrilovdm/chekin-request-pipeline/pull/8)) |
| T8 | Failure scenarios: worker killed, downstream flaky, downstream down, RabbitMQ down, idempotent replay | `make test-integration` covers all five | T7 | merged ([#9](https://github.com/gavrilovdm/chekin-request-pipeline/pull/9)) |
| T9 | Load test and capacity report | locust run, throughput and p95 for 1 and 4 workers | T8 | merged ([#10](https://github.com/gavrilovdm/chekin-request-pipeline/pull/10)) |
| T10 | README, ADRs, as-built architecture vs. target (+ CI integration job) | the architecture page compares target and as-built; gaps fixed or explained | T9 | merged ([#11](https://github.com/gavrilovdm/chekin-request-pipeline/pull/11)) |
| T11 | Edge cases found by probing: API input errors, operator replay for `needs_human`, lease budget, result cap | every malformed input is a 422; replay chaos scenario | T10 | merged ([#17](https://github.com/gavrilovdm/chekin-request-pipeline/pull/17)) |
| T12 | Fault campaign: property-based lifecycle tests, invariant checker, chaos under load until 3 clean runs | 3 consecutive clean campaign reports | T11 | in review |

## Schedule

| Day | Tasks |
|---|---|
| Thu 26.11 | T0, T1, T2 |
| Fri 27.11 | T3, T4, T5 |
| Sat 28.11 | T6, T7, T8 |
| Sun 29.11 | T9, T10. Review with the user, then make the repo public |
