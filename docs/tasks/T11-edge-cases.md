# T11 — Edge cases found by probing the live stack

Roadmap: [T11](../sdlc/roadmap.md) · Source: an edge-case probe after T10.

The probe ran against the live stack:
- Postgres down with 30 requests in flight;
- the work queue deleted;
- malformed API input.

The infrastructure faults were absorbed without loss or duplicate effects. The probe found these:

| # | Finding | Impact |
|---|---|---|
| 1 | Malformed input returns `500` or drops the connection instead of `422`: invalid JSON, empty body, JSON array, deep nesting, a key over 200 chars | The `422` detail carried the raw body bytes (`input`), which are not JSON-serialisable. The keep-alive connection was then broken for the next request |
| 2 | `\u0000` in data → `500` | Postgres `jsonb` cannot store NUL |
| 3 | `needs_human` has no way out | After the downstream is fixed, an operator cannot resume the requests, although `docs/alerts.yml` says "replay after the fix" |
| 4 | Compose lease (15 s) is shorter than the worst-case attempt (5 s claim + 10 s processing + 5 s complete) | A slow but healthy attempt can lose its lease and be processed twice. Fencing and idempotency keep it correct, but the work is wasted |
| 5 | DB unavailable → `500`; unknown body fields silently ignored; downstream result size unbounded | Clients cannot tell "retry later" from a bug; typos go unnoticed; one huge result bloats the table |

## Files that change

- `src/pipeline/api/app.py`
  - validation errors without `input` → always serialisable;
  - NUL rejected with `422`;
  - `RequestIn` forbids unknown fields;
  - `StoreUnavailable` → `503 + Retry-After`, counted as outcome `unavailable`.
- `src/pipeline/ports.py`
  - `StoreUnavailable`;
  - `ClaimedRequest.round_attempt`;
  - an `OperatorStore` port.
- `src/pipeline/adapters/postgres.py`
  - `PgIntakeStore` translates connection errors into `StoreUnavailable`, so the API does not import asyncpg;
  - the claim returns `round_attempt`;
  - `PgOperatorStore.replay_needs_human(limit, ids)`.
- `migrations/004_attempt_offset.sql`: an `attempt_offset` column.
- `src/pipeline/domain/request.py`: an operator edge `needs_human → retry_scheduled` (replay).
- `src/pipeline/worker/handler.py`
  - `decide` and the crash-loop guard use `round_attempt`;
  - fencing keeps the absolute `attempt`.
- `src/pipeline/config.py`: a validator that requires `lease_s > processing_timeout_s + 2 × db_command_timeout_s`.
- `docker-compose.yml`: demo lease 15 → 21 s.
- `src/pipeline/adapters/downstream.py`: a result over 64 KB → `PermanentError`.
- `src/pipeline/main.py`, `__main__.py`: `python -m pipeline replay [--limit N] [--id UUID …]`.
- Tests:
  - unit: API edge inputs, 503, domain edge, round attempt, settings validation, result cap;
  - integration: API edge inputs on one keep-alive connection, replay SQL;
  - chaos: downstream down → `needs_human` → fix → replay → `done`.
- Living docs:
  - README: failure modes, operator replay, demo timings;
  - `AGENTS.md`: commands, demo timings;
  - spec: lifecycle and FR;
  - `docs/alerts.yml`: the replay command;
  - architecture page: state diagram and as-built.

## Review fixes (verifier)

- README and ADR-0003 now describe the crash-loop guard as round-based. ADR-0003 states the lease budget rule; it was missing from the plan's living-docs list.
- The fencing-after-replay test runs against real Postgres (`test_postgres.py`). Before, it only exercised the fake.
- Replay disarms a pending `needs_human` webhook and resets `webhook_attempt` and `webhook_status`. A webhook with a non-final status can no longer go out, and the next final state gets a full delivery budget.
- The NUL check walks the decoded strings, so the literal text `\\u0000` is accepted.
- `OperatorInterventionError` (it includes `AdminShutdownError`) maps to `503`.

## Design notes

- **Why an offset instead of resetting `attempt` on replay.**
  - `attempt` is the fencing token. If a replay reset it to 0, a zombie worker from before `needs_human`, still holding attempt *k*, could complete the new round once that round reaches *k*.
  - So `attempt` stays monotonic. Replay sets `attempt_offset = attempt`, and the retry budget counts `round_attempt = attempt − attempt_offset`.
- **Only `needs_human` can be replayed.** `failed` means the downstream rejected the data, so replaying it would fail the same way.
- **Webhooks after a replay.** The tenant already got a `needs_human` webhook. When the replayed request finishes, it gets another one with the new final state. Webhook give-ups (`webhook_status = failed`) are not replayed in this task: the results stay available through `GET`.

## Proof

- `make check` passes, and the new unit tests cover:
  - every malformed input → `422`, never `500`;
  - store down → `503`;
  - the replay edge;
  - a replayed request gets a full retry budget;
  - a stale fencing token is still rejected;
  - settings validation;
  - the result cap.
- `make test-integration` passes, and covers:
  - the edge inputs on one connection, followed by a normal POST → `202`;
  - replay SQL touches only `needs_human`;
  - the chaos scenario: down → `needs_human` → fix → replay → `done`.
