# Review rules

Used by the verifier agent (`.claude/agents/verifier.md`) and by human reviewers. Rank findings by severity.

## Important (block the merge)

- **Correctness under at-least-once delivery.**
  - Can a message delivered twice cause two side effects or a wrong state?
  - Can a crash between two steps leave a request stuck in a non-terminal state that nothing repairs?
- **State changes bypass the domain.** Any status written without going through `domain/request.py` rules.
- **Lost work.** Any path where an accepted request (`202`) can end with no final state and no alert.
- **Unbounded things.** Retries without a maximum, queries without `LIMIT`, concurrency without a cap, external calls without a timeout.
- **Plan drift.** The diff does something the task plan does not mention, or skips a proof the plan promised.
- **Tests that prove nothing.** A test that still passes if the code under test returns nothing.
- **Living docs drift.** The diff changes behaviour, a command, a setting, a file layout or a number that one of the
  living docs states, and the doc is not updated in the same PR. Living docs: `README.md`, `AGENTS.md`,
  `docs/sdlc/spec.md`, `docs/sdlc/roadmap.md`, `docs/adr/`, `docs/architecture.html`, `docs/alerts.yml`, docstrings
  and comments next to the changed code. Task plans in `docs/tasks/` are records and are not updated after merge.

## Worth fixing (do not block)

- Layering: business rules importing FastAPI, asyncpg, aio-pika or httpx.
- Missing metric or log line on a failure path.
- Names that describe types instead of the domain.

## Nits (cap at three per review)

- Style that ruff does not catch.
