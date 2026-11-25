# T0 — Foundation

Roadmap: [T0](../sdlc/roadmap.md) · Spec: [spec.md](../sdlc/spec.md)

## Files that change

- `docs/sdlc/{intent,spec,roadmap}.md`, `docs/architecture.html` (target architecture page)
- `REVIEW.md`, `.claude/agents/verifier.md`
- `pyproject.toml`, `Makefile`, `src/pipeline/__init__.py`, `tests/unit/test_smoke.py`
- `.github/workflows/ci.yml`: run on `master`, add unit tests
- `AGENTS.md`: verification block and "Things Claude gets wrong"

## Order of work

1. Write the SDLC docs and the target architecture.
2. Set up `pyproject.toml` with uv, ruff and pytest settings.
3. Add a `Makefile` with a single `make check` that exits non-zero on failure.
4. Add the unit step to CI.

## Risks

- CI and local commands drift. Mitigation: CI calls the same `uv run` commands as `make check`.

## Proof

- `make check` passes locally.
- CI is green on the PR.
