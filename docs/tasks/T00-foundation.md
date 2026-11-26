# T0 — Foundation

Roadmap: [T0](../sdlc/roadmap.md) · Spec: [spec.md](../sdlc/spec.md)

## Files that change

- `docs/sdlc/{intent,spec,roadmap}.md`, `docs/architecture.html` (target architecture page)
- `REVIEW.md`, `.claude/agents/verifier.md`
- `pyproject.toml`, `uv.lock`, `Makefile`, `src/pipeline/__init__.py`, `mock_downstream/__init__.py`,
  `tests/__init__.py`, `tests/unit/test_smoke.py`
- Hub kit (`kit.py apply`): `.claude/kit.json` (docs-check hook), `.github/pull_request_template.md`,
  `.github/workflows/ci.yml`. CI is changed to run on `master` and to call `make check`
- Docs gate: ruff `D100`–`D103` (every public module, class and function has a docstring) is part of `make check`
- `AGENTS.md`: verification block and "Things Claude gets wrong"

## Order of work

1. Write the SDLC docs and the target architecture.
2. Set up `pyproject.toml` with uv, ruff and pytest settings.
3. Add a `Makefile` with a single `make check` that exits non-zero on failure.
4. Add the unit step to CI.

## Risks

- CI and local commands drift. Mitigation: CI runs `make check` itself, and ruff comes from `uv.lock` (`uv run`, not `uvx`).
- The docs-check excludes live in `Makefile` and `.claude/kit.json` (hub-managed). Keep the two in step. (Later: `kit.json` switched from `uvx ruff` to `uv run ruff`, so the hook, `make check` and CI all use the ruff from `uv.lock`.)

## Proof

- `make check` passes locally.
- CI is green on the PR.
