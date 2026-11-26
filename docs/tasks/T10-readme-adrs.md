# T10 — README, ADRs, as-built vs target

Roadmap: [T10](../sdlc/roadmap.md)

## Files that change

- `README.md`: the entry point for the hiring team. It covers:
  - the problem, the guarantees, and the evidence for each;
  - how to try it in 2 minutes;
  - the architecture;
  - what happens after a worker takes a request;
  - failure modes with the test that proves each;
  - options considered;
  - capacity;
  - observability;
  - next steps for production;
  - how the repo was built.
- `docs/adr/0001…0007`: one decision each, in the format context / options / decision / consequences.
- `docs/architecture.html`: roadmap statuses, plus the filled "Target vs as-built" section (each component and guarantee checked against the code, with its gaps fixed or explained).
- `docs/sdlc/roadmap.md`: task status.
- `.github/workflows/ci.yml`: an `integration` job.
  1. `docker compose up --wait`
  2. `make test-integration` (with the chaos scenarios)
  3. On failure, dump the logs

  So the failure scenarios run on every PR, not only on a laptop.

## Order of work

1. Write the ADRs, from the decisions already made in the task plans.
2. Write the README.
3. Compare the target architecture page with the code, then fill in "as-built".
4. Add the CI integration job and confirm it is green on the PR.

## Proof

- `make check` passes.
- The CI `integration` job is green on the PR.
- Every link in the README resolves to a file in the repo.
- Every number in the README matches `loadtest/results/README.md`, and every test name in the README exists.
