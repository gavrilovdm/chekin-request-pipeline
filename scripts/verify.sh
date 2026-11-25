#!/usr/bin/env bash
# Run the fresh-context verifier agent (.claude/agents/verifier.md) on one task branch.
# It works in a throwaway git worktree, so the main checkout can move on to the next task.
# Usage: scripts/verify.sh docs/tasks/T01-domain.md feat/t1-domain
set -euo pipefail
plan="$1"
branch="$2"
root="$(git rev-parse --show-toplevel)"
out="$root/.reviews/$(basename "$plan" .md).md"
wt="$(mktemp -d)/verify"
mkdir -p "$root/.reviews"
git worktree add -q --detach "$wt" "$branch"
trap 'git -C "$root" worktree remove --force "$wt"' EXIT
(
  cd "$wt"
  uv sync -q
  claude -p --agent verifier \
    --allowedTools Read Grep Glob "Bash(make check)" "Bash(make test-integration)" "Bash(git diff:*)" \
      "Bash(git log:*)" "Bash(uv run pytest:*)" \
    -- "Verify the task whose plan is $plan. The base branch is master. The docker compose stack is already running (do not run make up or docker); run make test-integration directly when the plan asks for it." > "$out"
)
echo "$out"
