#!/usr/bin/env bash
# One load scenario: scale workers, reset the mock, run locust, wait for the backlog to drain, write the report.
# Usage: loadtest/run.sh WORKERS USERS DURATION     e.g. loadtest/run.sh 4 300 60s
set -euo pipefail
workers="${1:-1}"
users="${2:-300}"
duration="${3:-60s}"
label="w${workers}-u${users}"

docker compose up -d --scale "worker=${workers}" --wait
curl -sf -X POST localhost:8081/admin/reset >/dev/null
curl -sf -X PUT localhost:8081/admin/config -H 'content-type: application/json' \
  -d '{"latency_ms": 200, "jitter_ms": 100}' >/dev/null

since="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
uv run --group load locust -f loadtest/locustfile.py --headless -u "${users}" -r 100 -t "${duration}" \
  --host http://localhost:8080 --csv "loadtest/results/${label}" --only-summary --loglevel WARNING
uv run python loadtest/report.py --label "${label}" --since "${since}" --csv "loadtest/results/${label}_stats.csv" \
  --workers "${workers}" --users "${users}" --duration "${duration}"
