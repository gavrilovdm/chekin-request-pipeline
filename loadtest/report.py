"""Write a markdown report for one load run: intake numbers from locust, processing numbers from Postgres.

Usage: python loadtest/report.py --label w1-u300 --since 2026-11-26T10:00:00Z --csv loadtest/results/w1-u300_stats.csv
"""

import argparse
import asyncio
import csv
import os
from datetime import datetime
from pathlib import Path

import asyncpg

DSN = os.environ.get("PIPELINE_DATABASE_URL", "postgresql://pipeline:pipeline@localhost:55432/pipeline")

PROCESSING = """
    SELECT count(*)                                                        AS total,
           count(*) FILTER (WHERE status = 'done')                         AS done,
           count(*) FILTER (WHERE status = 'failed')                       AS failed,
           count(*) FILTER (WHERE status = 'needs_human')                  AS needs_human,
           count(*) FILTER (WHERE status NOT IN ('done', 'failed', 'needs_human')) AS unfinished,
           percentile_cont(0.5)  WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM updated_at - created_at))
               FILTER (WHERE status = 'done')                              AS e2e_p50,
           percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM updated_at - created_at))
               FILTER (WHERE status = 'done')                              AS e2e_p95,
           EXTRACT(EPOCH FROM max(updated_at) - min(created_at))           AS span_s,
           avg(attempt) FILTER (WHERE status = 'done')                     AS avg_attempts
      FROM requests WHERE created_at >= $1
"""


def intake(csv_path: Path) -> dict[str, str]:
    """The aggregated POST row from locust's stats CSV."""
    with csv_path.open() as fh:
        for row in csv.DictReader(fh):
            if row["Name"] == "POST /v1/requests":
                return row
    raise SystemExit(f"no POST row in {csv_path}")


async def wait_drained(conn: asyncpg.Connection, since: datetime, limit_s: float = 300) -> float:
    """Seconds until nothing from this run is unfinished (or the limit)."""
    loop = asyncio.get_running_loop()
    start = loop.time()
    while loop.time() - start < limit_s:
        left = await conn.fetchval(
            "SELECT count(*) FROM requests WHERE created_at >= $1 AND status NOT IN ('done','failed','needs_human')",
            since,
        )
        if left == 0:
            break
        await asyncio.sleep(1)
    return loop.time() - start


async def main() -> None:
    """Collect numbers and write loadtest/results/<label>.md."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--label", required=True)
    parser.add_argument("--since", required=True, type=lambda s: datetime.fromisoformat(s.replace("Z", "+00:00")))
    parser.add_argument("--csv", required=True, type=Path)
    parser.add_argument("--workers", required=True)
    parser.add_argument("--users", required=True)
    parser.add_argument("--duration", required=True)
    args = parser.parse_args()

    conn = await asyncpg.connect(DSN)
    drain_s = await wait_drained(conn, args.since)
    p = await conn.fetchrow(PROCESSING, args.since)
    await conn.close()
    post = intake(args.csv)

    done_rate = p["done"] / p["span_s"] if p["span_s"] else 0
    lines = [
        f"# Load run `{args.label}`",
        "",
        f"Workers: **{args.workers}** · simulated tenants: **{args.users}** (~1 POST/s each)"
        f" · duration: {args.duration} · downstream mock latency 200 ± 50 ms"
        " · laptop: Docker with 4 vCPU shared by every service and locust.",
        "",
        "## Intake (locust)",
        "",
        "| POSTs | failures | rps | p50 ms | p95 ms | p99 ms |",
        "|---|---|---|---|---|---|",
        f"| {post['Request Count']} | {post['Failure Count']} | {float(post['Requests/s']):.0f} | {post['50%']} "
        f"| {post['95%']} | {post['99%']} |",
        "",
        "## Processing (Postgres)",
        "",
        "| accepted | done | failed | needs_human | unfinished | done/s | e2e p50 s | e2e p95 s | avg attempts "
        "| drain after load |",
        "|---|---|---|---|---|---|---|---|---|---|",
        f"| {p['total']} | {p['done']} | {p['failed']} | {p['needs_human']} | {p['unfinished']} | {done_rate:.0f} "
        f"| {p['e2e_p50'] or 0:.2f} | {p['e2e_p95'] or 0:.2f} | {p['avg_attempts'] or 0:.2f} | {drain_s:.0f} s |",
        "",
    ]
    out = Path("loadtest/results") / f"{args.label}.md"
    out.write_text("\n".join(lines))
    print(out.read_text())


if __name__ == "__main__":
    asyncio.run(main())
