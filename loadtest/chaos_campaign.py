"""Chaos under load: steady traffic while random faults hit every component, then check the invariants.

Usage: uv run python loadtest/chaos_campaign.py --run 1 [--rps 40] [--minutes 4] [--fault-every 20] [--seed N]

1. Scale to 2 workers, reset the mock.
2. Well-behaved clients POST at --rps for --minutes. They retry 5xx and connection errors with the same
   Idempotency-Key, as the API contract asks.
3. Every --fault-every seconds one random fault from FAULTS is injected, then healed.
4. Heal everything, wait for the backlog to drain, replay needs_human (the documented operator action),
   wait again, check invariants I1-I6 (tests/invariants.py), write loadtest/results/chaos-<run>.md.
Exit code 1 if any invariant is violated.
"""

import argparse
import asyncio
import random
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tests.invariants import check_all  # noqa: E402

API, MOCK, AMQP_API = "http://localhost:8080", "http://localhost:8081", "http://localhost:15672"
DSN = "postgresql://pipeline:pipeline@localhost:55432/pipeline"
KEYS = ["demo-key-a", "demo-key-b"]
HEALTHY = {"latency_ms": 50, "jitter_ms": 20}


def compose(*args: str) -> None:
    """Run a docker compose command; raise if it fails."""
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True, timeout=240)


async def mock_config(**config) -> None:
    """Set the downstream mock to the healthy config plus ``config`` overrides."""
    async with httpx.AsyncClient(base_url=MOCK, timeout=5) as mock:
        await mock.put("/admin/config", json=HEALTHY | config)


async def kill_restart(service: str) -> None:
    """SIGKILL a service, then bring it back."""
    await asyncio.to_thread(compose, "kill", service)
    await asyncio.sleep(2)
    # the harness plays the orchestrator: compose does not restart a container that was killed by hand
    scale = ["--scale", "worker=2"] if service == "worker" else []
    await asyncio.to_thread(compose, "up", "-d", "--no-deps", *scale, service)


async def stop_start(service: str, down_s: float) -> None:
    """Stop a service for ``down_s`` seconds, then start it again."""
    await asyncio.to_thread(compose, "stop", service)
    await asyncio.sleep(down_s)
    await asyncio.to_thread(compose, "start", service)


async def freeze(container: str, seconds: float) -> None:
    """docker pause: the process is frozen, not dead. Longer than the lease, it becomes a zombie on resume."""
    await asyncio.to_thread(subprocess.run, ["docker", "pause", container], check=True, capture_output=True)
    await asyncio.sleep(seconds)
    await asyncio.to_thread(subprocess.run, ["docker", "unpause", container], check=True, capture_output=True)


async def downstream(seconds: float, **config) -> None:
    """Put the downstream mock into a faulty config for ``seconds``, then heal it."""
    await mock_config(**config)
    await asyncio.sleep(seconds)
    await mock_config()


FAULTS = {
    "kill worker": lambda: kill_restart("worker"),
    "freeze worker past the lease": lambda: freeze("chekin-pipeline-worker-1", 25),
    "kill dispatcher": lambda: kill_restart("dispatcher"),
    "freeze dispatcher": lambda: freeze("chekin-pipeline-dispatcher-1", 12),
    "kill api": lambda: kill_restart("api"),
    "restart rabbitmq": lambda: stop_start("rabbitmq", 5),
    "restart postgres": lambda: stop_start("postgres", 5),
    "downstream flaky 40%": lambda: downstream(15, error_rate=0.4),
    "downstream down": lambda: downstream(8, down=True),
    "downstream slow 3 s": lambda: downstream(15, latency_ms=3000, jitter_ms=500),
    "tenant webhook endpoint down": lambda: downstream(20, webhooks_down=True),
}


class Load:
    """Well-behaved tenant clients submitting at a steady rate."""

    def __init__(self, rps: float):
        self.rps = rps
        self.accepted: set[str] = set()
        self.gave_up = 0
        self.client_retries = 0

    async def submit_one(self, client: httpx.AsyncClient) -> None:
        """Submit one request, retrying 5xx/429/connection errors with the same Idempotency-Key."""
        key = random.choice(KEYS)
        headers = {"Authorization": f"Bearer {key}", "Idempotency-Key": str(uuid4())}
        body = {"type": "guest.register", "data": {"name": "Ana"}}
        for attempt in range(8):  # a well-behaved client: same key, backoff, honour Retry-After
            try:
                response = await client.post("/v1/requests", json=body, headers=headers)
                if response.status_code == 202:
                    self.accepted.add(response.json()["id"])
                    return
                if response.status_code < 500 and response.status_code != 429:
                    return
                wait = float(response.headers.get("Retry-After", 2 ** attempt))
            except httpx.HTTPError:
                wait = 2 ** attempt
            self.client_retries += 1
            await asyncio.sleep(min(wait, 10) * random.uniform(0.5, 1.0))
        self.gave_up += 1

    async def run(self, seconds: float) -> None:
        """Submit at ``rps`` for ``seconds`` and wait for every client to finish."""
        async with httpx.AsyncClient(base_url=API, timeout=10) as client:
            tasks = []
            end = time.monotonic() + seconds
            while time.monotonic() < end:
                tasks.append(asyncio.create_task(self.submit_one(client)))
                await asyncio.sleep(1 / self.rps)
            await asyncio.gather(*tasks)


async def wait_drained(since: datetime, limit_s: float) -> float:
    """Seconds until no request of this run is unfinished or has a pending webhook (or the limit)."""
    import asyncpg

    start = time.monotonic()
    while time.monotonic() - start < limit_s:
        try:
            conn = await asyncpg.connect(DSN)
            row = await conn.fetchrow(
                """SELECT count(*) FILTER (WHERE status NOT IN ('done','failed','needs_human')) AS unfinished,
                          count(*) FILTER (WHERE webhook_pending) AS webhooks,
                          string_agg(DISTINCT status, ',') FILTER (WHERE status NOT IN ('done','failed','needs_human'))
                              AS statuses,
                          max(webhook_next_at) - now() AS last_webhook_due
                     FROM requests WHERE created_at >= $1""", since)
            await conn.close()
            if row["unfinished"] == 0 and row["webhooks"] == 0:
                break
            if int(time.monotonic() - start) % 10 < 2:
                print(f"drain +{time.monotonic() - start:.0f}s: unfinished={row['unfinished']} ({row['statuses']})"
                      f" webhooks={row['webhooks']} last_webhook_due_in={row['last_webhook_due']}", flush=True)
        except OSError:
            pass
        await asyncio.sleep(2)
    return time.monotonic() - start


async def main() -> int:
    """Run one campaign and write its report; exit code 1 on any invariant violation."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--run", required=True)
    parser.add_argument("--rps", type=float, default=40)
    parser.add_argument("--minutes", type=float, default=4)
    parser.add_argument("--fault-every", type=float, default=20)
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    seed = args.seed if args.seed is not None else random.randrange(10**6)
    random.seed(seed)

    await asyncio.to_thread(compose, "up", "-d", "--scale", "worker=2", "--wait")
    async with httpx.AsyncClient(base_url=MOCK) as mock:
        await mock.post("/admin/reset")
    await mock_config()
    since = datetime.now(UTC)
    load = Load(args.rps)
    injected: list[str] = []

    async def chaos(seconds: float) -> None:
        end = time.monotonic() + seconds
        while time.monotonic() + args.fault_every < end:
            await asyncio.sleep(args.fault_every / 2)
            name = random.choice(list(FAULTS))
            injected.append(f"{time.monotonic() - started:5.0f}s  {name}")
            print("fault:", name, flush=True)
            try:
                await FAULTS[name]()
            except Exception as exc:  # a fault helper failing must not stop the campaign
                injected[-1] += f"  (helper error: {exc!r})"
            await asyncio.sleep(args.fault_every / 2)

    started = time.monotonic()
    await asyncio.gather(load.run(args.minutes * 60), chaos(args.minutes * 60))

    # heal: everything up, downstream healthy, unpause anything left paused
    unpause = "docker ps -q --filter status=paused | xargs -r docker unpause"
    await asyncio.to_thread(subprocess.run, unpause, shell=True, capture_output=True)
    await asyncio.to_thread(compose, "up", "-d", "--scale", "worker=2", "--wait")
    await mock_config()
    drain_s = await wait_drained(since, 240)
    replay = ["docker", "compose", "exec", "-T", "dispatcher", "python", "-m", "pipeline", "replay"]
    done = await asyncio.to_thread(subprocess.run, replay, capture_output=True, text=True)
    replayed = done.stdout.strip().splitlines()[:1]
    drain2_s = await wait_drained(since, 240)
    result = await check_all(DSN, API, MOCK, AMQP_API, since)

    lines = [
        f"# Chaos campaign run {args.run}",
        "",
        f"Seed {seed} · {args.rps:.0f} rps for {args.minutes:g} min · a fault every {args.fault_every:g} s"
        " · 2 workers · laptop (Docker, 4 vCPU).",
        "",
        f"**Invariant violations: {len(result['violations'])}**",
        "",
        "| Accepted | Client gave up | Client retries | Final statuses | Webhooks | Drain | Replay | Drain after |",
        "|---|---|---|---|---|---|---|---|",
        f"| {len(load.accepted)} | {load.gave_up} | {load.client_retries} | {result['counts']} | {result['webhooks']}"
        f" | {drain_s:.0f} s | {replayed[0] if replayed else '-'} | {drain2_s:.0f} s |",
        "",
        "## Faults injected",
        "",
        "```",
        *injected,
        "```",
        "",
        "## Violations",
        "",
        *([f"- {v}" for v in result["violations"][:50]] or ["None."]),
        "",
    ]
    out = Path("loadtest/results") / f"chaos-{args.run}.md"
    out.write_text("\n".join(lines))
    print(out.read_text())
    return 1 if result["violations"] else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
