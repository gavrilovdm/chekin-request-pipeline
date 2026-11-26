"""Webhook delivery from the outbox: signed, bounded per tenant, retried with backoff, at least once."""

import asyncio
import hashlib
import hmac
import json
from datetime import timedelta

import httpx
import structlog

from pipeline.domain.retry_policy import RetryPolicy
from pipeline.observability import WEBHOOKS
from pipeline.ports import Clock, WebhookJob, WebhookStore
from pipeline.worker.handler import utcnow

log = structlog.get_logger(__name__)


def sign(secret: str, body: bytes) -> str:
    """``X-Signature`` value: HMAC-SHA256 of the exact body bytes with the tenant's secret."""
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookSender:
    """Sends due webhooks continuously.

    Deliveries run as background tasks. A tenant holds at most ``per_tenant`` deliveries in flight (a semaphore),
    and new jobs are claimed only for tenants below that, so a slow or hanging endpoint occupies its own slots and
    nobody else's. The loop claims again as soon as any delivery finishes, so a healthy tenant's throughput is
    bounded by its endpoint's latency, not by a polling interval (T12: a per-batch design capped it at 8/s).
    A claimed job waits at most about one HTTP timeout for a slot; the visibility window must exceed that.
    """

    def __init__(
        self,
        store: WebhookStore,
        client: httpx.AsyncClient,
        policy: RetryPolicy,
        *,
        per_tenant: int,
        batch: int,
        visibility: timedelta,
        timeout: float,
        clock: Clock = utcnow,
    ) -> None:
        self._store = store
        self._client = client
        self._policy = policy
        self._per_tenant = per_tenant
        self._batch = batch
        self._visibility = visibility
        self._timeout = timeout
        self._clock = clock
        if visibility.total_seconds() <= timeout:
            raise ValueError("visibility must exceed the webhook timeout, or in-flight jobs get re-claimed")
        self._limits: dict[str, asyncio.Semaphore] = {}
        self._claimed: dict[str, int] = {}  # per tenant: claimed and not finished (in flight or waiting for a slot)
        self._tasks: set[asyncio.Task[None]] = set()
        self._freed = asyncio.Event()

    async def send_due_once(self) -> int:
        """Claim due jobs for tenants with free slots and start delivering them; returns how many were claimed."""
        room = self._batch - len(self._tasks)  # global back-pressure: never more than ``batch`` in flight
        if room <= 0:
            return 0
        saturated = [tenant for tenant, n in self._claimed.items() if n >= self._per_tenant]
        jobs = await self._store.claim_due(room, self._visibility, self._per_tenant, saturated)
        for job in jobs:
            self._claimed[job.tenant_id] = self._claimed.get(job.tenant_id, 0) + 1
            task = asyncio.create_task(self._deliver_and_release(job))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
        return len(jobs)

    async def wait_idle(self) -> None:
        """Wait until every started delivery has finished (tests, shutdown)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def run(self, stop: asyncio.Event, idle_interval: float) -> None:
        """Claim and deliver until ``stop``; sleeps only when nothing was claimable, wakes when a slot frees."""
        while not stop.is_set():
            self._freed.clear()
            try:
                claimed = await self.send_due_once()
            except Exception:
                log.exception("webhook.claim_failed")
                claimed = 0
            if claimed:
                continue
            waiters = [asyncio.ensure_future(self._freed.wait()), asyncio.ensure_future(stop.wait())]
            await asyncio.wait(waiters, timeout=idle_interval, return_when=asyncio.FIRST_COMPLETED)
            for waiter in waiters:
                waiter.cancel()
        await asyncio.wait_for(self.wait_idle(), timeout=self._timeout * 2)

    async def _deliver_and_release(self, job: WebhookJob) -> None:
        try:
            await self._deliver(job)
        except Exception:
            log.exception("webhook.record_failed", request_id=str(job.request_id))  # visibility expiry retries it
        finally:
            self._claimed[job.tenant_id] -= 1
            self._freed.set()

    async def _deliver(self, job: WebhookJob) -> None:
        body = json.dumps(
            {"id": str(job.request_id), "status": job.status, "result": job.result, "error": job.error,
             "attempt": job.attempt},
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Signature": sign(job.secret, body),
            "X-Request-Id": str(job.request_id),
            "X-Tenant": job.tenant_id,
            "X-Webhook-Attempt": str(job.webhook_attempt + 1),
        }
        limit = self._limits.setdefault(job.tenant_id, asyncio.Semaphore(self._per_tenant))
        async with limit:
            try:
                response = await self._client.post(job.url, content=body, headers=headers, timeout=self._timeout)
                delivered = 200 <= response.status_code < 300
                detail = str(response.status_code)
            except Exception as exc:  # any failure (incl. httpx.InvalidURL) is a failed attempt, counted and bounded
                delivered, detail = False, type(exc).__name__

        if delivered:
            WEBHOOKS.labels("delivered").inc()
            await self._store.mark_delivered(job.request_id, job.webhook_attempt)
            return
        attempt = job.webhook_attempt + 1
        if attempt >= self._policy.max_attempts:
            WEBHOOKS.labels("failed").inc()
            log.error("webhook.gave_up", request_id=str(job.request_id), tenant=job.tenant_id, detail=detail)
            await self._store.mark_failed_attempt(job.request_id, job.webhook_attempt, None)
        else:
            WEBHOOKS.labels("retry").inc()
            log.warning("webhook.retry", request_id=str(job.request_id), tenant=job.tenant_id, detail=detail)
            next_at = self._clock() + self._policy.delay(attempt)
            await self._store.mark_failed_attempt(job.request_id, job.webhook_attempt, next_at)
