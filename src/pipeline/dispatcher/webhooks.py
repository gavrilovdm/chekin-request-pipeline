"""Webhook delivery from the outbox: signed, bounded per tenant, retried with backoff, at least once."""

import asyncio
import hashlib
import hmac
import json
from datetime import timedelta

import httpx
import structlog

from pipeline.domain.retry_policy import RetryPolicy
from pipeline.ports import Clock, WebhookJob, WebhookStore
from pipeline.worker.handler import utcnow

log = structlog.get_logger(__name__)


def sign(secret: str, body: bytes) -> str:
    """``X-Signature`` value: HMAC-SHA256 of the exact body bytes with the tenant's secret."""
    return "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


class WebhookSender:
    """Sends due webhooks. One slow tenant endpoint can hold at most ``per_tenant`` deliveries at a time."""

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
        self._limits: dict[str, asyncio.Semaphore] = {}

    async def send_due_once(self) -> int:
        """Claim and deliver one batch; returns how many jobs were attempted."""
        jobs = await self._store.claim_due(self._batch, self._visibility)
        await asyncio.gather(*(self._deliver(job) for job in jobs))
        return len(jobs)

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
            except httpx.HTTPError as exc:
                delivered, detail = False, type(exc).__name__

        if delivered:
            await self._store.mark_delivered(job.request_id)
            return
        attempt = job.webhook_attempt + 1
        if attempt >= self._policy.max_attempts:
            log.error("webhook.gave_up", request_id=str(job.request_id), tenant=job.tenant_id, detail=detail)
            await self._store.mark_failed_attempt(job.request_id, None)
        else:
            log.warning("webhook.retry", request_id=str(job.request_id), tenant=job.tenant_id, detail=detail)
            await self._store.mark_failed_attempt(job.request_id, self._clock() + self._policy.delay(attempt))
