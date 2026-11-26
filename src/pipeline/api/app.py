"""Public intake API. Thin handlers: rules live in auth, rate_limit and shedding; storage behind the ports."""

import hashlib
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Annotated, Any, Protocol
from uuid import UUID

import structlog
from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from pipeline.api.auth import TenantCache
from pipeline.api.rate_limit import TenantRateLimiter
from pipeline.observability import INTAKE, INTAKE_SECONDS, PUBLISH_FAILURES
from pipeline.ports import IntakeStore, Publisher, StoreUnavailable, TenantRecord

log = structlog.get_logger(__name__)

MAX_BODY_BYTES = 64 * 1024

# POST /v1/requests status code -> intake outcome label (bounded set; 202 splits into accepted / replayed).
_OUTCOMES = {400: "invalid", 401: "unauthorized", 413: "too_large", 422: "invalid", 429: "rate_limited", 503: "shed"}


class Shedder(Protocol):
    """Decides whether to refuse new work."""

    def retry_after(self) -> int | None:
        """Seconds to wait if overloaded, else ``None``."""
        ...


@dataclass
class ApiDeps:
    """Everything the API needs, passed in by the composition root (or by tests)."""

    store: IntakeStore
    publisher: Publisher
    tenants: TenantCache
    limiter: TenantRateLimiter
    shedder: Shedder
    readiness: Callable[[], Awaitable[dict[str, bool]]] | None = None  # per-dependency checks for /readyz


class RequestIn(BaseModel):
    """An abstract job from a tenant: a type and an opaque data object. Unknown fields are rejected."""

    model_config = ConfigDict(extra="forbid")

    type: str = Field(min_length=1, max_length=64)
    data: dict[str, Any]


async def _read_capped(request: Request) -> bytes:
    """Read the body, refusing early (by Content-Length) or mid-stream once it passes ``MAX_BODY_BYTES``."""
    too_large = HTTPException(413, f"Body larger than {MAX_BODY_BYTES} bytes")
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise too_large
    chunks, size = [], 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise too_large
        chunks.append(chunk)
    return b"".join(chunks)


def _has_nul(value: Any) -> bool:
    """True if any decoded string (key or value) contains NUL, which Postgres jsonb cannot store."""
    if isinstance(value, str):
        return "\x00" in value
    if isinstance(value, dict):
        return any(_has_nul(k) or _has_nul(v) for k, v in value.items())
    if isinstance(value, list):
        return any(_has_nul(v) for v in value)
    return False


def payload_hash(payload: dict[str, Any]) -> str:
    """sha256 of canonical JSON, so key order does not make two equal bodies differ."""
    return hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def create_app(deps: ApiDeps | None = None, lifespan: Any = None) -> FastAPI:
    """Build the FastAPI app.

    Tests pass ``deps`` directly. In production the composition root passes a ``lifespan`` that opens connections
    and sets ``app.state.deps``; handlers always read the dependencies from there.
    """
    app = FastAPI(title="Request pipeline API", version="0.1.0", lifespan=lifespan)
    if deps is not None:
        app.state.deps = deps

    @app.middleware("http")
    async def count_intake(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        if request.method != "POST" or request.url.path != "/v1/requests":
            return await call_next(request)
        started = time.perf_counter()
        try:
            response = await call_next(request)
        except Exception:
            INTAKE.labels("error").inc()  # an unhandled failure (e.g. Postgres down) must still be visible
            raise
        if response.headers.get("X-Intake-Outcome") in ("conflict", "unavailable"):
            INTAKE.labels(response.headers["X-Intake-Outcome"]).inc()
        elif response.status_code == 202:
            replayed = response.headers.get("Idempotent-Replayed") == "true"
            INTAKE.labels("replayed" if replayed else "accepted").inc()
            INTAKE_SECONDS.observe(time.perf_counter() - started)
        else:
            INTAKE.labels(_OUTCOMES.get(response.status_code, "error")).inc()
        return response

    def get_deps(request: Request) -> ApiDeps:
        return request.app.state.deps

    Deps = Annotated[ApiDeps, Depends(get_deps)]

    async def tenant(deps: Deps, authorization: Annotated[str | None, Header()] = None) -> TenantRecord:
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(401, "Missing bearer API key")
        found = await deps.tenants.resolve(authorization.removeprefix("Bearer ").strip())
        if found is None:
            raise HTTPException(401, "Unknown API key")
        return found

    def limited(deps: Deps, caller: Annotated[TenantRecord, Depends(tenant)]) -> TenantRecord:
        wait = deps.limiter.check(caller)
        if wait is not None:
            raise HTTPException(429, "Rate limit exceeded for this tenant", {"Retry-After": str(max(1, round(wait)))})
        return caller

    @app.post("/v1/requests", status_code=202)
    async def submit(
        deps: Deps,
        request: Request,
        response: Response,
        caller: Annotated[TenantRecord, Depends(limited)],
        idempotency_key: Annotated[str | None, Header(max_length=200)] = None,
    ) -> dict[str, Any]:
        """Accept a request for asynchronous processing. Safe to retry with the same Idempotency-Key."""
        if not idempotency_key:
            raise HTTPException(400, "Idempotency-Key header is required")
        retry_after = deps.shedder.retry_after()
        if retry_after is not None:
            raise HTTPException(503, "Overloaded, retry later", {"Retry-After": str(retry_after)})
        raw = await _read_capped(request)
        try:
            body = RequestIn.model_validate_json(raw).model_dump()
        except ValidationError as exc:
            # No 'input' in the detail: it can hold raw bytes, which are not JSON-serialisable (T11).
            raise HTTPException(422, exc.errors(include_url=False, include_context=False, include_input=False)) from exc
        if _has_nul(body):
            raise HTTPException(422, "NUL characters (\\u0000) are not allowed in request data")

        digest = payload_hash(body)
        created = await deps.store.create(caller.id, idempotency_key, body, digest)
        if not created.created:
            if created.payload_hash != digest:
                raise HTTPException(422, "Idempotency-Key was already used with a different body",
                                    {"X-Intake-Outcome": "conflict"})
            response.headers["Idempotent-Replayed"] = "true"
        else:
            await _publish(deps, created.id)
        return {"id": str(created.id), "status": "received", "status_url": f"/v1/requests/{created.id}"}

    async def _publish(deps: ApiDeps, request_id: UUID) -> None:
        try:
            await deps.publisher.publish(request_id)
        except Exception as exc:  # the row is committed; the sweeper re-publishes it after stale_after_s
            PUBLISH_FAILURES.inc()
            log.warning("intake.publish_failed", request_id=str(request_id), error=repr(exc))

    @app.get("/v1/requests/{request_id}")
    async def status(
        deps: Deps, request_id: UUID, caller: Annotated[TenantRecord, Depends(limited)]
    ) -> dict[str, Any]:
        """Current state of one of the caller's requests."""
        view = await deps.store.get(caller.id, request_id)
        if view is None:
            raise HTTPException(404, "Request not found")
        return {
            "id": str(view.id),
            "status": view.status,
            "attempt": view.attempt,
            "result": view.result,
            "last_error": view.last_error,
            "created_at": view.created_at.isoformat(),
            "updated_at": view.updated_at.isoformat(),
        }

    @app.exception_handler(StoreUnavailable)
    async def store_unavailable(_request: Request, _exc: StoreUnavailable) -> JSONResponse:
        """The database is unreachable: tell the client to retry instead of reporting a bug."""
        return JSONResponse({"detail": "Temporarily unavailable, retry later"}, status_code=503,
                            headers={"Retry-After": "5", "X-Intake-Outcome": "unavailable"})

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        """Liveness: the process answers. A failure here means restart the process."""
        return {"status": "ok"}

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        """Prometheus scrape endpoint."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @app.get("/readyz")
    async def readyz(deps: Deps) -> JSONResponse:
        """Readiness: dependencies reachable. A failure here means take the replica out of the load balancer."""
        checks = await deps.readiness() if deps.readiness else {}
        return JSONResponse(checks, status_code=200 if all(checks.values()) else 503)

    return app
