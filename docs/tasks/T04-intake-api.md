# T4 — Intake API

Roadmap: [T4](../sdlc/roadmap.md) · Spec: [functional requirements](../sdlc/spec.md#functional-requirements)

## Files that change

- `src/pipeline/api/rate_limit.py`: `TokenBucket` (pure, clock injected) and `TenantRateLimiter`.
- `src/pipeline/api/auth.py`: `TenantCache`. It hashes the bearer key and caches the lookup with a TTL, including misses.
- `src/pipeline/api/shedding.py`: `LoadShedder`. It refreshes the queue depth in the background and fails open when the depth is unknown.
- `src/pipeline/api/app.py`: `create_app(deps=None, lifespan=None)`. Handlers read `ApiDeps` from `app.state.deps`, so tests inject fakes directly and production sets them in the lifespan once connections are open.
  - `POST /v1/requests`
  - `GET /v1/requests/{id}`
  - `/healthz`
- `src/pipeline/main.py`: composition root (`api_app` factory). It builds the pool, the Rabbit channel, the publisher and the shedder in the lifespan.
- `Dockerfile` (one image for all services) and an `api` service in `docker-compose.yml` on port 8080.
- `src/pipeline/adapters/rabbitmq.py`: timeouts on the queue-depth read (review fix).
- `tests/unit/test_auth_shedding.py` (bounded cache, fail-open shedding), `tests/unit/test_rate_limit.py`, `tests/unit/test_api.py` (fakes), `tests/integration/test_api.py` (real stack).

## Order of work

1. Write the token bucket tests, then implement the bucket.
2. Write the API tests on fakes for each response code, then implement the app.
3. Wire the composition root, the Dockerfile and compose. Then write an integration test: POST → row in Postgres, then a replay.

## Design notes

**POST order.** Cheapest rejection first; nothing is written before the request is known to be acceptable.
1. Auth → `401`
2. Per-tenant token bucket → `429` + `Retry-After`
3. Load shedding (queue depth above `max_queue_depth`) → `503` + `Retry-After`
4. Validation → `422`
5. Idempotent insert
6. Publish

**Idempotency.** The payload hash is the sha256 of the canonical JSON (sorted keys).
- Same key and same body → the same `id`, `202`, `Idempotent-Replayed: true`. The request is not published again.
- Same key, different body → `422`.

**Publish failure.** If the publish fails or times out (`publish_timeout_s`, 1 s), the response is still `202`.
- The row is already committed as `received`. The sweeper (T6) re-publishes it after `stale_after_s`.
- The interview diagram retried the publish 1-2-4-8 s inside the request. That holds the client's connection for up to 15 s and still needs a fallback, so the retry moves to the sweeper.

**Limits.**
- Body ≤ 64 KB → otherwise `413`.
- `Idempotency-Key` is required, 1–200 characters.
- The rate limit is per api replica (in memory). This is approximate under N replicas. See the spec's areas of concern.

**Layering.** Handlers stay thin. Rules live in `rate_limit.py`, `auth.py` and `shedding.py`. Storage is reached only through the `IntakeStore` and `Publisher` ports.

## Review fixes

- **Body size.** The body is capped while it streams in. A declared `Content-Length` above the cap is refused before anything is read.
- **Tenant cache.** It is a bounded LRU with a TTL (10k entries). Distinct guessed keys still cost one indexed lookup each, and per-IP throttling of failed auth belongs to the API gateway or WAF.
- **Depth read.** It is bounded by a timeout, so a stalled broker fails open instead of freezing a stale "overloaded" depth.
- **Validation errors.** Only pydantic `ValidationError` maps to `422`. Any other `ValueError` is a server error.

## Risks

- **Fail-open shedding.** When RabbitMQ is down the depth is unknown, so the API keeps accepting. This is intended: the DB is the source of truth and the sweeper catches up later.

## Proof

- `make check` passes, and the API unit tests cover:
  - `202` for a new request;
  - a replay returns the same id;
  - a different body with the same key → `422`;
  - a missing key → `400`;
  - a bad token → `401`;
  - a tenant over its limit → `429` and other tenants are unaffected;
  - a deep queue → `503`;
  - a publish failure still returns `202`;
  - `GET` is tenant-scoped (`404` for another tenant);
  - an oversized body → `413`.
- `make up && make test-integration` passes, including the API round trip against the real stack.
