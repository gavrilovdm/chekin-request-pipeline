# T3 — Persistence and messaging

Roadmap: [T3](../sdlc/roadmap.md) · Builds on: [T2 ports](T02-worker-core.md)

## Files that change

- `migrations/001_schema.sql`: tables `tenants` and `requests`, plus partial indexes for the sweeper and webhook scans.
- `migrations/002_demo_tenants.sql`: three demo tenants. Their keys are test values.
- `src/pipeline/config.py`: `Settings` (pydantic-settings, env-driven).
- `src/pipeline/adapters/postgres.py`
  - `create_pool` (jsonb codec)
  - `PgRequestRepository`, which implements `RequestRepository`: claim and fenced complete
  - `PgIntakeStore`: idempotent create, tenant-scoped get, tenant lookup by key hash
- `src/pipeline/ports.py`: add `IntakeStore`, `TenantRecord`, `RequestView`, `CreateResult`.
- `src/pipeline/adapters/rabbitmq.py`
  - `declare_topology`: quorum queue `requests` with DLX → `requests.dead`
  - `RabbitPublisher`: persistent messages, publisher confirms, timeout, queue depth
- `docker-compose.yml`: `postgres` and `rabbitmq` with healthchecks. Migrations run through `docker-entrypoint-initdb.d`.
- `tests/integration/conftest.py`, `tests/integration/test_postgres.py`, `tests/integration/test_rabbitmq.py`

## Order of work

1. Write the schema and the compose infra.
2. Write the repository integration tests. They are the same scenarios as the in-memory fake in T2.
3. Implement the repositories.
4. Write the RabbitMQ topology and publisher, with their tests.

## Design notes

- **Claim** is one statement:

  ```sql
  UPDATE … SET status = 'processing', attempt = attempt + 1, locked_until = now() + lease
  WHERE id = $1 AND (status IN claimable OR (status = 'processing' AND locked_until < now()))
  RETURNING …
  ```

  - The lease uses the **database clock**, so workers with skewed clocks agree.
  - The claimable set comes from `domain.request.CLAIMABLE`. It is not duplicated as literals.
- **Complete** filters on `status = 'processing' AND attempt = $n`: this is the fencing token.
  - It joins `tenants` and sets `webhook_pending` only for final states of tenants that have a webhook URL.
  - Before the write, `ensure_transition(PROCESSING, target)` runs, so the domain rule guards the SQL too.
- **Idempotent create** is `INSERT … ON CONFLICT (tenant_id, idempotency_key) DO NOTHING RETURNING id`.
  - On conflict, the existing `id` and `payload_hash` are returned. The API turns a hash mismatch into `422`.
- **There is no `queued` status** ([spec](../sdlc/spec.md)). `enqueued_at` is touched only by the sweeper (T6). The
  API does not write it, which saves one write per request.
- **RabbitMQ.**
  - The quorum queue is replicated and durable.
  - The message body is only the request id.
  - `mandatory=True` + publisher confirms: a publish that is not routed or not confirmed raises an error, so the API
    knows to leave the request to the sweeper.

## Found while building

- aio-pika's robust channel caches a queue's declaration result, so `declare_queue(passive=True)` kept returning a
  depth of 0. `queue_depth` now uses a raw passive `queue_declare` on the underlying channel. This is pinned by
  `test_queue_depth_is_reported`.
- A `retry_scheduled` row with a NULL `next_attempt_at` is treated as due (`COALESCE`), the same as the fake does. The
  domain always sets the column, so NULL appears only in hand-made rows.

## Risks

- The fake from T2 and the SQL could disagree. The same scenarios run against both here.
- Port clashes on the laptop. Postgres is exposed on 55432 and the API (T4) on 8080.

## Proof

- `make check` passes.
- `make up && make test-integration` passes. The tests cover:
  - claim once, then `None`;
  - claim of a final request → `None`;
  - an expired lease is re-claimed;
  - a stale attempt cannot complete;
  - `webhook_pending` only for tenants with a URL;
  - idempotent create, including the hash on conflict;
  - publish → consume;
  - a rejected message lands in `requests.dead`;
  - queue depth is reported.
