-- Request pipeline schema. Postgres is the source of truth; RabbitMQ only carries request ids.

CREATE TABLE tenants (
    id                 text PRIMARY KEY,
    api_key_hash       text NOT NULL UNIQUE,          -- sha256 of the bearer key; the key itself is never stored
    webhook_url        text,                          -- NULL: the tenant polls GET /v1/requests/{id}
    webhook_secret     text,
    rate_per_sec       double precision NOT NULL DEFAULT 200,
    burst              integer NOT NULL DEFAULT 400
);

CREATE TABLE requests (
    id                 uuid PRIMARY KEY,
    tenant_id          text NOT NULL REFERENCES tenants (id),
    idempotency_key    text NOT NULL,
    payload_hash       text NOT NULL,                 -- detects a reused Idempotency-Key with a different body
    payload            jsonb NOT NULL,
    status             text NOT NULL CHECK (status IN
                         ('received', 'processing', 'retry_scheduled', 'done', 'failed', 'needs_human')),
    attempt            integer NOT NULL DEFAULT 0,    -- 1-based once claimed; the fencing token
    locked_until       timestamptz,                   -- lease of the worker holding it (status = processing)
    next_attempt_at    timestamptz,                   -- when a retry is due (status = retry_scheduled)
    enqueued_at        timestamptz,                   -- last re-publish by the sweeper
    result             jsonb,
    last_error         text,
    webhook_pending    boolean NOT NULL DEFAULT false,-- the outbox flag: a final state not yet reported
    webhook_attempt    integer NOT NULL DEFAULT 0,
    webhook_next_at    timestamptz,
    webhook_status     text CHECK (webhook_status IN ('delivered', 'failed')),
    created_at         timestamptz NOT NULL DEFAULT now(),
    updated_at         timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT requests_idempotency UNIQUE (tenant_id, idempotency_key)
);

-- Partial indexes keep the sweeper and webhook scans cheap: they only cover the few rows not yet final.
CREATE INDEX requests_sweep_received ON requests (created_at) WHERE status = 'received';
CREATE INDEX requests_sweep_retry    ON requests (next_attempt_at) WHERE status = 'retry_scheduled';
CREATE INDEX requests_sweep_lease    ON requests (locked_until) WHERE status = 'processing';
CREATE INDEX requests_webhook_due    ON requests (webhook_next_at) WHERE webhook_pending;
CREATE INDEX requests_needs_human    ON requests (updated_at) WHERE status = 'needs_human';
