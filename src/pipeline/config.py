"""Runtime settings, read from environment variables (prefix ``PIPELINE_``)."""

from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All tunables in one place. Defaults suit `docker compose up` on a laptop."""

    model_config = SettingsConfigDict(env_prefix="PIPELINE_")

    database_url: str = "postgresql://pipeline:pipeline@localhost:55432/pipeline"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    db_pool_size: int = 10
    db_command_timeout_s: float = 5.0

    # worker
    worker_concurrency: int = 50  # in-flight requests per worker process == RabbitMQ prefetch
    processing_timeout_s: float = 10.0
    lease_s: float = 30.0
    max_attempts: int = 5
    retry_base_s: float = 1.0
    retry_max_s: float = 60.0
    downstream_url: str = "http://localhost:8081"
    shutdown_grace_s: float = 25.0

    # api
    publish_timeout_s: float = 1.0
    max_queue_depth: int = 200_000  # above this, POST answers 503 + Retry-After (load shedding)
    tenant_cache_ttl_s: float = 60.0

    # dispatcher
    sweep_interval_s: float = 1.0
    # A request not claimed this long after its last publish may have lost its message; it is re-published, but
    # only while the queue is nearly empty (depth < sweep_batch): under a backlog the message is waiting (T9).
    stale_after_s: float = 60.0
    sweep_batch: int = 500
    webhook_timeout_s: float = 5.0
    webhook_max_attempts: int = 8
    webhook_per_tenant_concurrency: int = 4
    webhook_max_in_flight: int = 200  # deliveries in flight across tenants; also the HTTP connection pool size

    metrics_port: int = 9100
    log_level: str = "INFO"

    @model_validator(mode="after")
    def _lease_covers_the_worst_case_attempt(self) -> "Settings":
        # claim and complete are each bounded by the DB command timeout, processing by its own timeout. A shorter
        # lease lets a healthy slow attempt be re-claimed and processed twice (correct, but wasted work).
        worst = self.processing_timeout_s + 2 * self.db_command_timeout_s
        if self.lease_s <= worst:
            raise ValueError(f"lease_s ({self.lease_s}) must exceed processing + 2 x DB timeout ({worst})")
        return self
