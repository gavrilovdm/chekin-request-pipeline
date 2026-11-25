"""Runtime settings, read from environment variables (prefix ``PIPELINE_``)."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All tunables in one place. Defaults suit `docker compose up` on a laptop."""

    model_config = SettingsConfigDict(env_prefix="PIPELINE_")

    database_url: str = "postgresql://pipeline:pipeline@localhost:55432/pipeline"
    amqp_url: str = "amqp://guest:guest@localhost:5672/"
    db_pool_size: int = 10

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
    stale_after_s: float = 60.0  # re-publish a 'received' or re-published request not claimed after this long
    sweep_batch: int = 500
    webhook_timeout_s: float = 5.0
    webhook_max_attempts: int = 8
    webhook_per_tenant_concurrency: int = 4

    metrics_port: int = 9100
