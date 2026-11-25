"""Logging setup and every Prometheus series the pipeline exposes, defined once.

Labels are bounded (statuses, outcomes); tenant and request ids never become labels, they go to logs.
"""

import logging
import sys

import structlog
from prometheus_client import Counter, Gauge, Histogram


def configure_logging(level: str = "INFO") -> None:
    """JSON logs to stdout: one event per line, ready for any log pipeline."""
    logging.basicConfig(format="%(message)s", stream=sys.stdout, level=level)
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per HTTP call is noise at this volume
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level)),
        logger_factory=structlog.PrintLoggerFactory(),
    )


# --- api ---------------------------------------------------------------------
INTAKE = Counter(
    "pipeline_intake_total",
    "POST /v1/requests by outcome",
    ["outcome"],  # accepted | replayed | unauthorized | rate_limited | shed | invalid | conflict | too_large
)
INTAKE_SECONDS = Histogram(
    "pipeline_intake_seconds", "Latency of accepted POST /v1/requests",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
)
PUBLISH_FAILURES = Counter("pipeline_publish_failures_total", "Publishes that failed; the sweeper recovers them")

# --- worker ------------------------------------------------------------------
HANDLED = Counter("pipeline_handled_total", "Deliveries handled by result", ["result"])
ATTEMPT_SECONDS = Histogram(
    "pipeline_attempt_seconds", "Time to handle one delivery (claim, process, record)",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)
IN_FLIGHT = Gauge("pipeline_in_flight", "Deliveries being handled by this worker")
POISON = Counter("pipeline_poison_messages_total", "Unparseable messages sent to the dead-letter queue")

# --- dispatcher --------------------------------------------------------------
REPUBLISHED = Counter("pipeline_republished_total", "Ids re-published by the sweeper, by request status", ["status"])
REPUBLISH_FAILURES = Counter("pipeline_republish_failures_total", "Sweeper publishes that failed")
WEBHOOKS = Counter("pipeline_webhooks_total", "Webhook attempts by outcome: delivered, retry, failed", ["outcome"])
NEEDS_HUMAN = Gauge("pipeline_needs_human", "Requests waiting for an operator")
OLDEST_UNFINISHED = Gauge("pipeline_oldest_unfinished_seconds", "Age of the oldest request not yet final")
WEBHOOKS_PENDING = Gauge("pipeline_webhooks_pending", "Final states not yet reported to tenants")
QUEUE_DEPTH = Gauge("pipeline_queue_depth", "Messages ready in the work queue")
