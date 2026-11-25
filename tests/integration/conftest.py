"""Fixtures for tests against the docker compose stack (`make up`)."""

import os
from uuid import UUID

import pytest

from pipeline.adapters.postgres import create_pool

DATABASE_URL = os.environ.get("PIPELINE_DATABASE_URL", "postgresql://pipeline:pipeline@localhost:55432/pipeline")
AMQP_URL = os.environ.get("PIPELINE_AMQP_URL", "amqp://guest:guest@localhost:5672/")
API_URL = os.environ.get("PIPELINE_API_URL", "http://localhost:8080")
MOCK_URL = os.environ.get("PIPELINE_MOCK_URL", "http://localhost:8081")


def pytest_collection_modifyitems(items):
    for item in items:
        item.add_marker(pytest.mark.integration)


@pytest.fixture
async def pool():
    pool = await create_pool(DATABASE_URL, size=4)
    yield pool
    await pool.close()


async def set_row(pool, request_id: UUID, **fields) -> None:
    """Put a row into a precise state for a test (bypasses the domain on purpose: it is test setup)."""
    cols = ", ".join(f"{k} = ${i + 2}" for i, k in enumerate(fields))
    await pool.execute(f"UPDATE requests SET {cols} WHERE id = $1", request_id, *fields.values())
