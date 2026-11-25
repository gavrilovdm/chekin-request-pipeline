"""Fixtures for tests against the docker compose stack (`make up`)."""

import os
from uuid import UUID

import httpx
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


@pytest.fixture
async def api():
    """Client for the intake API."""
    async with httpx.AsyncClient(base_url=API_URL, timeout=10) as client:
        yield client


@pytest.fixture
async def mock():
    """Client for the downstream mock, reset to fast and healthy before and after the test."""
    async with httpx.AsyncClient(base_url=MOCK_URL, timeout=10) as client:
        await client.post("/admin/reset")
        await client.put("/admin/config", json={"latency_ms": 20, "jitter_ms": 10})
        yield client
        await client.post("/admin/reset")
