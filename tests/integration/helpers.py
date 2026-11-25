"""Shared helpers for tests against the running stack."""

import asyncio
from uuid import uuid4

A = {"Authorization": "Bearer demo-key-a"}


async def submit(api, body=None) -> str:
    response = await api.post("/v1/requests", json=body or {"type": "guest.register", "data": {"name": "Ana"}},
                              headers=A | {"Idempotency-Key": f"e2e-{uuid4()}"})
    assert response.status_code == 202
    return response.json()["id"]


async def wait_for(api, rid, statuses=("done", "failed", "needs_human"), limit_s=20.0) -> dict:
    deadline = asyncio.get_running_loop().time() + limit_s
    while True:
        view = (await api.get(f"/v1/requests/{rid}", headers=A)).json()
        if view["status"] in statuses:
            return view
        assert asyncio.get_running_loop().time() < deadline, f"stuck in {view['status']}: {view}"
        await asyncio.sleep(0.2)
