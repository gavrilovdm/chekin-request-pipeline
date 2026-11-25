"""Tenants submitting requests at a steady rate. Each simulated user sends ~1 request per second."""

import random
from uuid import uuid4

from locust import FastHttpUser, constant_throughput, task

KEYS = ["demo-key-a", "demo-key-b"]  # tenant-a has a webhook, tenant-b polls


class Tenant(FastHttpUser):
    """One integration of one tenant."""

    wait_time = constant_throughput(1)

    def on_start(self) -> None:
        """Pick a tenant for this simulated client."""
        self.headers = {"Authorization": f"Bearer {random.choice(KEYS)}"}
        self.recent: list[str] = []

    @task(10)
    def submit(self) -> None:
        """POST a new request with a fresh Idempotency-Key."""
        body = {"type": "guest.register", "data": {"name": "Ana", "document": "X1234567"}}
        with self.client.post("/v1/requests", json=body, headers=self.headers | {"Idempotency-Key": str(uuid4())},
                              name="POST /v1/requests", catch_response=True) as response:
            if response.status_code == 202:
                self.recent = (self.recent + [response.json()["id"]])[-20:]
                response.success()
            else:
                response.failure(f"{response.status_code}")

    @task(1)
    def poll(self) -> None:
        """GET the status of a recent request."""
        if self.recent:
            self.client.get(f"/v1/requests/{random.choice(self.recent)}", headers=self.headers,
                            name="GET /v1/requests/{id}")
