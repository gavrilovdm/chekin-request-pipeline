"""HTTP ``Processor``: sends the request to the downstream service and classifies the response."""

from typing import Any

import httpx

from pipeline.domain.errors import PermanentError, RetryableError
from pipeline.ports import ClaimedRequest


class HttpProcessor:
    """Calls ``POST /process`` on the downstream with ``Idempotency-Key`` = request id.

    The key makes a repeat safe: if a worker dies after the downstream succeeded but before the outcome was
    recorded, the next attempt gets the original result instead of a second side effect.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def process(self, request: ClaimedRequest) -> dict[str, Any]:
        """Process one request; raise ``RetryableError`` or ``PermanentError`` on failure."""
        try:
            response = await self._client.post(
                "/process",
                json={"request_id": str(request.id), "tenant_id": request.tenant_id, "payload": request.payload},
                headers={"Idempotency-Key": str(request.id)},
            )
        except httpx.TimeoutException as exc:
            raise RetryableError(f"downstream timeout ({type(exc).__name__})") from exc
        except httpx.TransportError as exc:
            raise RetryableError(f"downstream unreachable ({type(exc).__name__})") from exc

        if response.status_code == 429 or response.status_code >= 500:
            raise RetryableError(f"downstream {response.status_code}: {response.text[:200]}")
        if response.status_code >= 300:
            raise PermanentError(f"downstream {response.status_code}: {response.text[:200]}")
        if not response.content:
            return {}
        try:
            return response.json()
        except ValueError as exc:
            raise PermanentError(f"downstream {response.status_code} with a non-JSON body") from exc
