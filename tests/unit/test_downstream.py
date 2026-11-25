from uuid import uuid4

import httpx
import pytest

from pipeline.adapters.downstream import HttpProcessor
from pipeline.domain.errors import PermanentError, RetryableError
from pipeline.ports import ClaimedRequest

REQUEST = ClaimedRequest(uuid4(), "tenant-a", {"type": "t", "data": {}}, 1)


def processor(handler):
    return HttpProcessor(httpx.AsyncClient(transport=httpx.MockTransport(handler), base_url="http://downstream"))


async def test_success_returns_json_and_sends_idempotency_key():
    seen = {}

    def handler(request):
        seen["key"] = request.headers["Idempotency-Key"]
        return httpx.Response(200, json={"ref": "R-1"})

    assert await processor(handler).process(REQUEST) == {"ref": "R-1"}
    assert seen["key"] == str(REQUEST.id)


@pytest.mark.parametrize("code", [429, 500, 502, 503])
async def test_server_errors_and_throttling_are_retryable(code):
    with pytest.raises(RetryableError, match=str(code)):
        await processor(lambda r: httpx.Response(code, text="busy")).process(REQUEST)


@pytest.mark.parametrize("code", [400, 404, 422])
async def test_client_errors_are_permanent(code):
    with pytest.raises(PermanentError, match=str(code)):
        await processor(lambda r: httpx.Response(code, text="bad data")).process(REQUEST)


@pytest.mark.parametrize("exc", [httpx.ReadTimeout("slow"), httpx.ConnectError("refused")])
async def test_transport_failures_are_retryable(exc):
    def handler(request):
        raise exc

    with pytest.raises(RetryableError):
        await processor(handler).process(REQUEST)


async def test_empty_success_body_is_an_empty_result():
    assert await processor(lambda r: httpx.Response(204)).process(REQUEST) == {}


@pytest.mark.parametrize("response", [httpx.Response(302, text="moved"), httpx.Response(200, text="<html>")])
async def test_redirects_and_non_json_bodies_are_permanent(response):
    with pytest.raises(PermanentError):
        await processor(lambda r: response).process(REQUEST)
