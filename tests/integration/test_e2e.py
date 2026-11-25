"""End to end through the running stack: POST -> queue -> worker -> downstream -> done."""

import asyncio
import hashlib
import hmac
from uuid import uuid4

from tests.integration.helpers import submit, wait_for


async def test_request_is_processed_to_done(api, mock):
    rid = await submit(api)

    view = await wait_for(api, rid)

    assert view["status"] == "done"
    assert view["attempt"] == 1
    assert view["result"]["ref"] == f"R-{rid[:8]}"


async def test_lost_publish_is_recovered_by_the_sweeper(api, mock, pool):
    """A row committed without any message (as if the publish failed) still reaches done."""
    rid = uuid4()
    await pool.execute(
        """INSERT INTO requests (id, tenant_id, idempotency_key, payload_hash, payload, status, created_at)
           VALUES ($1, 'tenant-a', $2, 'h', $3, 'received', now() - interval '2 minutes')""",
        rid, f"lost-{rid}", {"type": "guest.register", "data": {}},
    )

    view = await wait_for(api, str(rid))

    assert view["status"] == "done"


async def test_final_state_is_pushed_by_a_signed_webhook(api, mock):
    rid = await submit(api)
    await wait_for(api, rid)

    for _ in range(50):
        hooks = (await mock.get("/admin/webhooks", params={"request_id": rid})).json()
        if hooks:
            break
        await asyncio.sleep(0.2)
    assert len(hooks) == 1
    hook = hooks[0]
    expected = "sha256=" + hmac.new(b"demo-secret-a", hook["body"].encode(), hashlib.sha256).hexdigest()
    assert hook["signature"] == expected
    assert hook["json"]["status"] == "done" and hook["json"]["id"] == rid
