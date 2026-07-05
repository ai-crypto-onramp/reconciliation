import httpx
import pytest
from httpx import ASGITransport

from reconciliation.app import app


@pytest.mark.asyncio
async def test_healthz_ok():
    async with httpx.AsyncClient(transport=ASGITransport(app=app)) as client:
        resp = await client.get("/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}