import httpx
import pytest
from httpx import ASGITransport

from reconciliation.app import READINESS_CHECKS, app, classify_readiness, readiness_report


@pytest.mark.asyncio
async def test_healthz_ok():
    async with httpx.AsyncClient(transport=ASGITransport(app=app)) as client:
        resp = await client.get("http://testserver/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_readyz_ok():
    async with httpx.AsyncClient(transport=ASGITransport(app=app)) as client:
        resp = await client.get("http://testserver/readyz")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ready"
    assert body["healthy"] == str(len(READINESS_CHECKS))
    assert body["failed"] == "0"
    assert body["total"] == str(len(READINESS_CHECKS))
    for name, _ in READINESS_CHECKS:
        assert body[name] == "ok"


def test_readiness_report_all_ok():
    results, failed, total = readiness_report()
    assert failed == 0
    assert total == len(READINESS_CHECKS)
    assert results["ledger"] == "ok"


def test_readiness_report_with_failure(monkeypatch):
    monkeypatch.setattr(
        "reconciliation.app.READINESS_CHECKS",
        [("ledger", lambda: True), ("db", lambda: False)],
    )
    results, failed, total = readiness_report()
    assert failed == 1
    assert total == 2
    assert results["ledger"] == "ok"
    assert results["db"] == "down"


def test_classify_readiness():
    assert classify_readiness(0, 18) == (200, "ready")
    assert classify_readiness(3, 18) == (200, "degraded")
    assert classify_readiness(0, 0) == (200, "ready")
