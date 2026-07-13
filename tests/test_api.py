"""Stage 8/10 tests: REST API endpoints."""

from __future__ import annotations

from decimal import Decimal

import httpx
import pytest
from httpx import ASGITransport

from reconciliation.app import create_app
from reconciliation.config import Settings
from reconciliation.kafka import InMemoryProducer
from reconciliation.reconciler import Reconciler


@pytest.fixture
def app_with_fake(fake_repo):
    producer = InMemoryProducer()
    settings = Settings(break_tolerance_seconds=300, escalation_age_minutes=60)
    recon = Reconciler(fake_repo, producer, settings)
    return create_app(reconciler=recon)


@pytest.fixture
async def client(app_with_fake):
    async with httpx.AsyncClient(transport=ASGITransport(app=app_with_fake)) as c:
        yield c


@pytest.mark.asyncio
async def test_get_breaks_empty(client):
    resp = await client.get("http://testserver/v1/breaks")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 0
    assert body["breaks"] == []


@pytest.mark.asyncio
async def test_create_recon_run_and_fetch(client):
    resp = await client.post(
        "http://testserver/v1/recon-runs",
        json={"source": "rails", "scope": "daily", "mode": "eod"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "completed"
    run_id = data["id"]

    status_resp = await client.get(f"http://testserver/v1/recon-runs/{run_id}")
    assert status_resp.status_code == 200
    assert status_resp.json()["id"] == run_id


@pytest.mark.asyncio
async def test_get_missing_break_returns_404(client):
    resp = await client.get("http://testserver/v1/breaks/999")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_resolve_break_endpoint(client, fake_repo):
    brk = await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    resp = await client.post(
        f"http://testserver/v1/breaks/{brk.id}/resolve",
        json={"actor": "ops", "note": "fixed"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "resolved"


@pytest.mark.asyncio
async def test_escalate_break_endpoint(client, fake_repo):
    brk = await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    resp = await client.post(
        f"http://testserver/v1/breaks/{brk.id}/escalate",
        json={"actor": "ops"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "escalated"


@pytest.mark.asyncio
async def test_breaks_export_csv(client, fake_repo):
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    resp = await client.get("http://testserver/v1/breaks-export?fmt=csv")
    assert resp.status_code == 200
    assert "break_id" in resp.text or "id" in resp.text


@pytest.mark.asyncio
async def test_run_report_csv(client, fake_repo):
    run = await fake_repo.create_recon_run(source="rails", scope="daily")
    await fake_repo.complete_recon_run(run.id, matched=1, unmatched=0, breaks=0)
    resp = await client.get(f"http://testserver/v1/recon-runs/{run.id}/report?fmt=csv")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_archive_run_report_endpoint(client, fake_repo):
    run = await fake_repo.create_recon_run(source="rails", scope="daily")
    await fake_repo.complete_recon_run(run.id, matched=1, unmatched=0, breaks=0)
    resp = await client.post(f"http://testserver/v1/recon-runs/{run.id}/report/archive")
    assert resp.status_code == 200
    body = resp.json()
    assert body["key"].endswith(f"{run.id}.csv")
    assert "signed_url" in body


@pytest.mark.asyncio
async def test_healthz_ok(client):
    resp = await client.get("http://testserver/healthz")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}
