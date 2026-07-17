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
        json={"source": "RAILS", "scope": "daily", "mode": "eod"},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "COMPLETED"
    run_id = data["id"]

    status_resp = await client.get(f"http://testserver/v1/recon-runs/{run_id}")
    assert status_resp.status_code == 200
    assert status_resp.json()["id"] == run_id


@pytest.mark.asyncio
async def test_get_missing_break_returns_404(client):
    resp = await client.get("http://testserver/v1/breaks/00000000-0000-7000-8000-000000000099")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_resolve_break_endpoint(client, fake_repo):
    brk = await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    resp = await client.post(
        f"http://testserver/v1/breaks/{brk.id}/resolve",
        json={"actor": "ops", "note": "fixed"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "RESOLVED"


@pytest.mark.asyncio
async def test_escalate_break_endpoint(client, fake_repo):
    brk = await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    resp = await client.post(
        f"http://testserver/v1/breaks/{brk.id}/escalate",
        json={"actor": "ops"},
    )
    assert resp.status_code == 200
    assert resp.json()["status"] == "ESCALATED"


@pytest.mark.asyncio
async def test_breaks_export_csv(client, fake_repo):
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    resp = await client.get("http://testserver/v1/breaks-export?fmt=csv")
    assert resp.status_code == 200
    assert "break_id" in resp.text or "id" in resp.text


@pytest.mark.asyncio
async def test_run_report_csv(client, fake_repo):
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
    await fake_repo.complete_recon_run(run.id, matched=1, unmatched=0, breaks=0)
    resp = await client.get(f"http://testserver/v1/recon-runs/{run.id}/report?fmt=csv")
    assert resp.status_code == 200


@pytest.mark.asyncio
async def test_archive_run_report_endpoint(client, fake_repo):
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
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


@pytest.mark.asyncio
async def test_list_recon_runs(client, fake_repo):
    await fake_repo.create_recon_run(source="RAILS", scope="daily")
    await fake_repo.create_recon_run(source="LEDGER", scope="daily")
    resp = await client.get("http://testserver/v1/recon-runs")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 2
    assert len(body["recon_runs"]) == 2


@pytest.mark.asyncio
async def test_list_recon_runs_filter_by_source(client, fake_repo):
    await fake_repo.create_recon_run(source="RAILS", scope="daily")
    await fake_repo.create_recon_run(source="LEDGER", scope="daily")
    resp = await client.get("http://testserver/v1/recon-runs?source=RAILS")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["recon_runs"][0]["source"] == "RAILS"


@pytest.mark.asyncio
async def test_list_recon_rules_empty(client):
    resp = await client.get("http://testserver/v1/recon-rules")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 0
    assert body["recon_rules"] == []


@pytest.mark.asyncio
async def test_create_and_list_recon_rule(client):
    payload = {
        "source": "RAILS",
        "asset": "USD",
        "match_strategy": "FUZZY",
        "tolerance_seconds": 120,
        "escalation_age_minutes": 30,
        "auto_resolve_timing": True,
        "config": {"threshold": 0.01},
    }
    resp = await client.post("http://testserver/v1/recon-rules", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    assert body["source"] == "RAILS"
    assert body["match_strategy"] == "FUZZY"
    assert body["tolerance_seconds"] == 120

    resp = await client.get("http://testserver/v1/recon-rules")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total"] == 1
    assert body["recon_rules"][0]["source"] == "RAILS"
