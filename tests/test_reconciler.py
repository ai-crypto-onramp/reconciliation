"""Stage 7 tests: reconciler orchestration, recon runs, intraday/EOD."""

from __future__ import annotations

from datetime import UTC
from decimal import Decimal

import pytest

from reconciliation.batch import eod_join
from reconciliation.config import Settings
from reconciliation.kafka import InMemoryConsumer, InMemoryProducer
from reconciliation.matching import ExternalEntry, LedgerEntry
from reconciliation.reconciler import Reconciler


@pytest.fixture
def recon(fake_repo):
    producer = InMemoryProducer()
    settings = Settings(break_tolerance_seconds=300)
    return Reconciler(fake_repo, producer, settings)


@pytest.mark.asyncio
async def test_execute_creates_run_with_counts(recon, fake_repo):
    ledger = [LedgerEntry(reference="ref1", asset="USD", amount=Decimal("100"))]
    external = [ExternalEntry(external_event_id="e1", source="rails", asset="USD", reference="ref1", amount=Decimal("100"))]
    run = await recon.execute(source="rails", scope="daily", ledger_entries=ledger, external_entries=external)
    assert run.status == "completed"
    assert run.matched_count == 1
    assert run.breaks_count == 0


@pytest.mark.asyncio
async def test_execute_surfaces_breaks_on_mismatch(recon, fake_repo):
    ledger = [LedgerEntry(reference="ref1", asset="USD", amount=Decimal("100"))]
    external = [ExternalEntry(external_event_id="e1", source="rails", asset="USD", reference="ref1", amount=Decimal("90"))]
    run = await recon.execute(source="rails", scope="daily", ledger_entries=ledger, external_entries=external)
    assert run.breaks_count >= 1
    breaks = await fake_repo.list_breaks()
    assert len(breaks) >= 1


@pytest.mark.asyncio
async def test_resolve_break_appends_resolution(recon, fake_repo):
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
    resolution = await recon.resolve_break(brk.id, actor="ops", note="fixed")
    assert resolution is not None
    updated = await fake_repo.get_break(brk.id)
    assert updated.status == "resolved"
    assert len(updated.resolutions) == 1
    assert updated.resolutions[0]["actor"] == "ops"


@pytest.mark.asyncio
async def test_resolve_break_returns_none_for_missing(recon):
    result = await recon.resolve_break(999, actor="ops")
    assert result is None


@pytest.mark.asyncio
async def test_consume_once_ingests_from_consumer(recon, fake_repo):
    consumer = InMemoryConsumer(["rail-connectors"])
    consumer.enqueue(
        "rail-connectors",
        {"external_event_id": "e1", "source": "rails", "asset": "USD", "amount": "100", "reference": "ref1"},
    )
    count = await recon.consume_once(consumer)
    assert count == 1
    events = await fake_repo.list_external_events(source="rails")
    assert len(events) == 1


def test_eod_join_matches_aligned_rows():
    ledger_rows = [{"reference": "ref1", "asset": "USD", "amount": 100.0, "counterparty": "bank1"}]
    external_rows = [
        {"external_event_id": "e1", "source": "rails", "reference": "ref1", "asset": "USD", "amount": 100.0, "counterparty": "bank1"}
    ]
    result = eod_join(ledger_rows, external_rows, strategy="exact")
    assert len(result.matched) == 1


def test_eod_join_surfaces_unmatched_ledger():
    ledger_rows = [{"reference": "ref1", "asset": "USD", "amount": 100.0}]
    external_rows = []
    result = eod_join(ledger_rows, external_rows, strategy="exact")
    assert len(result.unmatched_ledger) == 1


@pytest.mark.asyncio
async def test_age_and_escalate_promotes_stale_breaks(recon, fake_repo):
    from datetime import datetime, timedelta

    now = datetime.now(tz=UTC)
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
        detected_at=now - timedelta(hours=3),
        age_seconds=0,
    )
    settings = Settings(escalation_age_minutes=60)
    recon.settings = settings
    emitted = await recon.age_and_escalate()
    assert len(emitted) == 1
    assert emitted[0]["action"] == "escalated"
