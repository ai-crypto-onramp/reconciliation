"""Stage 7 tests: reconciler orchestration, recon runs, intraday/EOD."""

from __future__ import annotations

import uuid
from datetime import UTC
from decimal import Decimal

import pytest

from reconciliation.batch import eod_join
from reconciliation.config import Settings
from reconciliation.kafka import InMemoryConsumer, InMemoryProducer
from reconciliation.matching import ExternalEntry, LedgerEntry
from reconciliation.reconciler import Reconciler


def _new_uuid() -> uuid.UUID:
    gen = getattr(uuid, "uuid7", None)
    return gen() if gen is not None else uuid.uuid4()


@pytest.fixture
def recon(fake_repo):
    producer = InMemoryProducer()
    settings = Settings(break_tolerance_seconds=300)
    return Reconciler(fake_repo, producer, settings)


@pytest.mark.asyncio
async def test_execute_creates_run_with_counts(recon, fake_repo):
    ledger = [LedgerEntry(reference="ref1", asset="USD", amount=Decimal("100"))]
    external = [
        ExternalEntry(
            external_event_id="e1",
            source="RAILS",
            asset="USD",
            reference="ref1",
            amount=Decimal("100"),
        )
    ]
    run = await recon.execute(
        source="RAILS", scope="daily", ledger_entries=ledger, external_entries=external
    )
    assert run.status == "COMPLETED"
    assert run.matched_count == 1
    assert run.breaks_count == 0


@pytest.mark.asyncio
async def test_execute_surfaces_breaks_on_mismatch(recon, fake_repo):
    ledger = [LedgerEntry(reference="ref1", asset="USD", amount=Decimal("100"))]
    external = [
        ExternalEntry(
            external_event_id="e1",
            source="RAILS",
            asset="USD",
            reference="ref1",
            amount=Decimal("90"),
        )
    ]
    run = await recon.execute(
        source="RAILS", scope="daily", ledger_entries=ledger, external_entries=external
    )
    assert run.breaks_count >= 1
    breaks = await fake_repo.list_breaks()
    assert len(breaks) >= 1


@pytest.mark.asyncio
async def test_resolve_break_appends_resolution(recon, fake_repo):
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
    resolution = await recon.resolve_break(brk.id, actor="ops", note="fixed")
    assert resolution is not None
    updated = await fake_repo.get_break(brk.id)
    assert updated.status == "RESOLVED"
    assert len(updated.resolutions) == 1
    assert updated.resolutions[0]["actor"] == "ops"


@pytest.mark.asyncio
async def test_resolve_break_returns_none_for_missing(recon):
    result = await recon.resolve_break(_new_uuid(), actor="ops")
    assert result is None


@pytest.mark.asyncio
async def test_consume_once_ingests_from_consumer(recon, fake_repo):
    consumer = InMemoryConsumer(["rail-connectors"])
    consumer.enqueue(
        "rail-connectors",
        {
            "external_event_id": "e1",
            "source": "RAILS",
            "asset": "USD",
            "amount": "100",
            "reference": "ref1",
        },
    )
    count = await recon.consume_once(consumer)
    assert count == 1
    events = await fake_repo.list_external_events(source="RAILS")
    assert len(events) == 1


def test_eod_join_matches_aligned_rows():
    ledger_rows = [{"reference": "ref1", "asset": "USD", "amount": 100.0, "counterparty": "bank1"}]
    external_rows = [
        {
            "external_event_id": "e1",
            "source": "RAILS",
            "reference": "ref1",
            "asset": "USD",
            "amount": 100.0,
            "counterparty": "bank1",
        }
    ]
    result = eod_join(ledger_rows, external_rows, strategy="EXACT")
    assert len(result.matched) == 1


def test_eod_join_surfaces_unmatched_ledger():
    ledger_rows = [{"reference": "ref1", "asset": "USD", "amount": 100.0}]
    external_rows = []
    result = eod_join(ledger_rows, external_rows, strategy="EXACT")
    assert len(result.unmatched_ledger) == 1


@pytest.mark.asyncio
async def test_age_and_escalate_promotes_stale_breaks(recon, fake_repo):
    from datetime import datetime, timedelta

    now = datetime.now(tz=UTC)
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
        detected_at=now - timedelta(hours=3),
        age_seconds=0,
    )
    settings = Settings(escalation_age_minutes=60)
    recon.settings = settings
    emitted = await recon.age_and_escalate()
    assert len(emitted) == 1
    assert emitted[0]["action"] == "escalated"
