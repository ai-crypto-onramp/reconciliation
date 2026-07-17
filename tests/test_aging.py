"""Stage 6 tests: aging and escalation."""

from __future__ import annotations

import uuid


def _new_uuid() -> uuid.UUID:
    gen = getattr(uuid, "uuid7", None)
    return gen() if gen is not None else uuid.uuid4()
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from reconciliation.breaks.aging import (
    escalate_stale_breaks,
    manually_escalate_break,
    update_ages,
)
from reconciliation.config import Settings
from reconciliation.kafka import InMemoryProducer


@pytest.fixture
def producer():
    p = InMemoryProducer()
    return p


@pytest.fixture
def settings():
    return Settings(escalation_age_minutes=60, escalation_webhook="http://hook")


@pytest.mark.asyncio
async def test_update_ages_recomputes_seconds(fake_repo):
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
        detected_at=now - timedelta(seconds=120),
        age_seconds=0,
    )
    updated = await update_ages(fake_repo, now=now)
    assert updated == 1
    breaks = await fake_repo.list_breaks()
    assert breaks[0].age_seconds == 120


@pytest.mark.asyncio
async def test_escalate_stale_breaks_emits_alert_and_audit(fake_repo, producer, settings):
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
        detected_at=now - timedelta(hours=2),
        age_seconds=7200,
    )
    emitted = await escalate_stale_breaks(fake_repo, producer, settings=settings, now=now)
    assert len(emitted) == 1
    assert emitted[0]["action"] == "escalated"
    assert len(producer.emitted("break-alert")) == 1
    assert len(producer.emitted("break-event")) == 1
    breaks = await fake_repo.list_breaks()
    assert breaks[0].status == "ESCALATED"


@pytest.mark.asyncio
async def test_escalate_skips_breaks_under_threshold(fake_repo, producer, settings):
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
        detected_at=now - timedelta(seconds=30),
        age_seconds=0,
    )
    emitted = await escalate_stale_breaks(fake_repo, producer, settings=settings, now=now)
    assert emitted == []


@pytest.mark.asyncio
async def test_manually_escalate_break_emits_events(fake_repo, producer):
    now = datetime.now(tz=UTC)
    brk = await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
        detected_at=now,
    )
    result = await manually_escalate_break(
        fake_repo, producer, break_id=brk.id, actor="ops", now=now
    )
    assert result is not None
    assert result["action"] == "escalated"
    assert len(producer.emitted("break-alert")) == 1
    assert len(producer.emitted("break-event")) == 1


@pytest.mark.asyncio
async def test_manually_escalate_returns_none_for_missing_break(fake_repo, producer):
    result = await manually_escalate_break(fake_repo, producer, break_id=_new_uuid(), actor="ops")
    assert result is None
