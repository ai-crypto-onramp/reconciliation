"""Stage 5 tests: auto-resolution of timing breaks."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from reconciliation.breaks.auto_resolve import attempt_auto_resolve


@pytest.mark.asyncio
async def test_timing_break_auto_resolves_when_confirmation_arrives(fake_repo):
    now = datetime.now(tz=UTC)
    # Create an open timing break for a missing external confirmation.
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("100"),
        external_amount=None,
        status="OPEN",
        detected_at=now - timedelta(minutes=5),
        age_seconds=300,
    )
    resolutions = await attempt_auto_resolve(
        fake_repo,
        source="RAILS",
        asset="USD",
        reference="ref1",
        external_amount=Decimal("100"),
        external_timestamp=now,
        tolerance_seconds=600,
        auto_resolve_enabled=True,
    )
    assert len(resolutions) == 1
    assert resolutions[0]["action"] == "auto-resolved"
    breaks = await fake_repo.list_breaks()
    assert breaks[0].status == "RESOLVED"


@pytest.mark.asyncio
async def test_auto_resolve_disabled_leaves_break_open(fake_repo):
    now = datetime.now(tz=UTC)
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("100"),
        external_amount=None,
        status="OPEN",
        detected_at=now,
    )
    resolutions = await attempt_auto_resolve(
        fake_repo,
        source="RAILS",
        asset="USD",
        reference="ref1",
        external_amount=Decimal("100"),
        external_timestamp=now,
        auto_resolve_enabled=False,
    )
    assert resolutions == []
    breaks = await fake_repo.list_breaks()
    assert breaks[0].status == "OPEN"


@pytest.mark.asyncio
async def test_amount_mismatch_does_not_auto_resolve(fake_repo):
    now = datetime.now(tz=UTC)
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("100"),
        external_amount=None,
        status="OPEN",
        detected_at=now,
    )
    resolutions = await attempt_auto_resolve(
        fake_repo,
        source="RAILS",
        asset="USD",
        reference="ref1",
        external_amount=Decimal("99"),  # mismatched
        external_timestamp=now,
        auto_resolve_enabled=True,
    )
    assert resolutions == []
    breaks = await fake_repo.list_breaks()
    assert breaks[0].status == "OPEN"


@pytest.mark.asyncio
async def test_auto_resolution_appends_resolution_record(fake_repo):
    now = datetime.now(tz=UTC)
    brk = await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("100"),
        external_amount=None,
        status="OPEN",
        detected_at=now,
    )
    await attempt_auto_resolve(
        fake_repo,
        source="RAILS",
        asset="USD",
        reference="ref1",
        external_amount=Decimal("100"),
        external_timestamp=now,
        auto_resolve_enabled=True,
    )
    updated = await fake_repo.get_break(brk.id)
    assert len(updated.resolutions) == 1
    assert updated.resolutions[0]["type"] == "AUTO"
    assert updated.resolutions[0]["actor"] == "system"
