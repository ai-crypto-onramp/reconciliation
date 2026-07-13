"""Stage 1 tests: config, DB models, migrations, repository."""

from __future__ import annotations

from decimal import Decimal

import pytest

from reconciliation.config import BREAK_TYPES, SOURCES, Settings, get_settings, reset_settings


def test_settings_defaults():
    settings = Settings()
    assert settings.port == 8080
    assert settings.break_tolerance_seconds == 300
    assert settings.auto_resolve_timing_breaks is True
    assert settings.escalation_age_minutes == 60
    assert settings.consumer_concurrency == 4


def test_settings_from_env_overrides():
    settings = Settings(port=9090, db_url="sqlite+aiosqlite:///./test.db", break_tolerance_seconds=120)
    assert settings.port == 9090
    assert settings.break_tolerance_seconds == 120


def test_get_settings_cached():
    s1 = get_settings()
    s2 = get_settings()
    assert s1 is s2
    reset_settings()
    s3 = get_settings()
    assert s3 is not s1


def test_canonical_constants():
    assert "ledger" in SOURCES
    assert "rails" in SOURCES
    assert "exchanges" in SOURCES
    assert "onchain" in SOURCES
    assert "custody" in SOURCES
    assert "amount_mismatch" in BREAK_TYPES
    assert "timing_gap" in BREAK_TYPES
    assert "missing_entry" in BREAK_TYPES
    assert "duplicate" in BREAK_TYPES


@pytest.mark.asyncio
async def test_sql_repo_upsert_external_event_idempotent(sqlite_repo):
    repo = sqlite_repo
    event, created1 = await repo.upsert_external_event(
        source="rails", external_event_id="e1", payload={"amount": 100}
    )
    _, created2 = await repo.upsert_external_event(
        source="rails", external_event_id="e1", payload={"amount": 100}
    )
    assert created1 is True
    assert created2 is False
    events = await repo.list_external_events(source="rails")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_sql_repo_create_and_complete_run(sqlite_repo):
    repo = sqlite_repo
    run = await repo.create_recon_run(source="rails", scope="daily")
    assert run.status == "running"
    await repo.complete_recon_run(run.id, matched=5, unmatched=1, breaks=1)
    fetched = await repo.get_recon_run(run.id)
    assert fetched.status == "completed"
    assert fetched.matched_count == 5
    assert fetched.breaks_count == 1
    assert fetched.completed_at is not None


@pytest.mark.asyncio
async def test_sql_repo_create_and_list_breaks(sqlite_repo):
    repo = sqlite_repo
    run = await repo.create_recon_run(source="rails", scope="daily")
    await repo.create_break(
        run_id=run.id,
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    await repo.create_break(
        run_id=run.id,
        source="rails",
        asset="EUR",
        reference="ref2",
        type="timing_gap",
        classification="timing",
        internal_amount=Decimal("50"),
        external_amount=None,
        status="open",
    )
    all_breaks = await repo.list_breaks()
    assert len(all_breaks) == 2
    open_breaks = await repo.list_breaks(status="open")
    assert len(open_breaks) == 2
    usd_breaks = await repo.list_breaks(asset="USD")
    assert len(usd_breaks) == 1


@pytest.mark.asyncio
async def test_sql_repo_add_resolution_and_update_status(sqlite_repo):
    repo = sqlite_repo
    run = await repo.create_recon_run(source="rails", scope="daily")
    brk = await repo.create_break(
        run_id=run.id,
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    await repo.update_break_status(brk.id, "resolved")
    resolution = await repo.add_break_resolution(brk.id, type="manual", actor="ops", note="fixed")
    assert resolution is not None
    fetched = await repo.get_break(brk.id)
    assert fetched.status == "resolved"
    assert fetched.resolved_at is not None


@pytest.mark.asyncio
async def test_sql_repo_open_timing_breaks_for(sqlite_repo):
    repo = sqlite_repo
    await repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="timing_gap",
        classification="timing",
        internal_amount=Decimal("100"),
        external_amount=None,
        status="open",
    )
    matches = await repo.open_timing_breaks_for("rails", "USD", "ref1")
    assert len(matches) == 1
    none_matches = await repo.open_timing_breaks_for("rails", "EUR", "ref1")
    assert len(none_matches) == 0


@pytest.mark.asyncio
async def test_sql_repo_upsert_rule(sqlite_repo):
    repo = sqlite_repo
    rule = await repo.upsert_rule(source="rails", asset="USD", match_strategy="fuzzy", tolerance_seconds=120)
    assert rule.match_strategy == "fuzzy"
    # Upsert again to update.
    await repo.upsert_rule(source="rails", asset="USD", match_strategy="exact", tolerance_seconds=60)
    rules = await repo.get_rules("rails")
    assert len(rules) == 1
    assert rules[0].match_strategy == "exact"
    assert rules[0].tolerance_seconds == 60
