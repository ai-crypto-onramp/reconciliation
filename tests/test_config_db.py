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
    settings = Settings(
        port=9090, db_url="sqlite+aiosqlite:///./test.db", break_tolerance_seconds=120
    )
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
    assert "LEDGER" in SOURCES
    assert "RAILS" in SOURCES
    assert "EXCHANGES" in SOURCES
    assert "ONCHAIN" in SOURCES
    assert "CUSTODY" in SOURCES
    assert "AMOUNT_MISMATCH" in BREAK_TYPES
    assert "TIMING_GAP" in BREAK_TYPES
    assert "MISSING_ENTRY" in BREAK_TYPES
    assert "DUPLICATE" in BREAK_TYPES


@pytest.mark.asyncio
async def test_sql_repo_upsert_external_event_idempotent(sqlite_repo):
    repo = sqlite_repo
    event, created1 = await repo.upsert_external_event(
        source="RAILS", external_event_id="e1", payload={"amount": 100}
    )
    _, created2 = await repo.upsert_external_event(
        source="RAILS", external_event_id="e1", payload={"amount": 100}
    )
    assert created1 is True
    assert created2 is False
    events = await repo.list_external_events(source="RAILS")
    assert len(events) == 1


@pytest.mark.asyncio
async def test_sql_repo_create_and_complete_run(sqlite_repo):
    repo = sqlite_repo
    run = await repo.create_recon_run(source="RAILS", scope="daily")
    assert run.status == "RUNNING"
    await repo.complete_recon_run(run.id, matched=5, unmatched=1, breaks=1)
    fetched = await repo.get_recon_run(run.id)
    assert fetched.status == "COMPLETED"
    assert fetched.matched_count == 5
    assert fetched.breaks_count == 1
    assert fetched.completed_at is not None


@pytest.mark.asyncio
async def test_sql_repo_create_and_list_breaks(sqlite_repo):
    repo = sqlite_repo
    run = await repo.create_recon_run(source="RAILS", scope="daily")
    await repo.create_break(
        run_id=run.id,
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    await repo.create_break(
        run_id=run.id,
        source="RAILS",
        asset="EUR",
        reference="ref2",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("50"),
        external_amount=None,
        status="OPEN",
    )
    all_breaks = await repo.list_breaks()
    assert len(all_breaks) == 2
    open_breaks = await repo.list_breaks(status="OPEN")
    assert len(open_breaks) == 2
    usd_breaks = await repo.list_breaks(asset="USD")
    assert len(usd_breaks) == 1


@pytest.mark.asyncio
async def test_sql_repo_add_resolution_and_update_status(sqlite_repo):
    repo = sqlite_repo
    run = await repo.create_recon_run(source="RAILS", scope="daily")
    brk = await repo.create_break(
        run_id=run.id,
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    await repo.update_break_status(brk.id, "RESOLVED")
    resolution = await repo.add_break_resolution(brk.id, type="MANUAL", actor="ops", note="fixed")
    assert resolution is not None
    fetched = await repo.get_break(brk.id)
    assert fetched.status == "RESOLVED"
    assert fetched.resolved_at is not None


@pytest.mark.asyncio
async def test_sql_repo_open_timing_breaks_for(sqlite_repo):
    repo = sqlite_repo
    await repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("100"),
        external_amount=None,
        status="OPEN",
    )
    matches = await repo.open_timing_breaks_for("RAILS", "USD", "ref1")
    assert len(matches) == 1
    none_matches = await repo.open_timing_breaks_for("RAILS", "EUR", "ref1")
    assert len(none_matches) == 0


@pytest.mark.asyncio
async def test_sql_repo_upsert_rule(sqlite_repo):
    repo = sqlite_repo
    rule = await repo.upsert_rule(
        source="RAILS", asset="USD", match_strategy="FUZZY", tolerance_seconds=120
    )
    assert rule.match_strategy == "FUZZY"
    # Upsert again to update.
    await repo.upsert_rule(
        source="RAILS", asset="USD", match_strategy="EXACT", tolerance_seconds=60
    )
    rules = await repo.get_rules("RAILS")
    assert len(rules) == 1
    assert rules[0].match_strategy == "EXACT"
    assert rules[0].tolerance_seconds == 60
