"""Stage 4 tests: break detection and classification."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from reconciliation.breaks import (
    classify_amount_mismatch,
    classify_timing,
    compute_age,
    detect_and_persist_breaks,
)
from reconciliation.matching import (
    BalanceResult,
    ExternalEntry,
    LedgerEntry,
    MatchedPair,
    MatchResult,
    UnmatchedExternal,
    UnmatchedLedger,
)


@pytest.mark.asyncio
async def test_detect_amount_mismatch(fake_repo):
    now = datetime.now(tz=UTC)
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
    result = MatchResult(
        balances=[
            BalanceResult(
                asset="USD",
                source="RAILS",
                opening=Decimal("0"),
                net_flow=Decimal("90"),
                expected_closing=Decimal("100"),
                actual_closing=Decimal("90"),
                delta=Decimal("-10"),
                matched=False,
            )
        ]
    )
    created = await detect_and_persist_breaks(
        fake_repo, result, run_id=run.id, source="RAILS", now=now
    )
    assert len(created) == 1
    assert created[0]["type"] == "AMOUNT_MISMATCH"
    assert created[0]["classification"] == "REAL"
    breaks = await fake_repo.list_breaks()
    assert len(breaks) == 1
    assert breaks[0].type == "AMOUNT_MISMATCH"


@pytest.mark.asyncio
async def test_detect_timing_gap(fake_repo):
    now = datetime.now(tz=UTC)
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
    ledger_ts = now
    result = MatchResult(
        unmatched_ledger=[
            UnmatchedLedger(
                entry=LedgerEntry(
                    reference="ref1", asset="USD", amount=Decimal("100"), timestamp=ledger_ts
                )
            )
        ]
    )
    created = await detect_and_persist_breaks(
        fake_repo, result, run_id=run.id, source="RAILS", tolerance_seconds=300, now=now
    )
    assert created[0]["type"] == "TIMING_GAP"
    assert created[0]["classification"] == "TIMING"


@pytest.mark.asyncio
async def test_detect_missing_entry(fake_repo):
    now = datetime.now(tz=UTC)
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
    result = MatchResult(
        unmatched_external=[
            UnmatchedExternal(
                entry=ExternalEntry(
                    external_event_id="e1",
                    source="RAILS",
                    asset="USD",
                    reference="ref1",
                    amount=Decimal("100"),
                )
            )
        ]
    )
    created = await detect_and_persist_breaks(
        fake_repo, result, run_id=run.id, source="RAILS", now=now
    )
    assert created[0]["type"] == "MISSING_ENTRY"
    assert created[0]["classification"] == "REAL"


@pytest.mark.asyncio
async def test_detect_duplicate(fake_repo):
    now = datetime.now(tz=UTC)
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
    dup = ExternalEntry(
        external_event_id="e1", source="RAILS", asset="USD", reference="ref1", amount=Decimal("100")
    )
    result = MatchResult(duplicates=[dup])
    created = await detect_and_persist_breaks(
        fake_repo, result, run_id=run.id, source="RAILS", now=now
    )
    assert created[0]["type"] == "DUPLICATE"
    assert created[0]["classification"] == "REAL"


def test_classify_timing_within_tolerance():
    now = datetime.now(tz=UTC)
    assert (
        classify_timing(
            tolerance_seconds=300,
            ledger_ts=now,
            external_ts=now + timedelta(seconds=60),
        )
        == "TIMING"
    )


def test_classify_timing_outside_tolerance():
    now = datetime.now(tz=UTC)
    assert (
        classify_timing(
            tolerance_seconds=300,
            ledger_ts=now,
            external_ts=now + timedelta(seconds=600),
        )
        == "REAL"
    )


def test_classify_timing_missing_timestamp_defaults_to_timing():
    assert classify_timing(tolerance_seconds=300, ledger_ts=None, external_ts=None) == "TIMING"


def test_compute_age():
    now = datetime.now(tz=UTC)
    detected = now - timedelta(seconds=120)
    assert compute_age(detected, now=now) == 120


def test_compute_age_never_negative():
    now = datetime.now(tz=UTC)
    detected = now + timedelta(seconds=30)
    assert compute_age(detected, now=now) == 0


def test_classify_amount_mismatch_within_tolerance():
    pair = MatchedPair(
        ledger=LedgerEntry(reference="r1", asset="USD", amount=Decimal("100")),
        external=ExternalEntry(
            external_event_id="e1",
            source="RAILS",
            asset="USD",
            reference="r1",
            amount=Decimal("100"),
        ),
        strategy="EXACT",
        delta=Decimal("5"),
    )
    assert classify_amount_mismatch(tolerance_seconds=10, pair=pair) == "TIMING"
