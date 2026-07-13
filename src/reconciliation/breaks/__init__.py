"""Break detection and classification.

Turns a :class:`MatchResult` into concrete ``Break`` rows. Enforces the
no-false-negatives rule: every unmatched ledger/external pair and every
balance delta produces a break.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..db.repository import Repository
from ..matching import MatchedPair, MatchResult


def classify_timing(
    *,
    tolerance_seconds: int,
    ledger_ts: datetime | None,
    external_ts: datetime | None,
    now: datetime | None = None,
) -> str:
    """Classify an unmatched pair as ``timing`` or ``real``.

    If the difference between ledger and external timestamps is within
    ``tolerance_seconds`` (or either timestamp is unknown), treat as timing
    and expect auto-resolution; otherwise it's a genuine discrepancy.
    """
    if ledger_ts is None or external_ts is None:
        return "timing"
    delta = abs((ledger_ts - external_ts).total_seconds())
    if delta <= tolerance_seconds:
        return "timing"
    return "real"


def classify_amount_mismatch(
    *,
    tolerance_seconds: int,
    pair: MatchedPair,
) -> str:
    """A matched pair with a non-zero delta is classified as timing when the
    delta is within tolerance, else real."""
    if pair.delta is None or pair.delta == Decimal("0"):
        return "timing"
    if abs(float(pair.delta)) <= tolerance_seconds:
        return "timing"
    return "real"


async def detect_and_persist_breaks(
    repo: Repository,
    result: MatchResult,
    *,
    run_id: int,
    source: str,
    tolerance_seconds: int = 300,
    now: datetime | None = None,
) -> list[Any]:
    """Persist a :class:`Break` row for every unmatched pair or balance delta.

    Returns the list of created break dicts (without ORM instances) for audit
    emission.
    """
    now = now or datetime.now(tz=UTC)
    created: list[dict[str, Any]] = []

    for unmatched_ledger in result.unmatched_ledger:
        le = unmatched_ledger.entry
        classification = classify_timing(
            tolerance_seconds=tolerance_seconds,
            ledger_ts=le.timestamp,
            external_ts=None,
            now=now,
        )
        break_type = "timing_gap" if classification == "timing" else "missing_entry"
        brk = await repo.create_break(
            run_id=run_id,
            source=source,
            asset=le.asset,
            reference=le.reference,
            type=break_type,
            classification=classification,
            internal_amount=le.amount,
            external_amount=None,
            status="open",
            detected_at=now,
            age_seconds=0,
        )
        created.append({"id": brk.id, "type": break_type, "classification": classification, "action": "detected"})

    for unmatched_external in result.unmatched_external:
        ex = unmatched_external.entry
        # Missing ledger entry for an external confirmation.
        classification = "real"
        break_type = "missing_entry"
        brk = await repo.create_break(
            run_id=run_id,
            source=source,
            asset=ex.asset,
            reference=ex.reference,
            type=break_type,
            classification=classification,
            internal_amount=None,
            external_amount=ex.amount,
            status="open",
            detected_at=now,
            age_seconds=0,
        )
        created.append({"id": brk.id, "type": break_type, "classification": classification, "action": "detected"})

    for balance in result.balances:
        if balance.matched:
            continue
        brk = await repo.create_break(
            run_id=run_id,
            source=balance.source or source,
            asset=balance.asset,
            reference=None,
            type="amount_mismatch",
            classification="real",
            internal_amount=balance.expected_closing,
            external_amount=balance.actual_closing,
            status="open",
            detected_at=now,
            age_seconds=0,
        )
        created.append(
            {"id": brk.id, "type": "amount_mismatch", "classification": "real", "action": "detected"}
        )

    for duplicate in result.duplicates:
        brk = await repo.create_break(
            run_id=run_id,
            source=duplicate.source,
            asset=duplicate.asset,
            reference=duplicate.reference,
            type="duplicate",
            classification="real",
            internal_amount=None,
            external_amount=duplicate.amount,
            status="open",
            detected_at=now,
            age_seconds=0,
        )
        created.append({"id": brk.id, "type": "duplicate", "classification": "real", "action": "detected"})

    return created


def compute_age(detected_at: datetime, now: datetime | None = None) -> int:
    """Return seconds since ``detected_at``."""
    now = now or datetime.now(tz=UTC)
    delta = now - detected_at
    return max(0, int(delta.total_seconds()))
