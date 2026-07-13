"""Auto-resolution of timing breaks.

When a new external event is ingested, we attempt to re-match it against any
open ``timing`` breaks for the same source/asset/reference. If a match is
found, the break is closed and a resolution row with ``type='auto'`` is
appended. Gated behind ``AUTO_RESOLVE_TIMING_BREAKS``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from ..db.repository import Repository
from ..matching import ExternalEntry, LedgerEntry, get_strategy


async def attempt_auto_resolve(
    repo: Repository,
    *,
    source: str,
    asset: str,
    reference: str | None,
    external_amount: Decimal | None,
    external_timestamp: datetime | None,
    tolerance_seconds: int = 300,
    auto_resolve_enabled: bool = True,
    now: datetime | None = None,
) -> list[dict[str, Any]]:
    """Try to auto-resolve open timing breaks for ``(source, asset, reference)``.

    Returns a list of dicts describing each resolution for audit emission.
    """
    if not auto_resolve_enabled:
        return []
    now = now or datetime.now(tz=UTC)
    open_breaks = await repo.open_timing_breaks_for(source, asset, reference)
    resolutions: list[dict[str, Any]] = []
    for brk in open_breaks:
        if external_amount is not None and brk.internal_amount is not None:
            if external_amount != brk.internal_amount:
                continue
        # Use fuzzy strategy to confirm the external event matches this break.
        ledger = [LedgerEntry(reference=brk.reference or "", asset=brk.asset, amount=brk.internal_amount or Decimal("0"), timestamp=brk.detected_at)]
        external = [ExternalEntry(external_event_id="", source=source, asset=asset, reference=reference, amount=external_amount, timestamp=external_timestamp)]
        result = get_strategy("fuzzy").match(ledger, external, tolerance_seconds=tolerance_seconds)
        if not result.matched:
            continue
        await repo.update_break_status(brk.id, "resolved", age_seconds=0)
        await repo.add_break_resolution(
            break_id=brk.id,
            type="auto",
            actor="system",
            note=f"auto-resolved by external event for {source}/{asset}/{reference}",
        )
        resolutions.append(
            {
                "break_id": brk.id,
                "action": "auto-resolved",
                "actor": "system",
                "before": {"status": "open"},
                "after": {"status": "resolved"},
            }
        )
    return resolutions
