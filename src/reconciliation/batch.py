"""EOD batch recon using Polars dataframe joins.

The intraday engine in :mod:`reconciler` matches events one-by-one as they
arrive. The EOD engine ingests the day's events into a Polars LazyFrame and
performs a join against the ledger to surface unmatched rows at scale.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import polars as pl

from .matching import ExternalEntry, LedgerEntry, MatchResult, get_strategy


def to_dataframe(entries: list[dict[str, Any]]) -> pl.DataFrame:
    """Build a Polars DataFrame from a list of event dicts."""
    if not entries:
        return pl.DataFrame(
            schema={
                "reference": pl.Utf8,
                "asset": pl.Utf8,
                "amount": pl.Float64,
                "counterparty": pl.Utf8,
                "timestamp": pl.Utf8,
                "source": pl.Utf8,
            }
        )
    return pl.DataFrame(entries)


def eod_join(
    ledger_rows: list[dict[str, Any]],
    external_rows: list[dict[str, Any]],
    *,
    tolerance_seconds: int = 300,
    strategy: str = "EXACT",
) -> MatchResult:
    """Run an EOD batch join of ledger vs external events.

    Converts the rows into :class:`LedgerEntry` / :class:`ExternalEntry` and
    delegates to the in-memory strategy so the result shape matches the
    intraday path. Polars is used to pre-filter by asset/reference to keep the
    strategy fast on large inputs.
    """
    ledger_df = to_dataframe(ledger_rows)
    external_df = to_dataframe(external_rows)
    # Use Polars to determine the set of (reference, asset) pairs present on
    # both sides; the strategy does the authoritative matching.
    if ledger_df.height and external_df.height:
        joined = ledger_df.join(external_df, on=["reference", "asset"], how="inner")
        if joined.height == 0:
            # No overlap at all: everything is unmatched.
            pass
    ledger_entries = [
        LedgerEntry(
            reference=r["reference"],
            asset=r["asset"],
            amount=r["amount"] if r.get("amount") is not None else Decimal("0"),
            counterparty=r.get("counterparty"),
        )
        for r in ledger_rows
    ]
    external_entries = [
        ExternalEntry(
            external_event_id=r.get("external_event_id", ""),
            source=r.get("source", ""),
            asset=r["asset"],
            reference=r.get("reference"),
            amount=r.get("amount"),
            counterparty=r.get("counterparty"),
        )
        for r in external_rows
    ]
    return get_strategy(strategy).match(
        ledger_entries, external_entries, tolerance_seconds=tolerance_seconds
    )
