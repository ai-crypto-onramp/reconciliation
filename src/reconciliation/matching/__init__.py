"""Match strategies for the reconciliation engine.

Three strategies are supported, selectable per source/asset via ``recon_rules``:
- ``exact``: amount, reference, and counterparty align precisely.
- ``fuzzy``: amount matches but the external confirmation may arrive within a
  configurable tolerance window (``tolerance_seconds``).
- ``balance_rollforward``: opening balance + net flow vs. closing balance per
  asset/source over the recon window.

Each strategy implements :class:`MatchStrategy` and returns a
:class:`MatchResult` describing matched pairs and unmatched candidates that
the break detector turns into breaks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from ..schemas import ExternalEventPayload


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """Internal ledger posting to match against external events."""

    reference: str
    asset: str
    amount: Decimal
    counterparty: str | None = None
    timestamp: datetime | None = None


@dataclass(frozen=True, slots=True)
class ExternalEntry:
    """External event/snapshot to match against the internal ledger."""

    external_event_id: str
    source: str
    asset: str
    reference: str | None
    amount: Decimal | None
    counterparty: str | None = None
    timestamp: datetime | None = None


@dataclass(slots=True)
class MatchedPair:
    ledger: LedgerEntry
    external: ExternalEntry
    strategy: str
    delta: Decimal | None = None


@dataclass(slots=True)
class UnmatchedLedger:
    entry: LedgerEntry
    reason: str = "no_external"


@dataclass(slots=True)
class UnmatchedExternal:
    entry: ExternalEntry
    reason: str = "no_ledger"


@dataclass(slots=True)
class BalanceResult:
    """Result of a balance roll-forward check."""

    asset: str
    source: str
    opening: Decimal
    net_flow: Decimal
    expected_closing: Decimal
    actual_closing: Decimal
    delta: Decimal
    matched: bool

    @property
    def unexplained_delta(self) -> Decimal:
        return self.delta


@dataclass(slots=True)
class MatchResult:
    matched: list[MatchedPair] = field(default_factory=list)
    unmatched_ledger: list[UnmatchedLedger] = field(default_factory=list)
    unmatched_external: list[UnmatchedExternal] = field(default_factory=list)
    balances: list[BalanceResult] = field(default_factory=list)
    duplicates: list[ExternalEntry] = field(default_factory=list)


class MatchStrategy(Protocol):
    """Interface implemented by every match strategy."""

    name: str

    def match(
        self,
        ledger: list[LedgerEntry],
        external: list[ExternalEntry],
        *,
        tolerance_seconds: int = 300,
    ) -> MatchResult: ...


class ExactMatchStrategy:
    """Match entries whose amount, reference, and counterparty align precisely."""

    name = "exact"

    def match(
        self,
        ledger: list[LedgerEntry],
        external: list[ExternalEntry],
        *,
        tolerance_seconds: int = 300,  # noqa: ARG002 - unused for exact
    ) -> MatchResult:
        result = MatchResult()
        used_external: set[int] = set()
        for le in ledger:
            matched = False
            for idx, ex in enumerate(external):
                if idx in used_external:
                    continue
                if (
                    le.reference == (ex.reference or "")
                    and le.asset == ex.asset
                    and le.amount == (ex.amount or Decimal("0"))
                    and (le.counterparty or None) == (ex.counterparty or None)
                ):
                    result.matched.append(MatchedPair(le, ex, self.name))
                    used_external.add(idx)
                    matched = True
                    break
            if not matched:
                result.unmatched_ledger.append(UnmatchedLedger(le))
        for idx, ex in enumerate(external):
            if idx not in used_external:
                result.unmatched_external.append(UnmatchedExternal(ex))
        # Detect duplicates: multiple external entries with same reference+amount.
        seen: dict[tuple[str | None, Decimal | None], int] = {}
        for ex in external:
            key = (ex.reference, ex.amount)
            if key in seen:
                seen[key] += 1
                if seen[key] == 2:
                    result.duplicates.append(ex)
            else:
                seen[key] = 1
        return result


class FuzzyMatchStrategy:
    """Match by amount within a tolerance time window.

    A pair whose amounts match but whose timestamps differ by less than
    ``tolerance_seconds`` is considered a *timing* match. Pairs outside the
    window are reported as unmatched so the break detector can classify them.
    """

    name = "fuzzy"

    def match(
        self,
        ledger: list[LedgerEntry],
        external: list[ExternalEntry],
        *,
        tolerance_seconds: int = 300,
    ) -> MatchResult:
        result = MatchResult()
        used_external: set[int] = set()
        for le in ledger:
            best_idx = -1
            best_delta: float | None = None
            for idx, ex in enumerate(external):
                if idx in used_external:
                    continue
                if le.asset != ex.asset:
                    continue
                if le.amount != (ex.amount or Decimal("0")):
                    continue
                if le.timestamp is not None and ex.timestamp is not None:
                    delta = abs((le.timestamp - ex.timestamp).total_seconds())
                    if delta > tolerance_seconds:
                        continue
                    if best_delta is None or delta < best_delta:
                        best_delta = delta
                        best_idx = idx
                else:
                    best_idx = idx
                    break
            if best_idx >= 0:
                result.matched.append(
                    MatchedPair(le, external[best_idx], self.name, delta=Decimal(str(best_delta or 0)))
                )
                used_external.add(best_idx)
            else:
                result.unmatched_ledger.append(UnmatchedLedger(le, reason="no_amount_or_time"))
        for idx, ex in enumerate(external):
            if idx not in used_external:
                result.unmatched_external.append(UnmatchedExternal(ex))
        return result


class BalanceRollforwardStrategy:
    """Reconcile opening balance + net flow against the closing balance.

    Each external entry contributes a signed flow; the strategy matches the
    sum of flows against the recorded opening/closing balances. Unexplained
    deltas surface as candidate breaks.
    """

    name = "balance_rollforward"

    def match(
        self,
        ledger: list[LedgerEntry],
        external: list[ExternalEntry],
        *,
        tolerance_seconds: int = 300,  # noqa: ARG002 - unused for balances
    ) -> MatchResult:
        result = MatchResult()
        # Group flows by asset.
        flows: dict[str, Decimal] = {}
        for ex in external:
            if ex.amount is None:
                continue
            flows[ex.asset] = flows.get(ex.asset, Decimal("0")) + ex.amount
        # Group ledger postings by asset.
        ledger_by_asset: dict[str, Decimal] = {}
        for le in ledger:
            ledger_by_asset[le.asset] = ledger_by_asset.get(le.asset, Decimal("0")) + le.amount
        # Treat ledger closing as the expected closing; opening assumed zero.
        for asset, net_flow in flows.items():
            expected_closing = ledger_by_asset.get(asset, Decimal("0"))
            actual_closing = net_flow
            delta = actual_closing - expected_closing
            result.balances.append(
                BalanceResult(
                    asset=asset,
                    source=external[0].source if external else "",
                    opening=Decimal("0"),
                    net_flow=net_flow,
                    expected_closing=expected_closing,
                    actual_closing=actual_closing,
                    delta=delta,
                    matched=delta == Decimal("0"),
                )
            )
        # Entries without a balance counterpart go to unmatched buckets.
        consumed_external: set[int] = set()
        for br in result.balances:
            for idx, ex in enumerate(external):
                if idx in consumed_external:
                    continue
                if ex.asset == br.asset and ex.amount is not None:
                    consumed_external.add(idx)
        for idx, ex in enumerate(external):
            if idx not in consumed_external:
                result.unmatched_external.append(UnmatchedExternal(ex))
        return result


STRATEGIES: dict[str, type[MatchStrategy]] = {
    "exact": ExactMatchStrategy,
    "fuzzy": FuzzyMatchStrategy,
    "balance_rollforward": BalanceRollforwardStrategy,
}


def get_strategy(name: str) -> MatchStrategy:
    """Return a strategy instance by name (default: ``exact``)."""
    cls = STRATEGIES.get(name, ExactMatchStrategy)
    return cls()


def external_entry_from_payload(source: str, payload: dict) -> ExternalEntry:
    """Convert a raw event payload into an :class:`ExternalEntry`."""
    parsed = ExternalEventPayload(source=source, **payload)
    return ExternalEntry(
        external_event_id=parsed.external_event_id,
        source=parsed.source,
        asset=parsed.asset or "",
        reference=parsed.reference,
        amount=parsed.amount,
        counterparty=parsed.counterparty,
        timestamp=parsed.timestamp,
    )
