"""Stage 3 tests: match strategies."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

from reconciliation.matching import (
    BalanceRollforwardStrategy,
    ExactMatchStrategy,
    ExternalEntry,
    FuzzyMatchStrategy,
    LedgerEntry,
    get_strategy,
)


def _ledger(reference="ref1", asset="USD", amount=Decimal("100"), counterparty="bank1", ts=None):
    return LedgerEntry(reference=reference, asset=asset, amount=amount, counterparty=counterparty, timestamp=ts)


def _external(eid="ext1", source="rails", asset="USD", reference="ref1", amount=Decimal("100"), counterparty="bank1", ts=None):
    return ExternalEntry(external_event_id=eid, source=source, asset=asset, reference=reference, amount=amount, counterparty=counterparty, timestamp=ts)


class TestExactMatchStrategy:
    def test_matches_when_all_align(self):
        strategy = ExactMatchStrategy()
        result = strategy.match([_ledger()], [_external()])
        assert len(result.matched) == 1
        assert not result.unmatched_ledger
        assert not result.unmatched_external

    def test_no_match_when_amount_differs(self):
        strategy = ExactMatchStrategy()
        result = strategy.match([_ledger(amount=Decimal("100"))], [_external(amount=Decimal("99"))])
        assert not result.matched
        assert len(result.unmatched_ledger) == 1
        assert len(result.unmatched_external) == 1

    def test_no_match_when_reference_differs(self):
        strategy = ExactMatchStrategy()
        result = strategy.match([_ledger(reference="ref1")], [_external(reference="ref2")])
        assert not result.matched
        assert len(result.unmatched_ledger) == 1

    def test_duplicate_external_surfaces(self):
        strategy = ExactMatchStrategy()
        ext = [_external(eid="a"), _external(eid="b")]
        result = strategy.match([_ledger()], ext)
        assert result.duplicates, "duplicate should be flagged"


class TestFuzzyMatchStrategy:
    def test_matches_within_tolerance(self):
        strategy = FuzzyMatchStrategy()
        now = datetime.now(tz=UTC)
        le = _ledger(ts=now)
        ex = _external(ts=now + timedelta(seconds=60))
        result = strategy.match([le], [ex], tolerance_seconds=300)
        assert len(result.matched) == 1

    def test_unmatched_when_outside_tolerance(self):
        strategy = FuzzyMatchStrategy()
        now = datetime.now(tz=UTC)
        le = _ledger(ts=now)
        ex = _external(ts=now + timedelta(seconds=600))
        result = strategy.match([le], [ex], tolerance_seconds=300)
        assert not result.matched
        assert len(result.unmatched_ledger) == 1


class TestBalanceRollforwardStrategy:
    def test_surfaces_unexplained_delta(self):
        strategy = BalanceRollforwardStrategy()
        ledger = [_ledger(reference="r1", asset="USD", amount=Decimal("100"))]
        external = [_external(eid="e1", asset="USD", reference="r1", amount=Decimal("90"))]
        result = strategy.match(ledger, external)
        assert result.balances
        assert result.balances[0].delta != Decimal("0")
        assert not result.balances[0].matched

    def test_matched_when_balances_agree(self):
        strategy = BalanceRollforwardStrategy()
        ledger = [_ledger(reference="r1", asset="USD", amount=Decimal("100"))]
        external = [_external(eid="e1", asset="USD", reference="r1", amount=Decimal("100"))]
        result = strategy.match(ledger, external)
        assert result.balances[0].matched


def test_get_strategy_returns_correct_type():
    assert isinstance(get_strategy("exact"), ExactMatchStrategy)
    assert isinstance(get_strategy("fuzzy"), FuzzyMatchStrategy)
    assert isinstance(get_strategy("balance_rollforward"), BalanceRollforwardStrategy)
    # default
    assert isinstance(get_strategy("unknown"), ExactMatchStrategy)
