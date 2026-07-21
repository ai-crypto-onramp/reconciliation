"""Tests for the LedgerFetcher HTTP client."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import httpx
import pytest

from reconciliation.ledger_fetcher import (
    LedgerFetcher,
    _entries_from_posting,
    _entry_from_ledger_item,
)


def test_entry_from_ledger_item_signs_credit_negative():
    item = {
        "entry_id": "e1",
        "posting_id": "p1",
        "account_id": "acct1",
        "direction": "CREDIT",
        "amount": 100,
        "asset": "USD",
        "created_at": "2024-01-01T00:00:00Z",
    }
    entry = _entry_from_ledger_item(item)
    assert entry.reference == "p1"
    assert entry.asset == "USD"
    assert entry.amount == Decimal("-100")
    assert entry.counterparty == "acct1"
    assert entry.timestamp == datetime.fromisoformat("2024-01-01T00:00:00+00:00")


def test_entry_from_ledger_item_debit_positive():
    item = {
        "entry_id": "e2",
        "posting_id": "p2",
        "direction": "DEBIT",
        "amount": 50,
        "asset": "EUR",
        "created_at": "2024-01-02T00:00:00Z",
    }
    entry = _entry_from_ledger_item(item)
    assert entry.amount == Decimal("50")


def test_entries_from_posting_returns_one_per_entry():
    posting = {
        "posting_id": "p9",
        "ref_tx_id": "tx9",
        "created_at": "2024-03-01T00:00:00Z",
        "entries": [
            {"account_id": "a", "direction": "DEBIT", "amount": 10, "asset": "USD"},
            {"account_id": "b", "direction": "CREDIT", "amount": 10, "asset": "USD"},
        ],
    }
    entries = _entries_from_posting(posting)
    assert len(entries) == 2
    assert entries[0].amount == Decimal("10")
    assert entries[1].amount == Decimal("-10")
    assert entries[0].reference == "p9"


@pytest.mark.asyncio
async def test_fetch_entries_flat_postings(monkeypatch):
    fetcher = LedgerFetcher(base_url="http://ledger:8080")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            json={
                "postings": [
                    {
                        "posting_id": "p1",
                        "created_at": "2024-01-01T00:00:00Z",
                        "entries": [
                            {
                                "account_id": "a",
                                "direction": "DEBIT",
                                "amount": 100,
                                "asset": "USD",
                                "created_at": "2024-01-01T00:00:00Z",
                            }
                        ],
                    }
                ]
            },
        )
    )
    fetcher._client = httpx.AsyncClient(
        base_url="http://ledger:8080", transport=transport
    )
    entries = await fetcher.fetch_entries(limit=10)
    await fetcher.aclose()
    assert len(entries) == 1
    assert entries[0].reference == "p1"
    assert entries[0].amount == Decimal("100")


@pytest.mark.asyncio
async def test_fetch_entries_account_scoped():
    fetcher = LedgerFetcher(base_url="http://ledger:8080")

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/accounts/acct1/ledger"
        return httpx.Response(
            200,
            json={
                "account_id": "acct1",
                "entries": [
                    {
                        "entry_id": "e1",
                        "posting_id": "p1",
                        "account_id": "acct1",
                        "direction": "DEBIT",
                        "amount": 5,
                        "asset": "USD",
                        "created_at": "2024-01-01T00:00:00Z",
                    }
                ],
                "next_cursor": None,
                "final_balance": 5,
            },
        )

    fetcher._client = httpx.AsyncClient(
        base_url="http://ledger:8080", transport=httpx.MockTransport(handler)
    )
    entries = await fetcher.fetch_entries(account_id="acct1", limit=100)
    await fetcher.aclose()
    assert len(entries) == 1
    assert entries[0].asset == "USD"


@pytest.mark.asyncio
async def test_fetch_entries_404_returns_empty():
    fetcher = LedgerFetcher(base_url="http://ledger:8080")
    transport = httpx.MockTransport(lambda request: httpx.Response(404, json={}))
    fetcher._client = httpx.AsyncClient(
        base_url="http://ledger:8080", transport=transport
    )
    entries = await fetcher.fetch_entries(account_id="missing")
    await fetcher.aclose()
    assert entries == []


@pytest.mark.asyncio
async def test_fetch_entries_timeout_propagates():
    fetcher = LedgerFetcher(base_url="http://ledger:8080", timeout=0.001)

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.TimeoutException("timed out", request=request)

    fetcher._client = httpx.AsyncClient(
        base_url="http://ledger:8080", transport=httpx.MockTransport(handler)
    )
    with pytest.raises(httpx.TimeoutException):
        await fetcher.fetch_entries(account_id="acct1")
    await fetcher.aclose()


@pytest.mark.asyncio
async def test_fetch_all_uses_postings_when_no_accounts(monkeypatch):
    fetcher = LedgerFetcher(base_url="http://ledger:8080")
    monkeypatch.delenv("RECON_ACCOUNTS", raising=False)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/postings"
        return httpx.Response(
            200,
            json={
                "postings": [
                    {
                        "posting_id": "px",
                        "created_at": "2024-01-01T00:00:00Z",
                        "entries": [
                            {
                                "account_id": "a",
                                "direction": "DEBIT",
                                "amount": 1,
                                "asset": "USD",
                                "created_at": "2024-01-01T00:00:00Z",
                            }
                        ],
                    }
                ]
            },
        )

    fetcher._client = httpx.AsyncClient(
        base_url="http://ledger:8080", transport=httpx.MockTransport(handler)
    )
    entries = await fetcher.fetch_all()
    await fetcher.aclose()
    assert len(entries) == 1
    assert entries[0].reference == "px"


@pytest.mark.asyncio
async def test_fetch_all_iterates_accounts(monkeypatch):
    monkeypatch.setenv("RECON_ACCOUNTS", "a,b")
    fetcher = LedgerFetcher(base_url="http://ledger:8080")

    def handler(request: httpx.Request) -> httpx.Response:
        acct = request.url.path.split("/")[-2]
        return httpx.Response(
            200,
            json={
                "account_id": acct,
                "entries": [
                    {
                        "entry_id": f"e-{acct}",
                        "posting_id": f"p-{acct}",
                        "account_id": acct,
                        "direction": "DEBIT",
                        "amount": 7,
                        "asset": "USD",
                        "created_at": "2024-01-01T00:00:00Z",
                    }
                ],
                "next_cursor": None,
                "final_balance": 7,
            },
        )

    fetcher._client = httpx.AsyncClient(
        base_url="http://ledger:8080", transport=httpx.MockTransport(handler)
    )
    entries = await fetcher.fetch_all()
    await fetcher.aclose()
    refs = {e.reference for e in entries}
    assert refs == {"p-a", "p-b"}


def test_build_ledger_fetcher_none_when_no_url(monkeypatch):
    from reconciliation.config import Settings
    from reconciliation.ledger_fetcher import build_ledger_fetcher

    monkeypatch.delenv("LEDGER_URL", raising=False)
    settings = Settings(ledger_url="")
    assert build_ledger_fetcher(settings) is None


def test_build_ledger_fetcher_returns_client(monkeypatch):
    from reconciliation.config import Settings
    from reconciliation.ledger_fetcher import build_ledger_fetcher

    settings = Settings(ledger_url="http://ledger:8080/")
    fetcher = build_ledger_fetcher(settings)
    assert fetcher is not None
    assert fetcher.base_url == "http://ledger:8080"
