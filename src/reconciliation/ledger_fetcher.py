"""HTTP client that fetches ledger entries from the ledger-accounting service.

The ledger service exposes two read endpoints (see
``ledger-accounting/src/handlers.rs``):

* ``GET /v1/accounts/:id/ledger`` — paginated ledger for a single account,
  returning a ``LedgerPage`` (``{account_id, entries, next_cursor, final_balance}``).
* ``GET /v1/postings`` — flat list of ``PostingRecord`` objects
  (``{posting_id, ref_tx_id, memo, status, hash_head, entries, created_at}``)
  where each ``EntryRecord`` carries ``{entry_id, posting_id, account_id,
  direction, amount, asset, sequence_number, prev_hash, this_hash, created_at}``.

The recon engine consumes :class:`~reconciliation.matching.LedgerEntry`
objects keyed by ``reference`` (the posting id), ``asset``, signed ``amount``
(debit positive, credit negative), and ``timestamp``. The fetcher normalises
both shapes into that representation.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx

from .matching import LedgerEntry

logger = logging.getLogger(__name__)


def _signed_amount(direction: str, amount: Any) -> Decimal:
    raw = Decimal(str(amount))
    if direction.upper() == "CREDIT":
        return -raw
    return raw


def _parse_ts(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _entry_from_ledger_item(item: dict[str, Any]) -> LedgerEntry:
    """Build a LedgerEntry from a LedgerEntryItem (account-ledger endpoint)."""
    direction = item.get("direction") or "DEBIT"
    amount = _signed_amount(direction, item.get("amount", 0))
    return LedgerEntry(
        reference=str(item.get("posting_id") or item.get("entry_id") or ""),
        asset=str(item.get("asset") or ""),
        amount=amount,
        counterparty=str(item.get("account_id") or "") or None,
        timestamp=_parse_ts(item.get("created_at")),
    )


def _entries_from_posting(posting: dict[str, Any]) -> list[LedgerEntry]:
    """Build LedgerEntries from a PostingRecord (list-postings endpoint)."""
    out: list[LedgerEntry] = []
    reference = str(posting.get("posting_id") or posting.get("ref_tx_id") or "")
    posting_ts = _parse_ts(posting.get("created_at"))
    for entry in posting.get("entries") or []:
        direction = entry.get("direction") or "DEBIT"
        out.append(
            LedgerEntry(
                reference=reference,
                asset=str(entry.get("asset") or ""),
                amount=_signed_amount(direction, entry.get("amount", 0)),
                counterparty=str(entry.get("account_id") or "") or None,
                timestamp=_parse_ts(entry.get("created_at")) or posting_ts,
            )
        )
    return out


class LedgerFetcher:
    """Fetches :class:`LedgerEntry` rows from the ledger-accounting REST API."""

    def __init__(self, base_url: str, timeout: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: httpx.AsyncClient | None = None

    async def _acquire(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                base_url=self.base_url, timeout=self.timeout
            )
        return self._client

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def fetch_entries(
        self,
        account_id: str | None = None,
        since: datetime | None = None,
        limit: int = 1000,
    ) -> list[LedgerEntry]:
        """Fetch ledger entries; account-scoped via /v1/accounts/:id/ledger or flat via /v1/postings."""
        client = await self._acquire()
        params: dict[str, str] = {"limit": str(limit)}
        if since is not None:
            params["since"] = since.isoformat()
        entries: list[LedgerEntry] = []
        try:
            if account_id is not None:
                cursor: int | None = None
                while True:
                    if cursor is not None:
                        params["cursor"] = str(cursor)
                    resp = await client.get(
                        f"/v1/accounts/{account_id}/ledger", params=params
                    )
                    if resp.status_code == 404:
                        logger.warning(
                            "ledger account %s not found (404)", account_id
                        )
                        return []
                    resp.raise_for_status()
                    body = resp.json()
                    for item in body.get("entries") or []:
                        entries.append(_entry_from_ledger_item(item))
                    next_cursor = body.get("next_cursor")
                    if not next_cursor or len(entries) >= limit:
                        break
                    cursor = int(next_cursor)
            else:
                resp = await client.get("/v1/postings", params=params)
                resp.raise_for_status()
                body = resp.json()
                for posting in body.get("postings") or []:
                    entries.extend(_entries_from_posting(posting))
        except httpx.TimeoutException:
            logger.warning(
                "ledger fetch timed out (account=%s since=%s)", account_id, since
            )
            raise
        except httpx.HTTPStatusError as e:
            logger.warning(
                "ledger fetch failed status=%s body=%s",
                e.response.status_code,
                e.response.text[:200],
            )
            raise
        return entries[:limit]

    async def fetch_all(
        self, since: datetime | None = None, limit: int = 5000
    ) -> list[LedgerEntry]:
        """Fetch entries for every configured account, or the flat postings feed.

        ``RECON_ACCOUNTS`` (comma-separated) limits the scope; empty means
        the flat ``/v1/postings`` endpoint.
        """
        accounts_env = os.environ.get("RECON_ACCOUNTS", "") or ""
        accounts = [a.strip() for a in accounts_env.split(",") if a.strip()]
        if not accounts:
            return await self.fetch_entries(account_id=None, since=since, limit=limit)
        out: list[LedgerEntry] = []
        per_account = max(1, limit // max(1, len(accounts)))
        for acct in accounts:
            out.extend(
                await self.fetch_entries(account_id=acct, since=since, limit=per_account)
            )
        return out[:limit]


def build_ledger_fetcher(settings: Any) -> LedgerFetcher | None:
    """Factory: return a :class:`LedgerFetcher` if ``LEDGER_URL`` is configured."""
    base_url = getattr(settings, "ledger_url", "") or os.environ.get("LEDGER_URL", "")
    if not base_url:
        return None
    return LedgerFetcher(base_url=base_url)
