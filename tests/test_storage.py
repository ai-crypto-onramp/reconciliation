"""Stage 9 tests: object storage archiving."""

from __future__ import annotations

import pytest

from reconciliation.config import Settings
from reconciliation.storage import InMemoryObjectStorage, build_storage


@pytest.mark.asyncio
async def test_in_memory_storage_put_and_get():
    storage = InMemoryObjectStorage()
    await storage.put("bucket", "key.csv", b"hello", content_type="text/csv")
    data = await storage.get("bucket", "key.csv")
    assert data == b"hello"
    keys = await storage.list_keys("bucket", "key")
    assert keys == ["key.csv"]
    url = await storage.signed_url("bucket", "key.csv")
    assert "key.csv" in url


def test_build_storage_returns_in_memory_when_no_bucket():
    settings = Settings(reports_bucket="")
    storage = build_storage(settings)
    assert isinstance(storage, InMemoryObjectStorage)


@pytest.mark.asyncio
async def test_archive_run_report_round_trip(fake_repo):
    from reconciliation.reports.generator import archive_run_report
    from reconciliation.storage import InMemoryObjectStorage

    run = await fake_repo.create_recon_run(source="rails", scope="daily")
    await fake_repo.complete_recon_run(run.id, matched=1, unmatched=0, breaks=0)
    run_obj = await fake_repo.get_recon_run(run.id)
    storage = InMemoryObjectStorage()
    key = await archive_run_report(fake_repo, run_obj, storage, "reports")
    assert key == f"reports/rails/{run.id}.csv"
    data = await storage.get("reports", key)
    assert b"break_id" in data
