"""Stage 9 tests: reports and exports."""

from __future__ import annotations

from decimal import Decimal

import pytest

from reconciliation.reports.generator import export_breaks, generate_run_report


@pytest.mark.asyncio
async def test_generate_run_report_summarizes_breaks(fake_repo):
    run = await fake_repo.create_recon_run(source="RAILS", scope="daily")
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
        run_id=run.id,
    )
    await fake_repo.create_break(
        source="RAILS",
        asset="EUR",
        reference="ref2",
        type="TIMING_GAP",
        classification="TIMING",
        internal_amount=Decimal("50"),
        external_amount=None,
        status="OPEN",
        run_id=run.id,
    )
    await fake_repo.complete_recon_run(run.id, matched=0, unmatched=2, breaks=2)
    run_obj = await fake_repo.get_recon_run(run.id)
    report = await generate_run_report(fake_repo, run_obj)
    assert report.run_id == run.id
    assert report.breaks_count == 2
    assert report.breaks_by_type["AMOUNT_MISMATCH"] == 1
    assert report.breaks_by_type["TIMING_GAP"] == 1
    assert report.breaks_by_asset["USD"] == 1
    assert report.breaks_by_asset["EUR"] == 1
    csv_text = report.render_csv()
    assert "AMOUNT_MISMATCH" in csv_text
    assert "TIMING_GAP" in csv_text


@pytest.mark.asyncio
async def test_export_breaks_csv(fake_repo):
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    text = await export_breaks(fake_repo, source="RAILS", fmt="csv")
    assert "AMOUNT_MISMATCH" in text


@pytest.mark.asyncio
async def test_export_breaks_json(fake_repo):
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    text = await export_breaks(fake_repo, fmt="json")
    assert "AMOUNT_MISMATCH" in text


@pytest.mark.asyncio
async def test_export_breaks_filters_by_status(fake_repo):
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref1",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="OPEN",
    )
    await fake_repo.create_break(
        source="RAILS",
        asset="USD",
        reference="ref2",
        type="AMOUNT_MISMATCH",
        classification="REAL",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="RESOLVED",
    )
    text = await export_breaks(fake_repo, status="OPEN", fmt="csv")
    assert "ref1" in text
    assert "ref2" not in text
