"""Stage 9 tests: reports and exports."""

from __future__ import annotations

from decimal import Decimal

import pytest

from reconciliation.reports.generator import export_breaks, generate_run_report


@pytest.mark.asyncio
async def test_generate_run_report_summarizes_breaks(fake_repo):
    run = await fake_repo.create_recon_run(source="rails", scope="daily")
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
        run_id=run.id,
    )
    await fake_repo.create_break(
        source="rails",
        asset="EUR",
        reference="ref2",
        type="timing_gap",
        classification="timing",
        internal_amount=Decimal("50"),
        external_amount=None,
        status="open",
        run_id=run.id,
    )
    await fake_repo.complete_recon_run(run.id, matched=0, unmatched=2, breaks=2)
    run_obj = await fake_repo.get_recon_run(run.id)
    report = await generate_run_report(fake_repo, run_obj)
    assert report.run_id == run.id
    assert report.breaks_count == 2
    assert report.breaks_by_type["amount_mismatch"] == 1
    assert report.breaks_by_type["timing_gap"] == 1
    assert report.breaks_by_asset["USD"] == 1
    assert report.breaks_by_asset["EUR"] == 1
    csv_text = report.render_csv()
    assert "amount_mismatch" in csv_text
    assert "timing_gap" in csv_text


@pytest.mark.asyncio
async def test_export_breaks_csv(fake_repo):
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    text = await export_breaks(fake_repo, source="rails", fmt="csv")
    assert "amount_mismatch" in text


@pytest.mark.asyncio
async def test_export_breaks_json(fake_repo):
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    text = await export_breaks(fake_repo, fmt="json")
    assert "amount_mismatch" in text


@pytest.mark.asyncio
async def test_export_breaks_filters_by_status(fake_repo):
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref1",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="open",
    )
    await fake_repo.create_break(
        source="rails",
        asset="USD",
        reference="ref2",
        type="amount_mismatch",
        classification="real",
        internal_amount=Decimal("100"),
        external_amount=Decimal("90"),
        status="resolved",
    )
    text = await export_breaks(fake_repo, status="open", fmt="csv")
    assert "ref1" in text
    assert "ref2" not in text
