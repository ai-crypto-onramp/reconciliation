"""EOD report generation and break export."""

from __future__ import annotations

import csv
import io
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from ..db.repository import Repository


@dataclass
class RunReport:
    """Summary of a recon run and the breaks it produced."""

    run_id: int
    source: str
    scope: str
    status: str
    matched_count: int
    unmatched_count: int
    breaks_count: int
    started_at: datetime
    completed_at: datetime | None
    breaks_by_type: dict[str, int] = field(default_factory=dict)
    breaks_by_classification: dict[str, int] = field(default_factory=dict)
    breaks_by_asset: dict[str, int] = field(default_factory=dict)
    breaks: list[dict[str, Any]] = field(default_factory=list)

    def render_csv(self) -> str:
        """Render the per-break summary as CSV text."""
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            [
                "break_id",
                "type",
                "classification",
                "source",
                "asset",
                "reference",
                "internal_amount",
                "external_amount",
                "status",
                "detected_at",
                "age_seconds",
            ]
        )
        for b in self.breaks:
            writer.writerow(
                [
                    b.get("id"),
                    b.get("type"),
                    b.get("classification"),
                    b.get("source"),
                    b.get("asset"),
                    b.get("reference"),
                    b.get("internal_amount"),
                    b.get("external_amount"),
                    b.get("status"),
                    b.get("detected_at"),
                    b.get("age_seconds", 0),
                ]
            )
        return buf.getvalue()

    def render_json(self) -> str:
        """Render the full report as JSON text."""
        return json.dumps(self.__dict__, default=str, indent=2)


async def generate_run_report(repo: Repository, run: Any) -> RunReport:
    """Build a :class:`RunReport` from a ``ReconRun`` and its breaks."""
    breaks = await repo.list_breaks()
    run_breaks = [b for b in breaks if getattr(b, "run_id", None) == run.id]
    by_type: dict[str, int] = {}
    by_class: dict[str, int] = {}
    by_asset: dict[str, int] = {}
    serialized: list[dict[str, Any]] = []
    for b in run_breaks:
        by_type[b.type] = by_type.get(b.type, 0) + 1
        by_class[b.classification] = by_class.get(b.classification, 0) + 1
        by_asset[b.asset] = by_asset.get(b.asset, 0) + 1
        serialized.append(
            {
                "id": b.id,
                "type": b.type,
                "classification": b.classification,
                "source": b.source,
                "asset": b.asset,
                "reference": b.reference,
                "internal_amount": str(b.internal_amount) if b.internal_amount is not None else None,
                "external_amount": str(b.external_amount) if b.external_amount is not None else None,
                "status": b.status,
                "detected_at": b.detected_at.isoformat() if b.detected_at else None,
                "age_seconds": b.age_seconds,
            }
        )
    return RunReport(
        run_id=run.id,
        source=run.source,
        scope=run.scope,
        status=run.status,
        matched_count=run.matched_count,
        unmatched_count=run.unmatched_count,
        breaks_count=run.breaks_count,
        started_at=run.started_at,
        completed_at=run.completed_at,
        breaks_by_type=by_type,
        breaks_by_classification=by_class,
        breaks_by_asset=by_asset,
        breaks=serialized,
    )


async def archive_run_report(
    repo: Repository,
    run: Any,
    storage: Any,
    bucket: str,
) -> str:
    """Generate a run report and archive it to object storage.

    Returns the object key under which the CSV report was stored.
    """
    report = await generate_run_report(repo, run)
    csv_text = report.render_csv()
    key = f"reports/{run.source}/{run.id}.csv"
    await storage.put(bucket, key, csv_text.encode("utf-8"), content_type="text/csv")
    return key


async def export_breaks(
    repo: Repository,
    *,
    source: str | None = None,
    status: str | None = None,
    since: datetime | None = None,
    until: datetime | None = None,
    fmt: str = "csv",
) -> str:
    """Export filtered breaks as CSV or JSON text."""
    breaks = await repo.list_breaks(source=source, status=status, since=since, until=until)
    rows = [
        {
            "id": b.id,
            "type": b.type,
            "classification": b.classification,
            "source": b.source,
            "asset": b.asset,
            "reference": b.reference,
            "internal_amount": str(b.internal_amount) if b.internal_amount is not None else None,
            "external_amount": str(b.external_amount) if b.external_amount is not None else None,
            "status": b.status,
            "detected_at": b.detected_at.isoformat() if b.detected_at else None,
            "age_seconds": b.age_seconds,
        }
        for b in breaks
    ]
    if fmt == "json":
        return json.dumps(rows, default=str, indent=2)
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf,
        fieldnames=[
            "id",
            "type",
            "classification",
            "source",
            "asset",
            "reference",
            "internal_amount",
            "external_amount",
            "status",
            "detected_at",
            "age_seconds",
        ],
    )
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()
