"""CLI for ad-hoc reconciliation runs.

Usage:
    python -m reconciliation.cli run --source rails --scope daily
    python -m reconciliation.cli report --run-id 42
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
import uuid

from .config import SOURCES, get_settings
from .reconciler import Reconciler


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="reconciliation", description="Reconciliation service CLI"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Trigger a recon run for a source/scope")
    run_p.add_argument("--source", required=True, choices=list(SOURCES))
    run_p.add_argument("--scope", required=True, default="daily")
    run_p.add_argument("--mode", choices=["intraday", "eod"], default="eod")

    status_p = sub.add_parser("status", help="Show status of a recon run")
    status_p.add_argument("--run-id", type=uuid.UUID, required=True)

    report_p = sub.add_parser("report", help="Generate an EOD report for a run")
    report_p.add_argument("--run-id", type=uuid.UUID, required=True)
    report_p.add_argument("--out", default="-", help="output file ('-' for stdout)")

    return parser


async def _run_recon(source: str, scope: str, mode: str) -> int:
    settings = get_settings()
    reconciler = Reconciler.from_settings(settings)
    run = await reconciler.execute(source=source, scope=scope, mode=mode)
    print(f"run id={run.id} source={run.source} scope={run.scope} status={run.status}")
    return 0


async def _run_status(run_id: uuid.UUID) -> int:
    settings = get_settings()
    reconciler = Reconciler.from_settings(settings)
    run = await reconciler.get_run(run_id)
    if run is None:
        print(f"run {run_id} not found", file=sys.stderr)
        return 1
    print(
        f"run {run.id} source={run.source} scope={run.scope} status={run.status} "
        f"matched={run.matched_count} unmatched={run.unmatched_count} breaks={run.breaks_count}"
    )
    return 0


async def _run_report(run_id: uuid.UUID, out: str) -> int:
    from .reports.generator import generate_run_report

    settings = get_settings()
    reconciler = Reconciler.from_settings(settings)
    run = await reconciler.get_run(run_id)
    if run is None:
        print(f"run {run_id} not found", file=sys.stderr)
        return 1
    report = await generate_run_report(reconciler.repo, run)
    text = report.render_csv()
    if out == "-":
        sys.stdout.write(text)
    else:
        with open(out, "w", encoding="utf-8") as f:
            f.write(text)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=get_settings().log_level.upper())

    if args.command == "run":
        return asyncio.run(_run_recon(args.source, args.scope, args.mode))
    if args.command == "status":
        return asyncio.run(_run_status(args.run_id))
    if args.command == "report":
        return asyncio.run(_run_report(args.run_id, args.out))
    parser.print_help()
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
