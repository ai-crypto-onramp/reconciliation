"""CLI smoke tests."""

from __future__ import annotations

import uuid

import pytest

from reconciliation.cli import build_parser


def test_build_parser_requires_subcommand():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_build_parser_run_subcommand():
    parser = build_parser()
    args = parser.parse_args(["run", "--source", "RAILS", "--scope", "daily", "--mode", "eod"])
    assert args.command == "run"
    assert args.source == "RAILS"
    assert args.scope == "daily"
    assert args.mode == "eod"


def test_build_parser_status_subcommand():
    parser = build_parser()
    args = parser.parse_args(["status", "--run-id", "00000000-0000-7000-8000-000000000042"])
    assert args.command == "status"
    assert args.run_id == uuid.UUID("00000000-0000-7000-8000-000000000042")


def test_build_parser_report_subcommand():
    parser = build_parser()
    args = parser.parse_args(
        ["report", "--run-id", "00000000-0000-7000-8000-000000000042", "--out", "out.csv"]
    )
    assert args.command == "report"
    assert args.run_id == uuid.UUID("00000000-0000-7000-8000-000000000042")
    assert args.out == "out.csv"
