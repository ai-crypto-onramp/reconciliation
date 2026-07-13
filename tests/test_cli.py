"""CLI smoke tests."""

from __future__ import annotations

import pytest

from reconciliation.cli import build_parser


def test_build_parser_requires_subcommand():
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_build_parser_run_subcommand():
    parser = build_parser()
    args = parser.parse_args(["run", "--source", "rails", "--scope", "daily", "--mode", "eod"])
    assert args.command == "run"
    assert args.source == "rails"
    assert args.scope == "daily"
    assert args.mode == "eod"


def test_build_parser_status_subcommand():
    parser = build_parser()
    args = parser.parse_args(["status", "--run-id", "42"])
    assert args.command == "status"
    assert args.run_id == 42


def test_build_parser_report_subcommand():
    parser = build_parser()
    args = parser.parse_args(["report", "--run-id", "42", "--out", "out.csv"])
    assert args.command == "report"
    assert args.run_id == 42
    assert args.out == "out.csv"
