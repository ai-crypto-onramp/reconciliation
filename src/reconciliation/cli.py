"""Command-line interface for the reconciliation service.

Stage 1 scaffolding: wires up `python -m reconciliation.cli` with the
subcommands required by later stages (`run`, `migrate`, `reconcile`).
For Stage 1 we expose `migrate` so operators can apply Alembic migrations
from the CLI.
"""

from __future__ import annotations

import argparse
import logging
import sys
from typing import Sequence

from reconciliation.config import get_settings

logger = logging.getLogger("reconciliation.cli")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="reconciliation.cli", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    # migrate -----------------------------------------------------------------
    p_migrate = sub.add_parser("migrate", help="Apply Alembic migrations to DB_URL.")
    p_migrate.add_argument("--revision", default="head", help="Target Alembic revision.")
    p_migrate.set_defaults(func=cmd_migrate)

    # run (stub for Stage 7) -------------------------------------------------
    p_run = sub.add_parser("run", help="Trigger a recon run (Stage 7).")
    p_run.add_argument("--source", required=True)
    p_run.add_argument("--scope", default="daily")
    p_run.set_defaults(func=cmd_run)

    return parser


def cmd_migrate(args: argparse.Namespace) -> int:
    """Apply Alembic migrations up to the target revision."""
    from alembic import command
    from alembic.config import Config

    settings = get_settings()
    if not settings.DB_URL:
        logger.error("DB_URL is not configured")
        return 2

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "migrations")
    cfg.set_main_option("sqlalchemy.url", settings.db_url_sync or settings.DB_URL)
    command.upgrade(cfg, args.revision)
    return 0


def cmd_run(args: argparse.Namespace) -> int:
    """Stub for triggering a recon run (fully implemented in Stage 7)."""
    logger.info(
        "recon run requested (source=%s scope=%s) — implemented in Stage 7",
        args.source,
        args.scope,
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=get_settings_loglevel())
    return args.func(args)


def get_settings_loglevel() -> str:
    try:
        return get_settings().LOG_LEVEL.upper()
    except Exception:  # pragma: no cover - defensive
        return "INFO"


if __name__ == "__main__":
    sys.exit(main())