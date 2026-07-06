"""Alembic migration environment for the reconciliation service.

Migrations target PostgreSQL. The sync (psycopg2) URL is resolved from
``DB_URL`` at runtime via :mod:`reconciliation.config`, and the metadata
is sourced from the ORM models in :mod:`reconciliation.db.models`.
"""

from __future__ import annotations

import logging
import sys
from logging.config import fileConfig
from pathlib import Path

from alembic import context
from sqlalchemy import engine_from_config, pool

# Ensure `src/` is importable so we can resolve the reconciliation package
# regardless of where alembic is invoked from.
THIS_DIR = Path(__file__).resolve().parent
ROOT = THIS_DIR.parent
sys.path.insert(0, str(ROOT / "src"))

from reconciliation.config import get_settings  # noqa: E402
from reconciliation.db.base import Base  # noqa: E402
from reconciliation.db import models  # noqa: F401,E402  (registers tables on Base.metadata)

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

log = logging.getLogger("alembic.env")

# Inject the sync DB URL from settings if not already set on the command line.
if not config.get_main_option("sqlalchemy.url"):
    settings = get_settings()
    sync_url = settings.db_url_sync or settings.DB_URL
    if sync_url:
        config.set_main_option("sqlalchemy.url", sync_url)

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()