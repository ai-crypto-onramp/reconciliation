"""Alembic environment.

Runs migrations against the DB_URL from settings when available, otherwise
falls back to the URL configured in alembic.ini.
"""

from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

config = context.config
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Override sqlalchemy.url with DB_URL env var if present.
db_url = os.environ.get("DB_URL")
if db_url:
    # Alembic's default environment uses a sync engine; if an async driver is
    # configured (asyncpg/aiomysql), swap it for the sync equivalent so the
    # offline migration runner can introspect the DB.
    sync_url = db_url.replace("+asyncpg", "+psycopg2").replace("+aiosqlite", "").replace("+aiomysql", "+pymysql")
    config.set_main_option("sqlalchemy.url", sync_url)

target_metadata = None


def run_migrations_offline() -> None:
    url = config.get_main_option("sqlalchemy.url")
    context.configure(url=url, literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()