"""Verifies the Stage 1 Alembic migration applies to a real PostgreSQL DB.

Skipped automatically when:
  - Docker is not available (no testcontainers), OR
  - DB_URL is not set and a local `pg_ctl`/`psql` cannot be reached.

We run a temporary Postgres cluster via `initdb`/`pg_ctl` in a tmp dir when
`psql` is available on PATH; otherwise we fall back to skipping.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

import pytest

from reconciliation.config import get_settings


def _find_postgres_binaries() -> tuple[str, str, str, str] | None:
    initdb = shutil.which("initdb")
    pg_ctl = shutil.which("pg_ctl")
    psql = shutil.which("psql")
    postgres = shutil.which("postgres")
    if initdb and pg_ctl and psql and postgres:
        return initdb, pg_ctl, psql, postgres
    return None


@pytest.fixture(scope="module")
def live_postgres_url(tmp_path_factory):
    """Start a throwaway Postgres cluster and return a sync DB_URL."""
    bins = _find_postgres_binaries()
    if bins is None:
        pytest.skip("Postgres binaries (initdb/pg_ctl/psql) not available on PATH")
    initdb, pg_ctl, psql, postgres = bins

    data_dir = tmp_path_factory.mktemp("pgdata")
    port_env = os.environ.get("PG_TEST_PORT", "5433")
    host = "127.0.0.1"
    url = f"postgresql://recon:recon@{host}:{port_env}/recon"

    subprocess.run([initdb, "-D", str(data_dir), "-U", "recon", "-A", "trust"],
                   check=True, capture_output=True)
    # Set a non-default port to avoid clashing with a system Postgres.
    (data_dir / "postgresql.conf").write_text(
        f"port = {port_env}\nlisten_addresses = '{host}'\n"
    )
    proc_log = data_dir / "pg.log"
    subprocess.run(
        [pg_ctl, "-D", str(data_dir), "-l", str(proc_log), "start", "-w", "-o", f"-p {port_env}"],
        check=True,
        capture_output=True,
    )
    try:
        # create the recon database
        for _ in range(30):
            res = subprocess.run(
                [psql, "-h", host, "-p", str(port_env), "-U", "recon", "-d", "postgres",
                 "-c", "SELECT 1"],
                capture_output=True,
            )
            if res.returncode == 0:
                break
            time.sleep(0.2)
        subprocess.run(
            [psql, "-h", host, "-p", str(port_env), "-U", "recon", "-d", "postgres",
             "-c", "CREATE DATABASE recon OWNER recon;"],
            check=True,
            capture_output=True,
        )
        os.environ["DB_URL"] = url
        get_settings.cache_clear()
        yield url
    finally:
        subprocess.run([pg_ctl, "-D", str(data_dir), "stop", "-m", "fast"],
                       capture_output=True)
        os.environ.pop("DB_URL", None)
        get_settings.cache_clear()


def _psql(psql_bin: str, url: str, sql: str) -> str:
    res = subprocess.run(
        [psql_bin, url, "-t", "-A", "-c", sql],
        capture_output=True,
        text=True,
        check=True,
    )
    return res.stdout


def test_migration_applies_and_creates_all_tables(live_postgres_url: str):
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "migrations")
    cfg.set_main_option("sqlalchemy.url", live_postgres_url)

    # Apply migration head against the empty DB.
    command.upgrade(cfg, "head")

    psql_bin = shutil.which("psql")
    assert psql_bin, "psql missing mid-test"
    tables = _psql(psql_bin, live_postgres_url,
                   "SELECT table_name FROM information_schema.tables "
                   "WHERE table_schema='public' ORDER BY table_name;")
    table_names = {t.strip() for t in tables.splitlines() if t.strip()}
    assert {
        "external_events",
        "recon_runs",
        "breaks",
        "break_resolutions",
        "recon_rules",
    } <= table_names
    # Alembic bookkeeping table.
    assert "alembic_version" in table_names


def test_migration_downgrades_cleanly(live_postgres_url: str):
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "migrations")
    cfg.set_main_option("sqlalchemy.url", live_postgres_url)

    command.upgrade(cfg, "head")
    command.downgrade(cfg, "base")

    psql_bin = shutil.which("psql")
    tables = _psql(psql_bin, live_postgres_url,
                   "SELECT table_name FROM information_schema.tables "
                   "WHERE table_schema='public' ORDER BY table_name;")
    table_names = {t.strip() for t in tables.splitlines() if t.strip()}
    assert "external_events" not in table_names
    assert "breaks" not in table_names
    assert "recon_rules" not in table_names


def test_indexes_present(live_postgres_url: str):
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "migrations")
    cfg.set_main_option("sqlalchemy.url", live_postgres_url)
    command.upgrade(cfg, "head")

    psql_bin = shutil.which("psql")
    indexes = _psql(psql_bin, live_postgres_url,
                    "SELECT indexname FROM pg_indexes WHERE schemaname='public' ORDER BY indexname;")
    index_names = {t.strip() for t in indexes.splitlines() if t.strip()}
    expected = {
        "ix_external_events_source_ext_id",
        "ix_external_events_source_ingested",
        "ix_recon_runs_source_status",
        "ix_recon_runs_started_at",
        "ix_breaks_source_status",
        "ix_breaks_classification_status",
        "ix_breaks_run_id",
        "ix_breaks_detected_at",
        "ix_breaks_asset_status",
        "ix_break_resolutions_break_id",
        "ix_break_resolutions_created_at",
        "ix_recon_rules_source",
        "ix_recon_rules_source_asset",
        "uq_external_events_source_ext_id",
        "uq_recon_rules_source_asset",
    }
    assert expected <= index_names, expected - index_names


def test_unique_constraint_blocks_duplicate_external_events(live_postgres_url: str):
    from alembic import command
    from alembic.config import Config

    cfg = Config("alembic.ini")
    cfg.set_main_option("script_location", "migrations")
    cfg.set_main_option("sqlalchemy.url", live_postgres_url)
    command.upgrade(cfg, "head")

    psql_bin = shutil.which("psql")
    insert = (
        "INSERT INTO external_events (source, external_event_id, payload) "
        "VALUES ('rails', 'ext-1', '{}'::jsonb);"
    )
    _psql(psql_bin, live_postgres_url, insert)
    with pytest.raises(subprocess.CalledProcessError):
        _psql(psql_bin, live_postgres_url, insert)