"""Tests for the SQLAlchemy ORM model definitions (Stage 1).

Validates that all five required tables exist with the expected columns,
foreign keys, unique constraints, indexes, and CHECK constraints that gate
break/run/source enum values.
"""

from __future__ import annotations

from sqlalchemy import CheckConstraint, ForeignKey, ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.inspection import inspect

from reconciliation.db import (
    Base,
    Break,
    BreakResolution,
    ExternalEvent,
    ReconRule,
    ReconRun,
)
from reconciliation.db.models import (
    BREAK_CLASSIFICATIONS,
    BREAK_STATUSES,
    BREAK_TYPES,
    MATCH_STRATEGIES,
    RESOLUTION_TYPES,
    RUN_STATUSES,
    SOURCE_TYPES,
)

EXPECTED_TABLES = {
    "external_events",
    "recon_runs",
    "breaks",
    "break_resolutions",
    "recon_rules",
}


def test_all_five_tables_registered_on_metadata():
    assert EXPECTED_TABLES.issubset(set(Base.metadata.tables))


def test_external_events_idempotent_key():
    t = ExternalEvent.__table__
    cols = {c.name for c in t.columns}
    assert {"id", "source", "external_event_id", "payload", "ingested_at"} <= cols

    uq = [c for c in t.constraints if isinstance(c, UniqueConstraint)]
    assert any(
        {col.name for col in c.columns} == {"source", "external_event_id"}
        and c.name == "uq_external_events_source_ext_id"
        for c in uq
    )
    idx_names = {i.name for i in t.indexes}
    assert {
        "ix_external_events_source_ext_id",
        "ix_external_events_source_ingested",
    } <= idx_names


def test_recon_runs_columns_and_indexes():
    t = ReconRun.__table__
    cols = {c.name for c in t.columns}
    assert {
        "id",
        "source",
        "scope",
        "status",
        "matched_count",
        "unmatched_count",
        "breaks_count",
        "started_at",
        "completed_at",
    } <= cols
    idx_names = {i.name for i in t.indexes}
    assert {"ix_recon_runs_source_status", "ix_recon_runs_started_at"} <= idx_names


def test_breaks_fk_and_indexes():
    t = Break.__table__
    cols = {c.name for c in t.columns}
    assert {
        "id",
        "run_id",
        "type",
        "classification",
        "source",
        "asset",
        "reference",
        "internal_amount",
        "external_amount",
        "status",
        "detected_at",
        "resolved_at",
        "age_seconds",
    } <= cols

    fks = [c for c in t.constraints if isinstance(c, ForeignKeyConstraint)]
    assert any(
        any(fk.parent.name == "run_id" and fk.target_fullname == "recon_runs.id"
            for fk in c.elements)
        for c in fks
    )

    idx_names = {i.name for i in t.indexes}
    assert {
        "ix_breaks_source_status",
        "ix_breaks_classification_status",
        "ix_breaks_run_id",
        "ix_breaks_detected_at",
        "ix_breaks_asset_status",
    } <= idx_names


def test_break_resolutions_fk_to_breaks_and_appendonly():
    t = BreakResolution.__table__
    cols = {c.name for c in t.columns}
    assert {"id", "break_id", "type", "actor", "note", "created_at"} <= cols
    fks = [c for c in t.constraints if isinstance(c, ForeignKeyConstraint)]
    assert any(
        any(fk.parent.name == "break_id" and fk.target_fullname == "breaks.id"
            for fk in c.elements)
        for c in fks
    )
    # No `updated_at` column → append-only semantics.
    assert "updated_at" not in {c.name for c in t.columns}
    assert "resolved_at" not in {c.name for c in t.columns}


def test_recon_rules_unique_source_asset_and_strategy_check():
    t = ReconRule.__table__
    cols = {c.name for c in t.columns}
    assert {
        "id",
        "source",
        "asset",
        "match_strategy",
        "tolerance_seconds",
        "escalation_age_minutes",
        "auto_resolve_timing",
        "config",
        "created_at",
        "updated_at",
    } <= cols
    uq = [c for c in t.constraints if isinstance(c, UniqueConstraint)]
    assert any(
        {col.name for col in c.columns} == {"source", "asset"}
        and c.name == "uq_recon_rules_source_asset"
        for c in uq
    )
    idx_names = {i.name for i in t.indexes}
    assert {"ix_recon_rules_source", "ix_recon_rules_source_asset"} <= idx_names


def _check_sql(c: CheckConstraint, expected_values: tuple[str, ...]) -> bool:
    sql = str(c.sqltext)
    return all(f"'{v}'" in sql for v in expected_values) and " IN " in sql.upper()


def test_breaks_check_constraints_cover_enums():
    checks = [c for c in Break.__table__.constraints if isinstance(c, CheckConstraint)]
    found = {}
    for c in checks:
        sql = str(c.sqltext)
        if "type IN" in sql:
            found["type"] = c
        elif "classification IN" in sql:
            found["classification"] = c
        elif "status IN" in sql:
            found["status"] = c
        elif "source IN" in sql:
            found["source"] = c
        elif "age_seconds" in sql:
            found["age"] = c
    assert _check_sql(found["type"], BREAK_TYPES)
    assert _check_sql(found["classification"], BREAK_CLASSIFICATIONS)
    assert _check_sql(found["status"], BREAK_STATUSES)
    assert _check_sql(found["source"], SOURCE_TYPES)
    assert ">= 0" in str(found["age"].sqltext)


def test_recon_rules_check_constraints_cover_strategies_and_sources():
    checks = [c for c in ReconRule.__table__.constraints if isinstance(c, CheckConstraint)]
    found = {}
    for c in checks:
        sql = str(c.sqltext)
        if "match_strategy IN" in sql:
            found["strategy"] = c
        elif "source IN" in sql:
            found["source"] = c
        elif "tolerance_seconds" in sql:
            found["tol"] = c
        elif "escalation_age_minutes" in sql:
            found["esc"] = c
    assert _check_sql(found["strategy"], MATCH_STRATEGIES)
    assert _check_sql(found["source"], SOURCE_TYPES)
    assert ">= 0" in str(found["tol"].sqltext)
    assert ">= 0" in str(found["esc"].sqltext)


def test_break_resolutions_type_check():
    checks = [c for c in BreakResolution.__table__.constraints if isinstance(c, CheckConstraint)]
    assert any(
        "type IN" in str(c.sqltext)
        and all(f"'{v}'" in str(c.sqltext) for v in RESOLUTION_TYPES)
        for c in checks
    )


def test_recon_runs_status_check():
    checks = [c for c in ReconRun.__table__.constraints if isinstance(c, CheckConstraint)]
    assert any(
        "status IN" in str(c.sqltext)
        and all(f"'{v}'" in str(c.sqltext) for v in RUN_STATUSES)
        for c in checks
    )


def test_models_importable_from_db_package():
    # The `db` package re-exports the models + helpers used by later stages.
    from reconciliation import db

    assert {db.ExternalEvent, db.ReconRun, db.Break, db.BreakResolution, db.ReconRule}
    assert db.Base.metadata is Base.metadata


def test_get_sync_engine_raises_without_db_url():
    import pytest

    from reconciliation.db.session import get_sync_engine

    with pytest.raises(RuntimeError, match="DB_URL"):
        get_sync_engine()