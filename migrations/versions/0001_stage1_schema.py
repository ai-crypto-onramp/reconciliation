"""Stage 1: initial schema (external_events, recon_runs, breaks, break_resolutions, recon_rules).

Revision ID: 0001_stage1_schema
Revises:
Create Date: 2026-07-06
"""

from __future__ import annotations

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001_stage1_schema"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

SOURCE_TYPES = ("ledger", "rails", "exchanges", "onchain", "custody")
BREAK_TYPES = ("amount_mismatch", "timing_gap", "missing_entry", "duplicate")
BREAK_CLASSIFICATIONS = ("timing", "real")
BREAK_STATUSES = ("open", "resolved", "escalated", "closed")
RESOLUTION_TYPES = ("manual", "auto")
MATCH_STRATEGIES = ("exact", "fuzzy", "balance_rollforward")
RUN_STATUSES = ("running", "completed", "failed")


def _txt_tuple(values: tuple[str, ...]) -> str:
    """Render a SQL tuple literal suitable for a CHECK constraint IN list."""
    return "(" + ", ".join(f"'{v}'" for v in values) + ")"


def upgrade() -> None:
    op.create_table(
        "external_events",
        sa.Column("id", sa.BigInteger, autoincrement=True, nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("external_event_id", sa.String(128), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("ingested_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_external_events")),
        sa.UniqueConstraint("source", "external_event_id", name="uq_external_events_source_ext_id"),
        sa.CheckConstraint(f"source IN {_txt_tuple(SOURCE_TYPES)}", name="ck_external_events_source"),
    )
    op.create_index("ix_external_events_source_ext_id", "external_events", ["source", "external_event_id"], unique=False)
    op.create_index("ix_external_events_source_ingested", "external_events", ["source", "ingested_at"], unique=False)

    op.create_table(
        "recon_runs",
        sa.Column("id", sa.BigInteger, autoincrement=True, nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("scope", sa.String(64), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="running"),
        sa.Column("matched_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("unmatched_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("breaks_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_recon_runs")),
        sa.CheckConstraint(f"source IN {_txt_tuple(SOURCE_TYPES)}", name="ck_recon_runs_source"),
        sa.CheckConstraint(f"status IN {_txt_tuple(RUN_STATUSES)}", name="ck_recon_runs_status"),
    )
    op.create_index("ix_recon_runs_source_status", "recon_runs", ["source", "status"], unique=False)
    op.create_index("ix_recon_runs_started_at", "recon_runs", ["started_at"], unique=False)

    op.create_table(
        "breaks",
        sa.Column("id", sa.BigInteger, autoincrement=True, nullable=False),
        sa.Column("run_id", sa.BigInteger, sa.ForeignKey("recon_runs.id", ondelete="SET NULL"), nullable=True),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("classification", sa.String(16), nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(32), nullable=False),
        sa.Column("reference", sa.String(128), nullable=True),
        sa.Column("internal_amount", sa.Numeric(28, 8), nullable=True),
        sa.Column("external_amount", sa.Numeric(28, 8), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="open"),
        sa.Column("detected_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("age_seconds", sa.Integer, nullable=False, server_default="0"),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_breaks")),
        sa.CheckConstraint(f"type IN {_txt_tuple(BREAK_TYPES)}", name="ck_breaks_type"),
        sa.CheckConstraint(f"classification IN {_txt_tuple(BREAK_CLASSIFICATIONS)}", name="ck_breaks_classification"),
        sa.CheckConstraint(f"status IN {_txt_tuple(BREAK_STATUSES)}", name="ck_breaks_status"),
        sa.CheckConstraint(f"source IN {_txt_tuple(SOURCE_TYPES)}", name="ck_breaks_source"),
        sa.CheckConstraint("age_seconds >= 0", name="ck_breaks_age_seconds_nonneg"),
    )
    op.create_index("ix_breaks_source_status", "breaks", ["source", "status"], unique=False)
    op.create_index("ix_breaks_classification_status", "breaks", ["classification", "status"], unique=False)
    op.create_index("ix_breaks_run_id", "breaks", ["run_id"], unique=False)
    op.create_index("ix_breaks_detected_at", "breaks", ["detected_at"], unique=False)
    op.create_index("ix_breaks_asset_status", "breaks", ["asset", "status"], unique=False)

    op.create_table(
        "break_resolutions",
        sa.Column("id", sa.BigInteger, autoincrement=True, nullable=False),
        sa.Column("break_id", sa.BigInteger, sa.ForeignKey("breaks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("type", sa.String(16), nullable=False),
        sa.Column("actor", sa.String(128), nullable=False),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_break_resolutions")),
        sa.CheckConstraint(f"type IN {_txt_tuple(RESOLUTION_TYPES)}", name="ck_break_resolutions_type"),
    )
    op.create_index("ix_break_resolutions_break_id", "break_resolutions", ["break_id"], unique=False)
    op.create_index("ix_break_resolutions_created_at", "break_resolutions", ["created_at"], unique=False)

    op.create_table(
        "recon_rules",
        sa.Column("id", sa.BigInteger, autoincrement=True, nullable=False),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("asset", sa.String(32), nullable=True),
        sa.Column("match_strategy", sa.String(32), nullable=False, server_default="exact"),
        sa.Column("tolerance_seconds", sa.Integer, nullable=False, server_default="300"),
        sa.Column("escalation_age_minutes", sa.Integer, nullable=False, server_default="60"),
        sa.Column("auto_resolve_timing", sa.Boolean, nullable=False, server_default=sa.text("true")),
        sa.Column("config", postgresql.JSONB(astext_type=sa.Text()), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_recon_rules")),
        sa.UniqueConstraint("source", "asset", name="uq_recon_rules_source_asset"),
        sa.CheckConstraint(f"match_strategy IN {_txt_tuple(MATCH_STRATEGIES)}", name="ck_recon_rules_match_strategy"),
        sa.CheckConstraint(f"source IN {_txt_tuple(SOURCE_TYPES)}", name="ck_recon_rules_source"),
        sa.CheckConstraint("tolerance_seconds >= 0", name="ck_recon_rules_tolerance_nonneg"),
        sa.CheckConstraint("escalation_age_minutes >= 0", name="ck_recon_rules_escalation_age_nonneg"),
    )
    op.create_index("ix_recon_rules_source", "recon_rules", ["source"], unique=False)
    op.create_index("ix_recon_rules_source_asset", "recon_rules", ["source", "asset"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_recon_rules_source_asset", table_name="recon_rules")
    op.drop_index("ix_recon_rules_source", table_name="recon_rules")
    op.drop_table("recon_rules")
    op.drop_index("ix_break_resolutions_created_at", table_name="break_resolutions")
    op.drop_index("ix_break_resolutions_break_id", table_name="break_resolutions")
    op.drop_table("break_resolutions")
    op.drop_index("ix_breaks_asset_status", table_name="breaks")
    op.drop_index("ix_breaks_detected_at", table_name="breaks")
    op.drop_index("ix_breaks_run_id", table_name="breaks")
    op.drop_index("ix_breaks_classification_status", table_name="breaks")
    op.drop_index("ix_breaks_source_status", table_name="breaks")
    op.drop_table("breaks")
    op.drop_index("ix_recon_runs_started_at", table_name="recon_runs")
    op.drop_index("ix_recon_runs_source_status", table_name="recon_runs")
    op.drop_table("recon_runs")
    op.drop_index("ix_external_events_source_ingested", table_name="external_events")
    op.drop_index("ix_external_events_source_ext_id", table_name="external_events")
    op.drop_table("external_events")