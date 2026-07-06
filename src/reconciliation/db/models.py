"""ORM models for the reconciliation service (Stage 1 schema).

Tables:
    external_events      — idempotent upstream ingest keyed by (source, external_event_id).
    recon_runs           — one row per recon cycle (intraday/EOD).
    breaks               — detected discrepancies with classification + aging.
    break_resolutions    — append-only resolution records keyed to breaks.id.
    recon_rules          — configurable match strategies/tolerances per source/asset.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from reconciliation.db.base import Base


# --- Enums (stored as TEXT with CHECK constraints for portability) ---------
SOURCE_TYPES = ("ledger", "rails", "exchanges", "onchain", "custody")
BREAK_TYPES = ("amount_mismatch", "timing_gap", "missing_entry", "duplicate")
BREAK_CLASSIFICATIONS = ("timing", "real")
BREAK_STATUSES = ("open", "resolved", "escalated", "closed")
RESOLUTION_TYPES = ("manual", "auto")
MATCH_STRATEGIES = ("exact", "fuzzy", "balance_rollforward")
RUN_STATUSES = ("running", "completed", "failed")


class ExternalEvent(Base):
    """Idempotent ingest of an event/snapshot from an upstream source.

    Natural key: (source, external_event_id). Upserts on this key guarantee
    that redelivered Kafka messages do not create duplicate rows.
    """

    __tablename__ = "external_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    external_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("source", "external_event_id", name="uq_external_events_source_ext_id"),
        Index("ix_external_events_source_ext_id", "source", "external_event_id"),
        Index("ix_external_events_source_ingested", "source", "ingested_at"),
        CheckConstraint(
            f"source IN {SOURCE_TYPES!r}".replace(")", ")"),
            name="ck_external_events_source",
        ),
    )


class ReconRun(Base):
    """One row per reconciliation cycle (intraday or end-of-day)."""

    __tablename__ = "recon_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="running")
    matched_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unmatched_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    breaks_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    breaks: Mapped[list[Break]] = relationship(back_populates="run")

    __table_args__ = (
        Index("ix_recon_runs_source_status", "source", "status"),
        Index("ix_recon_runs_started_at", "started_at"),
        CheckConstraint(f"source IN {SOURCE_TYPES!r}".replace(")", ")"), name="ck_recon_runs_source"),
        CheckConstraint(f"status IN {RUN_STATUSES!r}".replace(")", ")"), name="ck_recon_runs_status"),
    )


class Break(Base):
    """A detected reconciliation break.

    Links to the recon run that produced it and owns append-only
    break_resolutions rows.
    """

    __tablename__ = "breaks"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    run_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("recon_runs.id", ondelete="SET NULL"), nullable=True
    )
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    classification: Mapped[str] = mapped_column(String(16), nullable=False)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    asset: Mapped[str] = mapped_column(String(32), nullable=False)
    reference: Mapped[str | None] = mapped_column(String(128), nullable=True)
    internal_amount: Mapped[float | None] = mapped_column(Numeric(28, 8), nullable=True)
    external_amount: Mapped[float | None] = mapped_column(Numeric(28, 8), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="open")
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    age_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    run: Mapped[ReconRun | None] = relationship(back_populates="breaks")
    resolutions: Mapped[list[BreakResolution]] = relationship(
        back_populates="break", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_breaks_source_status", "source", "status"),
        Index("ix_breaks_classification_status", "classification", "status"),
        Index("ix_breaks_run_id", "run_id"),
        Index("ix_breaks_detected_at", "detected_at"),
        Index("ix_breaks_asset_status", "asset", "status"),
        CheckConstraint(f"type IN {BREAK_TYPES!r}".replace(")", ")"), name="ck_breaks_type"),
        CheckConstraint(
            f"classification IN {BREAK_CLASSIFICATIONS!r}".replace(")", ")"),
            name="ck_breaks_classification",
        ),
        CheckConstraint(f"status IN {BREAK_STATUSES!r}".replace(")", ")"), name="ck_breaks_status"),
        CheckConstraint(f"source IN {SOURCE_TYPES!r}".replace(")", ")"), name="ck_breaks_source"),
        CheckConstraint("age_seconds >= 0", name="ck_breaks_age_seconds_nonneg"),
    )


class BreakResolution(Base):
    """Append-only resolution record keyed to breaks.id."""

    __tablename__ = "break_resolutions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    break_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("breaks.id", ondelete="CASCADE"), nullable=False
    )
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    break_: Mapped[Break] = relationship(back_populates="resolutions")

    __table_args__ = (
        Index("ix_break_resolutions_break_id", "break_id"),
        Index("ix_break_resolutions_created_at", "created_at"),
        CheckConstraint(
            f"type IN {RESOLUTION_TYPES!r}".replace(")", ")"), name="ck_break_resolutions_type"
        ),
    )


class ReconRule(Base):
    """Configurable match strategy, tolerances, and escalation thresholds.

    One row per (source, asset). NULL asset means a source-wide default.
    """

    __tablename__ = "recon_rules"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    asset: Mapped[str | None] = mapped_column(String(32), nullable=True)
    match_strategy: Mapped[str] = mapped_column(String(32), nullable=False, default="exact")
    tolerance_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=300)
    escalation_age_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    auto_resolve_timing: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("source", "asset", name="uq_recon_rules_source_asset"),
        Index("ix_recon_rules_source", "source"),
        Index("ix_recon_rules_source_asset", "source", "asset"),
        CheckConstraint(
            f"match_strategy IN {MATCH_STRATEGIES!r}".replace(")", ")"),
            name="ck_recon_rules_match_strategy",
        ),
        CheckConstraint(f"source IN {SOURCE_TYPES!r}".replace(")", ")"), name="ck_recon_rules_source"),
        CheckConstraint("tolerance_seconds >= 0", name="ck_recon_rules_tolerance_nonneg"),
        CheckConstraint(
            "escalation_age_minutes >= 0", name="ck_recon_rules_escalation_age_nonneg"
        ),
    )