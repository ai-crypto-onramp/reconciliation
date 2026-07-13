"""SQLAlchemy 2.x ORM models for the Reconciliation service."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from ..config import (
    BREAK_CLASSIFICATIONS,
    BREAK_STATUSES,
    BREAK_TYPES,
    MATCH_STRATEGIES,
    RESOLUTION_TYPES,
    RUN_STATUSES,
    SOURCES,
)


class Base(DeclarativeBase):
    """Declarative base shared by all reconciliation models."""


def _check_in(column: str, values: tuple[str, ...]) -> CheckConstraint:
    rendered = ", ".join(f"'{v}'" for v in values)
    return CheckConstraint(f"{column} IN ({rendered})", name=f"ck_{column}_valid")


class ExternalEvent(Base):
    """Idempotent ingest of an event/snapshot from an upstream source.

    Keyed by (source, external_event_id) with a unique constraint so that
    redelivered events do not produce duplicate rows.
    """

    __tablename__ = "external_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    external_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    __table_args__ = (
        _check_in("source", SOURCES),
        Index("ix_external_events_source_ext_id", "source", "external_event_id", unique=False),
        Index("ix_external_events_source_ingested", "source", "ingested_at", unique=False),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<ExternalEvent {self.source}:{self.external_event_id}>"


class ReconRun(Base):
    """One row per reconciliation cycle (intraday or EOD)."""

    __tablename__ = "recon_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    scope: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="running")
    matched_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    unmatched_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    breaks_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    breaks: Mapped[list[Break]] = relationship(back_populates="run", cascade="save-update, merge")

    __table_args__ = (
        _check_in("source", SOURCES),
        _check_in("status", RUN_STATUSES),
        Index("ix_recon_runs_source_status", "source", "status", unique=False),
        Index("ix_recon_runs_started_at", "started_at", unique=False),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<ReconRun {self.id} {self.source}/{self.scope} {self.status}>"


class Break(Base):
    """A detected discrepancy between internal and external state."""

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
    internal_amount: Mapped[Decimal | None] = mapped_column(Numeric(28, 8), nullable=True)
    external_amount: Mapped[Decimal | None] = mapped_column(Numeric(28, 8), nullable=True)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="open")
    detected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    age_seconds: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    run: Mapped[ReconRun | None] = relationship(back_populates="breaks")
    resolutions: Mapped[list[BreakResolution]] = relationship(
        back_populates="break_", cascade="all, delete-orphan"
    )

    __table_args__ = (
        _check_in("type", BREAK_TYPES),
        _check_in("classification", BREAK_CLASSIFICATIONS),
        _check_in("status", BREAK_STATUSES),
        _check_in("source", SOURCES),
        CheckConstraint("age_seconds >= 0", name="ck_breaks_age_seconds_nonneg"),
        Index("ix_breaks_source_status", "source", "status", unique=False),
        Index("ix_breaks_classification_status", "classification", "status", unique=False),
        Index("ix_breaks_run_id", "run_id", unique=False),
        Index("ix_breaks_detected_at", "detected_at", unique=False),
        Index("ix_breaks_asset_status", "asset", "status", unique=False),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<Break {self.id} {self.type}/{self.classification} {self.source} {self.status}>"


class BreakResolution(Base):
    """Append-only resolution record for a break."""

    __tablename__ = "break_resolutions"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    break_id: Mapped[int] = mapped_column(
        BigInteger, ForeignKey("breaks.id", ondelete="CASCADE"), nullable=False
    )
    type: Mapped[str] = mapped_column(String(16), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    note: Mapped[str | None] = mapped_column(Text(), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    break_: Mapped[Break] = relationship(back_populates="resolutions")

    __table_args__ = (
        _check_in("type", RESOLUTION_TYPES),
        Index("ix_break_resolutions_break_id", "break_id", unique=False),
        Index("ix_break_resolutions_created_at", "created_at", unique=False),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<BreakResolution {self.id} {self.type} break={self.break_id}>"


class ReconRule(Base):
    """Configurable match strategy, tolerances, and escalation thresholds per source/asset."""

    __tablename__ = "recon_rules"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    asset: Mapped[str | None] = mapped_column(String(32), nullable=True)
    match_strategy: Mapped[str] = mapped_column(String(32), nullable=False, server_default="exact")
    tolerance_seconds: Mapped[int] = mapped_column(Integer, nullable=False, server_default="300")
    escalation_age_minutes: Mapped[int] = mapped_column(Integer, nullable=False, server_default="60")
    auto_resolve_timing: Mapped[bool] = mapped_column(
        nullable=False, server_default=text("true")
    )
    config: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=text("now()"), nullable=False
    )

    __table_args__ = (
        _check_in("match_strategy", MATCH_STRATEGIES),
        _check_in("source", SOURCES),
        CheckConstraint("tolerance_seconds >= 0", name="ck_recon_rules_tolerance_nonneg"),
        CheckConstraint("escalation_age_minutes >= 0", name="ck_recon_rules_escalation_age_nonneg"),
        Index("ix_recon_rules_source", "source", unique=False),
        Index("ix_recon_rules_source_asset", "source", "asset", unique=False),
    )

    def __repr__(self) -> str:  # pragma: no cover - debug helper
        return f"<ReconRule {self.id} {self.source}/{self.asset} {self.match_strategy}>"
