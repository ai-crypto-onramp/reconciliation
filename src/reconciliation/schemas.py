"""Pydantic schemas for inbound events and outbound API models."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .config import (
    BREAK_CLASSIFICATIONS,
    BREAK_STATUSES,
    BREAK_TYPES,
    RESOLUTION_TYPES,
    SOURCES,
)


class ExternalEventPayload(BaseModel):
    """Schema for an event payload ingested from an upstream source."""

    model_config = ConfigDict(extra="allow")

    external_event_id: str = Field(..., description="Producer-assigned idempotency key")
    source: str = Field(..., description="Upstream source (ledger/rails/exchanges/onchain/custody)")
    asset: str | None = None
    reference: str | None = None
    amount: Decimal | None = None
    timestamp: datetime | None = None
    counterparty: str | None = None


class BreakOut(BaseModel):
    """Public representation of a break."""

    id: int
    run_id: int | None = None
    type: str
    classification: str
    source: str
    asset: str
    reference: str | None = None
    internal_amount: Decimal | None = None
    external_amount: Decimal | None = None
    status: str
    detected_at: datetime
    resolved_at: datetime | None = None
    age_seconds: int = 0
    resolutions: list[BreakResolutionOut] = Field(default_factory=list)

    model_config = ConfigDict(from_attributes=True)


class BreakResolutionOut(BaseModel):
    id: int
    break_id: int
    type: str
    actor: str
    note: str | None = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


class BreakListResponse(BaseModel):
    breaks: list[BreakOut]
    total: int


class ResolveBreakRequest(BaseModel):
    actor: str = Field(..., min_length=1, max_length=128)
    note: str | None = None


class EscalateBreakRequest(BaseModel):
    actor: str = Field(default="system", min_length=1, max_length=128)
    note: str | None = None


class ReconRunCreateRequest(BaseModel):
    source: str = Field(..., description="Upstream source to run recon against")
    scope: str = Field("daily")
    mode: str = Field("eod", pattern="^(intraday|eod)$")


class ReconRunOut(BaseModel):
    id: int
    source: str
    scope: str
    status: str
    matched_count: int
    unmatched_count: int
    breaks_count: int
    started_at: datetime
    completed_at: datetime | None = None

    model_config = ConfigDict(from_attributes=True)


class ReconRunCreatedResponse(BaseModel):
    id: int
    status: str


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


class BreakAlertEvent(BaseModel):
    """Event payload emitted to the ``break-alert`` topic (Notification)."""

    break_id: int
    type: str
    classification: str
    source: str
    asset: str
    reference: str | None = None
    internal_amount: Decimal | None = None
    external_amount: Decimal | None = None
    detected_at: datetime
    age_seconds: int = 0
    action: str = Field(..., description="detected/classified/auto-resolved/escalated/manually-resolved")
    actor: str = "system"
    timestamp: datetime = Field(default_factory=_utcnow)


class BreakAuditEvent(BaseModel):
    """Event payload emitted to the ``break-event`` topic (Audit Event Log)."""

    break_id: int
    action: str
    actor: str = "system"
    before: dict[str, Any] = Field(default_factory=dict)
    after: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=_utcnow)


BreakOut.model_rebuild()


__all__ = [
    "ExternalEventPayload",
    "BreakOut",
    "BreakResolutionOut",
    "BreakListResponse",
    "ResolveBreakRequest",
    "EscalateBreakRequest",
    "ReconRunCreateRequest",
    "ReconRunOut",
    "ReconRunCreatedResponse",
    "BreakAlertEvent",
    "BreakAuditEvent",
    "SOURCES",
    "BREAK_TYPES",
    "BREAK_CLASSIFICATIONS",
    "BREAK_STATUSES",
    "RESOLUTION_TYPES",
]
