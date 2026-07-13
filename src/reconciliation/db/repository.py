"""Repository pattern for external events, breaks, runs, resolutions, rules.

A repository wraps a SQLAlchemy async session. Tests may pass a real SQLite
session or instantiate a ``FakeRepository`` (in ``tests/fakes``) that mimics
the same interface without any database.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any, Protocol

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import Break, BreakResolution, ExternalEvent, ReconRule, ReconRun


class Repository(Protocol):
    """Minimal repository protocol used by the service layer."""

    async def upsert_external_event(
        self, source: str, external_event_id: str, payload: dict[str, Any]
    ) -> tuple[ExternalEvent, bool]:
        """Insert or ignore; return ``(event, created)``."""

    async def list_external_events(
        self, source: str | None = None, limit: int = 1000
    ) -> Sequence[ExternalEvent]: ...

    async def create_recon_run(self, source: str, scope: str) -> ReconRun: ...
    async def get_recon_run(self, run_id: int) -> ReconRun | None: ...
    async def complete_recon_run(
        self,
        run_id: int,
        *,
        matched: int,
        unmatched: int,
        breaks: int,
        status: str = "completed",
    ) -> ReconRun | None: ...
    async def list_recon_runs(self, source: str | None = None) -> Sequence[ReconRun]: ...

    async def create_break(self, **fields: Any) -> Break: ...
    async def get_break(self, break_id: int) -> Break | None: ...
    async def list_breaks(
        self,
        *,
        source: str | None = None,
        status: str | None = None,
        classification: str | None = None,
        asset: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[Break]: ...
    async def update_break_status(
        self, break_id: int, status: str, age_seconds: int | None = None
    ) -> Break | None: ...
    async def add_break_resolution(
        self, break_id: int, type: str, actor: str, note: str | None = None
    ) -> BreakResolution | None: ...
    async def open_timing_breaks_for(
        self, source: str, asset: str, reference: str | None
    ) -> Sequence[Break]: ...

    async def get_rules(self, source: str, asset: str | None = None) -> Sequence[ReconRule]: ...
    async def upsert_rule(self, **fields: Any) -> ReconRule: ...

    async def commit(self) -> None: ...


class SqlRepository:
    """Concrete repository backed by an ``AsyncSession``."""

    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert_external_event(
        self, source: str, external_event_id: str, payload: dict[str, Any]
    ) -> tuple[ExternalEvent, bool]:
        stmt = select(ExternalEvent).where(
            ExternalEvent.source == source,
            ExternalEvent.external_event_id == external_event_id,
        )
        existing = (await self.session.execute(stmt)).scalar_one_or_none()
        if existing is not None:
            return existing, False
        event = ExternalEvent(source=source, external_event_id=external_event_id, payload=payload)
        self.session.add(event)
        await self.session.flush()
        return event, True

    async def list_external_events(
        self, source: str | None = None, limit: int = 1000
    ) -> Sequence[ExternalEvent]:
        stmt = select(ExternalEvent).limit(limit)
        if source is not None:
            stmt = stmt.where(ExternalEvent.source == source).order_by(ExternalEvent.ingested_at)
        return (await self.session.execute(stmt)).scalars().all()

    async def create_recon_run(self, source: str, scope: str) -> ReconRun:
        run = ReconRun(source=source, scope=scope, status="running")
        self.session.add(run)
        await self.session.flush()
        return run

    async def get_recon_run(self, run_id: int) -> ReconRun | None:
        return await self.session.get(ReconRun, run_id)

    async def complete_recon_run(
        self,
        run_id: int,
        *,
        matched: int,
        unmatched: int,
        breaks: int,
        status: str = "completed",
    ) -> ReconRun | None:
        run = await self.get_recon_run(run_id)
        if run is None:
            return None
        run.matched_count = matched
        run.unmatched_count = unmatched
        run.breaks_count = breaks
        run.status = status
        run.completed_at = datetime.now(run.started_at.tzinfo) if run.started_at else datetime.utcnow()
        await self.session.flush()
        return run

    async def list_recon_runs(self, source: str | None = None) -> Sequence[ReconRun]:
        stmt = select(ReconRun)
        if source is not None:
            stmt = stmt.where(ReconRun.source == source)
        stmt = stmt.order_by(ReconRun.started_at.desc())
        return (await self.session.execute(stmt)).scalars().all()

    async def create_break(self, **fields: Any) -> Break:
        brk = Break(**fields)
        self.session.add(brk)
        await self.session.flush()
        return brk

    async def get_break(self, break_id: int) -> Break | None:
        return await self.session.get(Break, break_id)

    async def list_breaks(
        self,
        *,
        source: str | None = None,
        status: str | None = None,
        classification: str | None = None,
        asset: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[Break]:
        conditions = []
        if source is not None:
            conditions.append(Break.source == source)
        if status is not None:
            conditions.append(Break.status == status)
        if classification is not None:
            conditions.append(Break.classification == classification)
        if asset is not None:
            conditions.append(Break.asset == asset)
        if since is not None:
            conditions.append(Break.detected_at >= since)
        if until is not None:
            conditions.append(Break.detected_at <= until)
        stmt = select(Break)
        if conditions:
            stmt = stmt.where(and_(*conditions))
        stmt = stmt.order_by(Break.detected_at.desc()).limit(limit)
        return (await self.session.execute(stmt)).scalars().all()

    async def update_break_status(
        self, break_id: int, status: str, age_seconds: int | None = None
    ) -> Break | None:
        brk = await self.get_break(break_id)
        if brk is None:
            return None
        brk.status = status
        if age_seconds is not None:
            brk.age_seconds = age_seconds
        if status in ("resolved", "closed") and brk.resolved_at is None:
            brk.resolved_at = datetime.now(tz=UTC)
        await self.session.flush()
        return brk

    async def add_break_resolution(
        self, break_id: int, type: str, actor: str, note: str | None = None
    ) -> BreakResolution | None:
        brk = await self.get_break(break_id)
        if brk is None:
            return None
        resolution = BreakResolution(break_id=break_id, type=type, actor=actor, note=note)
        self.session.add(resolution)
        await self.session.flush()
        return resolution

    async def open_timing_breaks_for(
        self, source: str, asset: str, reference: str | None
    ) -> Sequence[Break]:
        stmt = select(Break).where(
            Break.source == source,
            Break.asset == asset,
            Break.classification == "timing",
            Break.status == "open",
        )
        if reference is not None:
            stmt = stmt.where(Break.reference == reference)
        return (await self.session.execute(stmt)).scalars().all()

    async def get_rules(self, source: str, asset: str | None = None) -> Sequence[ReconRule]:
        stmt = select(ReconRule).where(ReconRule.source == source)
        if asset is not None:
            stmt = stmt.where((ReconRule.asset == asset) | (ReconRule.asset.is_(None)))
        return (await self.session.execute(stmt)).scalars().all()

    async def upsert_rule(self, **fields: Any) -> ReconRule:
        source = fields.get("source")
        asset = fields.get("asset")
        stmt = select(ReconRule).where(ReconRule.source == source)
        if asset is not None:
            stmt = stmt.where(ReconRule.asset == asset)
        else:
            stmt = stmt.where(ReconRule.asset.is_(None))
        existing = (await self.session.execute(stmt)).scalar_one_or_none()
        if existing is None:
            rule = ReconRule(**fields)
            self.session.add(rule)
            await self.session.flush()
            return rule
        for key, value in fields.items():
            setattr(existing, key, value)
        await self.session.flush()
        return existing

    async def commit(self) -> None:
        await self.session.commit()
