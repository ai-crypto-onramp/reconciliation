"""Test fixtures and helpers shared across the test suite."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Sequence
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from reconciliation.db.models import Base
from reconciliation.db.repository import Repository, SqlRepository


def _new_uuid() -> uuid.UUID:
    """Generate a UUID; prefers v7 on Python 3.14+, falls back to v4."""
    gen = getattr(uuid, "uuid7", None)
    if gen is not None:
        return gen()
    return uuid.uuid4()


class FakeRepository:
    """In-memory repository that satisfies the :class:`Repository` protocol.

    Mirrors the SqlRepository interface without any database. Used in unit
    tests that exercise the matching, break detection, auto-resolution, aging,
    and escalation logic without spinning up SQLite.
    """

    def __init__(self) -> None:
        self._events: list[dict[str, Any]] = []
        self._event_ids: set[tuple[str, str]] = set()
        self._runs: list[dict[str, Any]] = []
        self._breaks: list[dict[str, Any]] = []
        self._resolutions: list[dict[str, Any]] = []
        self._rules: list[dict[str, Any]] = []
        self._next_id = 1

    def _id(self) -> uuid.UUID:
        self._next_id += 1
        return _new_uuid()

    async def upsert_external_event(
        self, source: str, external_event_id: str, payload: dict[str, Any]
    ) -> tuple[Any, bool]:
        key = (source, external_event_id)
        if key in self._event_ids:
            existing = next(e for e in self._events if (e["source"], e["external_event_id"]) == key)
            return _DictObj(existing), False
        record = {
            "id": self._id(),
            "source": source,
            "external_event_id": external_event_id,
            "payload": payload,
            "ingested_at": datetime.now(tz=UTC),
        }
        self._events.append(record)
        self._event_ids.add(key)
        return _DictObj(record), True

    async def list_external_events(
        self, source: str | None = None, limit: int = 1000
    ) -> Sequence[Any]:
        items = (
            self._events if source is None else [e for e in self._events if e["source"] == source]
        )
        return [_DictObj(e) for e in items[:limit]]

    async def create_recon_run(self, source: str, scope: str) -> Any:
        record = {
            "id": self._id(),
            "source": source,
            "scope": scope,
            "status": "RUNNING",
            "matched_count": 0,
            "unmatched_count": 0,
            "breaks_count": 0,
            "started_at": datetime.now(tz=UTC),
            "completed_at": None,
        }
        self._runs.append(record)
        return _DictObj(record)

    async def get_recon_run(self, run_id: uuid.UUID) -> Any | None:
        for r in self._runs:
            if r["id"] == run_id:
                return _DictObj(r)
        return None

    async def complete_recon_run(
        self,
        run_id: uuid.UUID,
        *,
        matched: int,
        unmatched: int,
        breaks: int,
        status: str = "COMPLETED",
    ) -> Any | None:
        for r in self._runs:
            if r["id"] == run_id:
                r["matched_count"] = matched
                r["unmatched_count"] = unmatched
                r["breaks_count"] = breaks
                r["status"] = status
                r["completed_at"] = datetime.now(tz=UTC)
                return _DictObj(r)
        return None

    async def list_recon_runs(self, source: str | None = None) -> Sequence[Any]:
        items = self._runs if source is None else [r for r in self._runs if r["source"] == source]
        return [_DictObj(r) for r in items]

    async def create_break(self, **fields: Any) -> Any:
        record = {
            "id": self._id(),
            "run_id": fields.get("run_id"),
            "type": fields["type"],
            "classification": fields["classification"],
            "source": fields["source"],
            "asset": fields["asset"],
            "reference": fields.get("reference"),
            "internal_amount": fields.get("internal_amount"),
            "external_amount": fields.get("external_amount"),
            "status": fields.get("status", "OPEN"),
            "detected_at": fields.get("detected_at", datetime.now(tz=UTC)),
            "resolved_at": fields.get("resolved_at"),
            "age_seconds": fields.get("age_seconds", 0),
            "resolutions": [],
        }
        self._breaks.append(record)
        return _DictObj(record)

    async def get_break(self, break_id: uuid.UUID) -> Any | None:
        for b in self._breaks:
            if b["id"] == break_id:
                return _DictObj(b)
        return None

    async def list_breaks(self, **filters: Any) -> Sequence[Any]:
        items = list(self._breaks)
        if filters.get("source") is not None:
            items = [b for b in items if b["source"] == filters["source"]]
        if filters.get("status") is not None:
            items = [b for b in items if b["status"] == filters["status"]]
        if filters.get("classification") is not None:
            items = [b for b in items if b["classification"] == filters["classification"]]
        if filters.get("asset") is not None:
            items = [b for b in items if b["asset"] == filters["asset"]]
        since = filters.get("since")
        if since is not None:
            items = [b for b in items if b["detected_at"] >= since]
        until = filters.get("until")
        if until is not None:
            items = [b for b in items if b["detected_at"] <= until]
        limit = filters.get("limit", 1000)
        return [_DictObj(b) for b in items[:limit]]

    async def update_break_status(
        self, break_id: uuid.UUID, status: str, age_seconds: int | None = None
    ) -> Any | None:
        for b in self._breaks:
            if b["id"] == break_id:
                b["status"] = status
                if age_seconds is not None:
                    b["age_seconds"] = age_seconds
                if status in ("RESOLVED", "CLOSED") and b["resolved_at"] is None:
                    b["resolved_at"] = datetime.now(tz=UTC)
                return _DictObj(b)
        return None

    async def add_break_resolution(
        self, break_id: uuid.UUID, type: str, actor: str, note: str | None = None
    ) -> Any | None:
        for b in self._breaks:
            if b["id"] == break_id:
                record = {
                    "id": self._id(),
                    "break_id": break_id,
                    "type": type,
                    "actor": actor,
                    "note": note,
                    "created_at": datetime.now(tz=UTC),
                }
                self._resolutions.append(record)
                b["resolutions"].append(record)
                return _DictObj(record)
        return None

    async def open_timing_breaks_for(
        self, source: str, asset: str, reference: str | None
    ) -> Sequence[Any]:
        items = [
            b
            for b in self._breaks
            if b["source"] == source
            and b["asset"] == asset
            and b["classification"] == "TIMING"
            and b["status"] == "OPEN"
            and (reference is None or b["reference"] == reference)
        ]
        return [_DictObj(b) for b in items]

    async def get_rules(self, source: str, asset: str | None = None) -> Sequence[Any]:
        items = [r for r in self._rules if r["source"] == source]
        if asset is not None:
            items = [r for r in items if r["asset"] == asset or r["asset"] is None]
        return [_DictObj(r) for r in items]

    async def list_rules(self, source: str | None = None) -> Sequence[Any]:
        items = self._rules if source is None else [r for r in self._rules if r["source"] == source]
        return [_DictObj(r) for r in items]

    async def upsert_rule(self, **fields: Any) -> Any:
        for r in self._rules:
            if r["source"] == fields["source"] and r.get("asset") == fields.get("asset"):
                r.update(fields)
                return _DictObj(r)
        record = {"id": self._id(), **fields}
        self._rules.append(record)
        return _DictObj(record)

    async def commit(self) -> None:
        return None


class _DictObj:
    """Attribute-access wrapper around a dict so fakes mimic ORM models."""

    def __init__(self, data: dict[str, Any]) -> None:
        object.__setattr__(self, "_data", data)

    def __getattr__(self, name: str) -> Any:
        try:
            return self._data[name]
        except KeyError as e:  # pragma: no cover - defensive
            raise AttributeError(name) from e

    def __getitem__(self, key: str) -> Any:
        return self._data[key]


@pytest_asyncio.fixture
async def sqlite_engine():
    """Create an in-memory SQLite engine with all tables for integration tests."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:", future=True)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def sqlite_session(sqlite_engine) -> AsyncIterator[AsyncSession]:
    factory = async_sessionmaker(sqlite_engine, expire_on_commit=False)
    async with factory() as session:
        yield session


@pytest_asyncio.fixture
async def sqlite_repo(sqlite_session) -> AsyncIterator[Repository]:
    yield SqlRepository(sqlite_session)


@pytest.fixture
def fake_repo() -> FakeRepository:
    return FakeRepository()


@pytest.fixture
def settings_factory():
    """Return a callable that builds a Settings with overridden fields."""
    from reconciliation.config import Settings

    def _factory(**overrides: Any) -> Settings:
        return Settings(**overrides)

    return _factory
