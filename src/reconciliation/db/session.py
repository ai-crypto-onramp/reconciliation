"""Async engine/session factory helpers.

Tests run against an in-memory SQLite database; production uses PostgreSQL.
The ORM models use ``postgresql.JSONB`` which SQLite cannot understand, so we
register a dialect-aware type that falls back to ``JSON`` on SQLite.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from sqlalchemy import JSON, BigInteger, Integer, TypeDecorator, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from ..config import get_settings
from .models import Base


class JSONBType(TypeDecorator):
    """A JSONB type that degrades to JSON on non-PostgreSQL dialects."""

    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:  # noqa: ANN401
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB())
        return dialect.type_descriptor(JSON())


class BigIntType(TypeDecorator):
    """A BigInteger that degrades to Integer on SQLite for autoincrement support."""

    impl = BigInteger
    cache_ok = True

    def load_dialect_impl(self, dialect: Any) -> Any:  # noqa: ANN401
        if dialect.name == "sqlite":
            return dialect.type_descriptor(Integer())
        return dialect.type_descriptor(BigInteger())


def _patch_jsonb_columns() -> None:
    """Replace ``JSONB`` columns with the dialect-aware ``JSONBType``.

    Done once at import time so SQLite-based tests can persist payloads. Also
    rewrites Postgres-specific ``server_default`` literals (``'{}'::jsonb``
    and ``now()``) to SQLite-compatible equivalents.
    """
    for table in Base.metadata.tables.values():
        for column in table.columns:
            if isinstance(column.type, JSONB):
                column.type = JSONBType()
            if isinstance(column.type, BigInteger):
                column.type = BigIntType()
            if column.server_default is not None:
                arg = getattr(column.server_default, "arg", None)
                if arg is None:
                    continue
                raw = getattr(arg, "text", arg)
                if not isinstance(raw, str):
                    continue
                new_raw = raw
                if "::jsonb" in new_raw:
                    new_raw = new_raw.replace("::jsonb", "")
                if new_raw == "now()":
                    new_raw = "CURRENT_TIMESTAMP"
                if new_raw != raw:
                    column.server_default.arg = text(new_raw)


_patch_jsonb_columns()


def async_engine_factory(url: str | None = None, **kwargs: Any) -> AsyncEngine:
    """Create an async engine bound to ``url`` (or the configured DB_URL)."""
    db_url = url or get_settings().db_url
    return create_async_engine(db_url, future=True, **kwargs)


def async_session_factory(engine: AsyncEngine | None = None) -> async_sessionmaker[AsyncSession]:
    """Return an ``async_sessionmaker`` bound to ``engine``."""
    return async_sessionmaker(engine or async_engine_factory(), expire_on_commit=False)


async def init_db(engine: AsyncEngine | None = None, *, create_all: bool = True) -> AsyncEngine:
    """Create all tables on ``engine`` (used for tests and first boot)."""
    eng = engine or async_engine_factory()
    if create_all:
        async with eng.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    return eng


async def session_scope(engine: AsyncEngine | None = None) -> AsyncIterator[AsyncSession]:
    """Yield a single async session, closing it on exit (test convenience)."""
    factory = async_session_factory(engine)
    async with factory() as session:
        yield session
