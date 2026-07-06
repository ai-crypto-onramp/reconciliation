"""Engine and session helpers.

Keeps the runtime (async) path separate from the migration (sync) path so
that Alembic — which is sync — can use the same DB_URL setting via a coerced
psycopg2 driver while the application uses asyncpg.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker, create_async_engine

from reconciliation.config import get_settings


def get_sync_engine():
    """Return a sync SQLAlchemy engine bound to DB_URL (psycopg2 driver)."""
    settings = get_settings()
    url = settings.db_url_sync
    if not url:
        raise RuntimeError("DB_URL is not configured")
    return create_engine(url, future=True)


def create_async_engine_from_settings() -> AsyncEngine:
    """Return an async engine bound to DB_URL (asyncpg driver)."""
    settings = get_settings()
    url = settings.db_url_async
    if not url:
        raise RuntimeError("DB_URL is not configured")
    return create_async_engine(url, future=True)


# Public alias matching the export in __init__.
create_async_engine = create_async_engine_from_settings


async def get_async_session() -> AsyncIterator[AsyncSession]:
    """FastAPI dependency yielding an async session bound to the app engine."""
    engine = create_async_engine_from_settings()
    factory: async_sessionmaker[AsyncSession] = async_sessionmaker(
        engine, expire_on_commit=False, class_=AsyncSession
    )
    async with factory() as session:
        yield session