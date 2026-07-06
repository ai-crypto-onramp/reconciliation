"""Application configuration loaded from environment variables.

All settings have documented defaults; override by setting the matching
environment variable (see README.md for the full variable table).
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service-wide settings sourced from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore",
    )

    # --- HTTP / runtime -------------------------------------------------
    PORT: int = Field(default=8080, description="HTTP port for the REST API.")
    LOG_LEVEL: str = Field(
        default="info",
        description="Application log level (debug/info/warning/error).",
    )

    # --- Datastores / messaging -----------------------------------------
    DB_URL: str | None = Field(
        default=None,
        description="PostgreSQL connection string (asyncpg/psycopg2).",
    )
    KAFKA_BROKERS: str = Field(
        default="",
        description="Comma-separated Kafka bootstrap brokers.",
    )

    # --- Reconciliation tuning ------------------------------------------
    BREAK_TOLERANCE_SECONDS: int = Field(
        default=300,
        description="Tolerance window for classifying a break as timing vs. real.",
    )
    AUTO_RESOLVE_TIMING_BREAKS: bool = Field(
        default=True,
        description="Auto-resolve timing breaks when the delayed confirmation arrives.",
    )
    ESCALATION_WEBHOOK: str | None = Field(
        default=None,
        description="Webhook URL invoked when a break is escalated or ages out.",
    )
    ESCALATION_AGE_MINUTES: int = Field(
        default=60,
        description="Age (minutes) after which an unresolved break is auto-escalated.",
    )
    EOD_RUN_CRON: str = Field(
        default="0 23 * * *",
        description="Cron schedule for the daily end-of-day recon run.",
    )
    CONSUMER_CONCURRENCY: int = Field(
        default=4,
        description="Number of concurrent Kafka consumer workers per source.",
    )

    # --- Reports --------------------------------------------------------
    REPORTS_BUCKET: str | None = Field(
        default=None,
        description="Object storage bucket for EOD recon report archives.",
    )

    # --- Helpers --------------------------------------------------------
    @field_validator("LOG_LEVEL")
    @classmethod
    def _normalise_log_level(cls, value: str) -> str:
        return value.lower()

    @property
    def kafka_broker_list(self) -> list[str]:
        """Kafka brokers parsed from the comma-separated KAFKA_BROKERS env var."""
        if not self.KAFKA_BROKERS:
            return []
        return [b.strip() for b in self.KAFKA_BROKERS.split(",") if b.strip()]

    @property
    def db_url_sync(self) -> str | None:
        """DB_URL coerced to a sync psycopg2 driver for Alembic migrations.

        Returns None when DB_URL is unset. Replaces ``postgresql+asyncpg://``/
        ``postgresql://`` schemes with the psycopg2 sync driver so Alembic can
        use a single connection URL.
        """
        return _coerce_db_url(self.DB_URL, driver="psycopg2")

    @property
    def db_url_async(self) -> str | None:
        """DB_URL coerced to the asyncpg async driver for SQLAlchemy runtime use."""
        return _coerce_db_url(self.DB_URL, driver="asyncpg")


def _coerce_db_url(url: str | None, *, driver: Literal["asyncpg", "psycopg2"]) -> str | None:
    if not url:
        return None
    if "+" in url and "://" in url.split("+", 1)[1]:
        # already has an explicit driver: replace it
        scheme, rest = url.split("://", 1)
        head = scheme.split("+", 1)[0]
        return f"{head}+{driver}://{rest}"
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", f"postgresql+{driver}://", 1)
    return url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached Settings instance built from the environment."""
    return Settings()