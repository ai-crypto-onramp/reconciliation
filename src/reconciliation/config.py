"""Application configuration loaded from environment variables."""

from __future__ import annotations

from typing import Any

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime settings for the Reconciliation service.

    All values are read from environment variables; see README for the full list.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    port: int = 8080
    db_url: str = "sqlite+aiosqlite:///:memory:"
    kafka_brokers: str = ""
    reports_bucket: str = ""
    break_tolerance_seconds: int = 300
    auto_resolve_timing_breaks: bool = True
    escalation_webhook: str = ""
    escalation_age_minutes: int = 60
    eod_run_cron: str = "0 23 * * *"
    consumer_concurrency: int = 4
    log_level: str = "info"

    # Test-friendly knobs.
    enable_kafka: bool = False

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        """Build settings from a (possibly partial) environment mapping."""
        if env is None:
            return cls()
        coerced: dict[str, Any] = {}
        for key, value in env.items():
            coerced[key.lower()] = value
        return cls(**coerced)


# Canonical source identifiers reused across the service.
SOURCES: tuple[str, ...] = ("ledger", "rails", "exchanges", "onchain", "custody")

BREAK_TYPES: tuple[str, ...] = ("amount_mismatch", "timing_gap", "missing_entry", "duplicate")
BREAK_CLASSIFICATIONS: tuple[str, ...] = ("timing", "real")
BREAK_STATUSES: tuple[str, ...] = ("open", "resolved", "escalated", "closed")
RESOLUTION_TYPES: tuple[str, ...] = ("manual", "auto")
MATCH_STRATEGIES: tuple[str, ...] = ("exact", "fuzzy", "balance_rollforward")
RUN_STATUSES: tuple[str, ...] = ("running", "completed", "failed")

# Topics consumed by this service from upstream producers.
CONSUMER_TOPICS: dict[str, str] = {
    "ledger": "ledger-accounting",
    "rails": "rail-connectors",
    "exchanges": "exchange-connectors",
    "onchain": "blockchain-gateway",
    "custody": "blockchain-gateway",
}

# Topics emitted by this service.
ALERT_TOPIC = "break-alert"
AUDIT_TOPIC = "break-event"


_settings: Settings | None = None


def get_settings() -> Settings:
    """Return a process-wide cached Settings instance."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Clear the cached settings (used in tests)."""
    global _settings
    _settings = None
