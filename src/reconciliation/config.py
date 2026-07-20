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
SOURCES: tuple[str, ...] = ("LEDGER", "RAILS", "EXCHANGES", "ONCHAIN", "CUSTODY")

BREAK_TYPES: tuple[str, ...] = ("AMOUNT_MISMATCH", "TIMING_GAP", "MISSING_ENTRY", "DUPLICATE")
BREAK_CLASSIFICATIONS: tuple[str, ...] = ("TIMING", "REAL")
BREAK_STATUSES: tuple[str, ...] = ("OPEN", "RESOLVED", "ESCALATED", "CLOSED")
RESOLUTION_TYPES: tuple[str, ...] = ("MANUAL", "AUTO")
MATCH_STRATEGIES: tuple[str, ...] = ("EXACT", "FUZZY", "BALANCE_ROLLFORWARD")
RUN_STATUSES: tuple[str, ...] = ("RUNNING", "COMPLETED", "FAILED")

# Topics consumed by this service from upstream producers.
CONSUMER_TOPICS: dict[str, str] = {
    "LEDGER": "ledger-accounting",
    "RAILS": "rail-connectors",
    "EXCHANGES": "exchange-connectors",
    "ONCHAIN": "blockchain-gateway",
    "CUSTODY": "blockchain-gateway",
}

# Topics emitted by this service.
ALERT_TOPIC = "break-alert"
AUDIT_TOPIC = "audit.v1"


def audit_envelope(payload: dict[str, Any], target_id: Any) -> dict[str, Any]:
    """Build the canonical audit.v1 envelope (see
    .github/contracts/asyncapi/audit/v1/asyncapi.yaml) around an arbitrary payload.
    """
    import hashlib
    import json
    import uuid

    payload_bytes = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    payload_hash = "sha256:" + hashlib.sha256(payload_bytes).hexdigest()
    ts_val: Any = payload.get("timestamp") or payload.get("detected_at") or ""
    if hasattr(ts_val, "isoformat"):
        ts_val = ts_val.isoformat()
    return {
        "schema_version": "1",
        "id": str(uuid.uuid4()),
        "ts": str(ts_val),
        "source_service": "reconciliation",
        "actor_id": payload.get("actor", "reconciliation"),
        "action": "recon." + str(payload.get("action", "event")),
        "target_type": "break",
        "target_id": str(target_id),
        "payload_hash": payload_hash,
        "payload": payload,
    }


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
