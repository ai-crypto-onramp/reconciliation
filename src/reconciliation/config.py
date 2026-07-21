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

    # External service URLs.
    ledger_url: str = ""

    # Recon scope: comma-separated account ids to fetch from the ledger
    # service. Empty means "all accounts".
    recon_accounts: str = ""

    # Kafka topic names consumed by this service. Defaults match the
    # canonical contracts in .github/contracts/asyncapi/.
    recon_ledger_topic: str = "ledger.events.v1"
    recon_rails_topic: str = "rail.events.v1"
    recon_blockchain_topic: str = "blockchain.events.v1"
    recon_liquidity_topic: str = "liquidity.fills"
    recon_fraud_topic: str = "fraud.scored"
    recon_payment_topic: str = "payment.events.v1"
    recon_exchange_topic: str = "exchange.events.v1"
    recon_custody_topic: str = "custody.events.v1"

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

# Topics consumed by this service from upstream producers. Defaults are
# derived from the canonical AsyncAPI contracts in
# .github/contracts/asyncapi/<service>/v1/asyncapi.yaml; override via the
# matching ``recon_*_topic`` env vars (see :class:`Settings).
def _consumer_topics_from_settings(settings: Settings | None = None) -> dict[str, str]:
    s = settings or get_settings()
    return {
        "LEDGER": s.recon_ledger_topic,
        "RAILS": s.recon_rails_topic,
        "EXCHANGES": s.recon_exchange_topic,
        "ONCHAIN": s.recon_blockchain_topic,
        "CUSTODY": s.recon_custody_topic,
        "LIQUIDITY": s.recon_liquidity_topic,
        "FRAUD": s.recon_fraud_topic,
        "PAYMENT": s.recon_payment_topic,
    }


# Backwards-compatible static map; tests import this directly. The values
# are the canonical defaults — runtime code should prefer
# :func:`_consumer_topics_from_settings` so env overrides take effect.
CONSUMER_TOPICS: dict[str, str] = {
    "LEDGER": "ledger.events.v1",
    "RAILS": "rail.events.v1",
    "EXCHANGES": "exchange.events.v1",
    "ONCHAIN": "blockchain.events.v1",
    "CUSTODY": "custody.events.v1",
    "LIQUIDITY": "liquidity.fills",
    "FRAUD": "fraud.scored",
    "PAYMENT": "payment.events.v1",
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
