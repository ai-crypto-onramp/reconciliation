"""Tests for the environment-driven settings loader."""

from __future__ import annotations

import importlib

import pytest

from reconciliation import config as config_module
from reconciliation.config import Settings, get_settings


@pytest.fixture(autouse=True)
def _reset_settings_cache(monkeypatch):
    """Ensure each test rebuilds Settings from a clean env."""
    get_settings.cache_clear()
    monkeypatch.delenv("DB_URL", raising=False)
    monkeypatch.delenv("KAFKA_BROKERS", raising=False)
    monkeypatch.delenv("PORT", raising=False)
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    yield
    get_settings.cache_clear()


def test_defaults_match_readme_table():
    s = Settings()
    assert s.PORT == 8080
    assert s.BREAK_TOLERANCE_SECONDS == 300
    assert s.AUTO_RESOLVE_TIMING_BREAKS is True
    assert s.ESCALATION_AGE_MINUTES == 60
    assert s.EOD_RUN_CRON == "0 23 * * *"
    assert s.CONSUMER_CONCURRENCY == 4
    assert s.LOG_LEVEL == "info"
    assert s.DB_URL is None
    assert s.KAFKA_BROKERS == ""
    assert s.kafka_broker_list == []


def test_env_var_overrides(monkeypatch):
    monkeypatch.setenv("PORT", "9090")
    monkeypatch.setenv("DB_URL", "postgresql://user:pass@localhost:5432/recon")
    monkeypatch.setenv("KAFKA_BROKERS", "broker1:9092, broker2:9092 ,")
    monkeypatch.setenv("BREAK_TOLERANCE_SECONDS", "120")
    monkeypatch.setenv("AUTO_RESOLVE_TIMING_BREAKS", "false")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    s = Settings()
    assert s.PORT == 9090
    assert s.DB_URL == "postgresql://user:pass@localhost:5432/recon"
    assert s.KAFKA_BROKERS.startswith("broker1")
    assert s.kafka_broker_list == ["broker1:9092", "broker2:9092"]
    assert s.BREAK_TOLERANCE_SECONDS == 120
    assert s.AUTO_RESOLVE_TIMING_BREAKS is False
    assert s.LOG_LEVEL == "debug"  # normalised to lower


def test_db_url_coercion_to_async_and_sync():
    s = Settings(DB_URL="postgresql://u:p@localhost:5432/recon")
    assert s.db_url_async == "postgresql+asyncpg://u:p@localhost:5432/recon"
    assert s.db_url_sync == "postgresql+psycopg2://u:p@localhost:5432/recon"


def test_db_url_coercion_replaces_existing_driver():
    s = Settings(DB_URL="postgresql+asyncpg://u:p@localhost:5432/recon")
    assert s.db_url_sync == "postgresql+psycopg2://u:p@localhost:5432/recon"
    assert s.db_url_async == "postgresql+asyncpg://u:p@localhost:5432/recon"


def test_db_url_none_returns_none():
    s = Settings(DB_URL=None)
    assert s.db_url_async is None
    assert s.db_url_sync is None


def test_get_settings_is_cached(monkeypatch):
    monkeypatch.setenv("PORT", "1111")
    first = get_settings()
    second = get_settings()
    assert first is second
    assert first.PORT == 1111


def test_module_reload_picks_up_env(monkeypatch):
    monkeypatch.setenv("PORT", "4242")
    importlib.reload(config_module)
    assert config_module.get_settings().PORT == 4242