"""FastAPI entry point exposing the reconciliation REST API.

Stage 1: minimal scaffolding wiring the settings loader and the health
endpoint from app.py. Subsequent stages mount the v1 routers here.
"""

from __future__ import annotations

import logging

from fastapi import FastAPI

from reconciliation import app as _app_module  # noqa: F401  (re-export healthz)
from reconciliation.app import app as app
from reconciliation.config import get_settings


def _configure_logging() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.LOG_LEVEL.upper())


def create_app() -> FastAPI:
    """Application factory used by `reconciliation.server`."""
    _configure_logging()
    # Stage 1 keeps the existing app (with /healthz). Stage 2+ mounts /v1.
    return app


if __name__ == "__main__":
    import uvicorn

    settings = get_settings()
    uvicorn.run(
        "reconciliation.server:app",
        host="0.0.0.0",
        port=settings.PORT,
        log_level=settings.LOG_LEVEL,
    )