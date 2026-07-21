"""FastAPI application: REST API + wiring for the Reconciliation service.

The app keeps the existing ``/healthz`` and ``/readyz`` endpoints intact and
adds the documented ``/v1/`` endpoints for breaks and recon runs. The service
dependencies (repository, producer, storage) are attached to ``app.state`` so
tests can inject in-memory fakes.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from .config import get_settings
from .kafka import InMemoryProducer
from .reconciler import Reconciler
from .reports.generator import archive_run_report, export_breaks, generate_run_report
from .schemas import (
    EscalateBreakRequest,
    ReconRuleCreateRequest,
    ReconRunCreateRequest,
    ResolveBreakRequest,
)
from .storage import InMemoryObjectStorage, build_storage
from .tracing import init_tracing, instrument_app

init_tracing()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Readiness checks (preserved from the original scaffolding).
# ---------------------------------------------------------------------------


def ledger_ready() -> bool:
    return True


def db_ready() -> bool:
    return True


def mq_ready() -> bool:
    return True


def source_feeds_ready() -> bool:
    return True


def exchange_feeds_ready() -> bool:
    return True


def blockchain_feeds_ready() -> bool:
    return True


def rail_feeds_ready() -> bool:
    return True


def pricing_ready() -> bool:
    return True


def fx_ready() -> bool:
    return True


def liquidity_ready() -> bool:
    return True


def treasury_ready() -> bool:
    return True


def wallet_ready() -> bool:
    return True


def mpc_ready() -> bool:
    return True


def identity_ready() -> bool:
    return True


def policy_ready() -> bool:
    return True


def audit_ready() -> bool:
    return True


def notification_ready() -> bool:
    return True


def aml_ready() -> bool:
    return True


READINESS_CHECKS = [
    ("ledger", ledger_ready),
    ("db", db_ready),
    ("mq", mq_ready),
    ("source_feeds", source_feeds_ready),
    ("exchange_feeds", exchange_feeds_ready),
    ("blockchain_feeds", blockchain_feeds_ready),
    ("rail_feeds", rail_feeds_ready),
    ("pricing", pricing_ready),
    ("fx", fx_ready),
    ("liquidity", liquidity_ready),
    ("treasury", treasury_ready),
    ("wallet", wallet_ready),
    ("mpc", mpc_ready),
    ("identity", identity_ready),
    ("policy", policy_ready),
    ("audit", audit_ready),
    ("notification", notification_ready),
    ("aml", aml_ready),
]


def readiness_report() -> tuple[dict[str, str], int, int]:
    results: dict[str, str] = {}
    failed = 0
    total = 0
    for name, fn in READINESS_CHECKS:
        total += 1
        if fn():
            results[name] = "ok"
        else:
            results[name] = "down"
            failed += 1
    return results, failed, total


def classify_readiness(failed: int, total: int) -> tuple[int, str]:
    if failed == total and total > 0:
        return 503, "not ready"
    if failed > 0:
        return 200, "degraded"
    return 200, "ready"


# ---------------------------------------------------------------------------
# App factory.
# ---------------------------------------------------------------------------


def create_app(reconciler: Reconciler | None = None) -> FastAPI:
    """Build a FastAPI app, optionally wiring a prebuilt ``Reconciler``.

    Tests pass a reconciler backed by in-memory fakes; production builds one
    from settings on first request.
    """
    app = FastAPI(title="Reconciliation", version="0.1.0")
    instrument_app(app)
    app.state.ledger_consumer = None
    if reconciler is not None:
        app.state.reconciler = reconciler
        app.state.producer = reconciler.producer
        app.state.storage = InMemoryObjectStorage()
    else:
        app.state.reconciler = None
        app.state.producer = InMemoryProducer()
        app.state.storage = build_storage(get_settings())

        @app.on_event("startup")
        async def _init_db() -> None:
            settings = get_settings()
            if settings.db_url and settings.db_url != "sqlite+aiosqlite:///:memory:":
                from .db.session import async_engine_factory, init_db

                await init_db(async_engine_factory(settings.db_url))
            await _start_ledger_consumer(app, settings)

        @app.on_event("shutdown")
        async def _stop_ledger_consumer() -> None:
            consumer = getattr(app.state, "ledger_consumer", None)
            if consumer is not None:
                await consumer.stop()
                app.state.ledger_consumer = None

    _register_routes(app)
    return app


async def _start_ledger_consumer(app: FastAPI, settings: Any) -> None:
    from .kafka_ledger_consumer import build_ledger_consumer
    from .reconciler import Reconciler as _Reconciler

    recon = getattr(app.state, "reconciler", None) or _Reconciler.from_settings(settings)
    app.state.reconciler = recon
    consumer = build_ledger_consumer(recon, settings)
    if consumer is None:
        return
    try:
        await consumer.start()
        app.state.ledger_consumer = consumer
    except Exception as e:  # noqa: BLE001 - don't fail app boot on broker error
        logger.warning("ledger consumer failed to start: %s", e)


def _get_reconciler(request: Request) -> Reconciler:
    reconciler = getattr(request.app.state, "reconciler", None)
    if reconciler is None:
        from .reconciler import Reconciler as _Reconciler

        reconciler = _Reconciler.from_settings(get_settings())
        request.app.state.reconciler = reconciler
    return reconciler


def _register_routes(app: FastAPI) -> None:
    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        results, failed, total = readiness_report()
        code, status = classify_readiness(failed, total)
        results["status"] = status
        results["healthy"] = str(total - failed)
        results["failed"] = str(failed)
        results["total"] = str(total)
        return JSONResponse(status_code=code, content=results)

    @app.get("/v1/breaks")
    async def list_breaks(
        request: Request,
        source: str | None = Query(None),
        status: str | None = Query(None),
        classification: str | None = Query(None),
        asset: str | None = Query(None),
        from_: datetime | None = Query(None, alias="from"),
        to: datetime | None = Query(None),
        limit: int = Query(1000, le=10_000),
    ) -> dict[str, Any]:
        recon = _get_reconciler(request)
        breaks = await recon.list_breaks(
            source=source,
            status=status,
            classification=classification,
            asset=asset,
            since=from_,
            until=to,
            limit=limit,
        )
        serialized = [_serialize_break(b) for b in breaks]
        return {"breaks": serialized, "total": len(serialized)}

    @app.get("/v1/breaks/{break_id}")
    async def get_break(request: Request, break_id: uuid.UUID) -> dict[str, Any]:
        recon = _get_reconciler(request)
        brk = await recon.get_break(break_id)
        if brk is None:
            raise HTTPException(status_code=404, detail="break not found")
        return _serialize_break(brk, include_resolutions=True)

    @app.post("/v1/breaks/{break_id}/resolve")
    async def resolve_break(
        request: Request, break_id: uuid.UUID, body: ResolveBreakRequest
    ) -> dict[str, Any]:
        recon = _get_reconciler(request)
        resolution = await recon.resolve_break(break_id, actor=body.actor, note=body.note)
        if resolution is None:
            raise HTTPException(status_code=404, detail="break not found")
        return {"status": "RESOLVED", "break_id": str(break_id)}

    @app.post("/v1/breaks/{break_id}/escalate")
    async def escalate_break(
        request: Request, break_id: uuid.UUID, body: EscalateBreakRequest
    ) -> dict[str, Any]:
        recon = _get_reconciler(request)
        result = await recon.escalate_break(break_id, actor=body.actor, note=body.note)
        if result is None:
            raise HTTPException(status_code=404, detail="break not found")
        return {"status": "ESCALATED", "break_id": str(break_id)}

    @app.get("/v1/recon-runs")
    async def list_recon_runs(
        request: Request,
        source: str | None = Query(None),
    ) -> dict[str, Any]:
        recon = _get_reconciler(request)
        runs = await recon.list_recon_runs(source=source)
        return {"recon_runs": [_serialize_run(r) for r in runs], "total": len(runs)}

    @app.get("/v1/recon-runs/{run_id}")
    async def get_recon_run(request: Request, run_id: uuid.UUID) -> dict[str, Any]:
        recon = _get_reconciler(request)
        run = await recon.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        return _serialize_run(run)

    @app.post("/v1/recon-runs")
    async def create_recon_run(request: Request, body: ReconRunCreateRequest) -> dict[str, Any]:
        recon = _get_reconciler(request)
        run = await recon.execute(source=body.source, scope=body.scope, mode=body.mode)
        return {"id": run.id, "status": run.status}

    @app.get("/v1/recon-runs/{run_id}/report")
    async def get_run_report(request: Request, run_id: uuid.UUID, fmt: str = Query("csv")) -> Any:
        recon = _get_reconciler(request)
        run = await recon.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        report = await generate_run_report(recon.repo, run)
        if fmt == "json":
            return PlainTextResponse(report.render_json(), media_type="application/json")
        return PlainTextResponse(report.render_csv(), media_type="text/csv")

    @app.post("/v1/recon-runs/{run_id}/report/archive")
    async def archive_run_report_endpoint(request: Request, run_id: uuid.UUID) -> dict[str, Any]:
        recon = _get_reconciler(request)
        run = await recon.get_run(run_id)
        if run is None:
            raise HTTPException(status_code=404, detail="run not found")
        storage = getattr(request.app.state, "storage", None) or build_storage(get_settings())
        bucket = get_settings().reports_bucket or "recon-reports"
        key = await archive_run_report(recon.repo, run, storage, bucket)
        url = await storage.signed_url(bucket, key)
        return {"bucket": bucket, "key": key, "signed_url": url}

    @app.get("/v1/recon-rules")
    async def list_recon_rules(
        request: Request,
        source: str | None = Query(None),
    ) -> dict[str, Any]:
        recon = _get_reconciler(request)
        rules = await recon.list_rules(source=source)
        return {"recon_rules": [_serialize_rule(r) for r in rules], "total": len(rules)}

    @app.post("/v1/recon-rules")
    async def create_recon_rule(request: Request, body: ReconRuleCreateRequest) -> dict[str, Any]:
        recon = _get_reconciler(request)
        rule = await recon.upsert_rule(
            source=body.source,
            asset=body.asset,
            match_strategy=body.match_strategy,
            tolerance_seconds=body.tolerance_seconds,
            escalation_age_minutes=body.escalation_age_minutes,
            auto_resolve_timing=body.auto_resolve_timing,
            config=body.config,
        )
        return _serialize_rule(rule)

    @app.get("/v1/breaks-export")
    async def breaks_export(
        request: Request,
        source: str | None = Query(None),
        status: str | None = Query(None),
        from_: datetime | None = Query(None, alias="from"),
        to: datetime | None = Query(None),
        fmt: str = Query("csv"),
    ) -> Any:
        recon = _get_reconciler(request)
        text = await export_breaks(
            recon.repo,
            source=source,
            status=status,
            since=from_,
            until=to,
            fmt=fmt,
        )
        media = "application/json" if fmt == "json" else "text/csv"
        return PlainTextResponse(text, media_type=media)


def _serialize_break(brk: Any, *, include_resolutions: bool = False) -> dict[str, Any]:
    data: dict[str, Any] = {
        "id": brk.id,
        "run_id": brk.run_id,
        "type": brk.type,
        "classification": brk.classification,
        "source": brk.source,
        "asset": brk.asset,
        "reference": brk.reference,
        "internal_amount": str(brk.internal_amount) if brk.internal_amount is not None else None,
        "external_amount": str(brk.external_amount) if brk.external_amount is not None else None,
        "status": brk.status,
        "detected_at": brk.detected_at.isoformat() if brk.detected_at else None,
        "resolved_at": brk.resolved_at.isoformat() if brk.resolved_at else None,
        "age_seconds": brk.age_seconds,
    }
    if include_resolutions:
        resolutions = []
        for r in getattr(brk, "resolutions", []) or []:
            resolutions.append(
                {
                    "id": r.id,
                    "break_id": r.break_id,
                    "type": r.type,
                    "actor": r.actor,
                    "note": r.note,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                }
            )
        data["resolutions"] = resolutions
    return data


def _serialize_run(run: Any) -> dict[str, Any]:
    return {
        "id": run.id,
        "source": run.source,
        "scope": run.scope,
        "status": run.status,
        "matched_count": run.matched_count,
        "unmatched_count": run.unmatched_count,
        "breaks_count": run.breaks_count,
        "started_at": run.started_at.isoformat() if run.started_at else None,
        "completed_at": run.completed_at.isoformat() if run.completed_at else None,
    }


def _serialize_rule(rule: Any) -> dict[str, Any]:
    return {
        "id": rule.id,
        "source": rule.source,
        "asset": rule.asset,
        "match_strategy": rule.match_strategy,
        "tolerance_seconds": rule.tolerance_seconds,
        "escalation_age_minutes": rule.escalation_age_minutes,
        "auto_resolve_timing": rule.auto_resolve_timing,
        "config": rule.config,
        "created_at": rule.created_at.isoformat() if getattr(rule, "created_at", None) else None,
        "updated_at": rule.updated_at.isoformat() if getattr(rule, "updated_at", None) else None,
    }


# Backwards-compatible module-level app instance for ``uvicorn reconciliation.app:app``.
app = create_app()
