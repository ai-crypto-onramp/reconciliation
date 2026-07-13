from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI(title="Reconciliation")


def ledger_ready() -> bool: return True
def db_ready() -> bool: return True
def mq_ready() -> bool: return True
def source_feeds_ready() -> bool: return True
def exchange_feeds_ready() -> bool: return True
def blockchain_feeds_ready() -> bool: return True
def rail_feeds_ready() -> bool: return True
def pricing_ready() -> bool: return True
def fx_ready() -> bool: return True
def liquidity_ready() -> bool: return True
def treasury_ready() -> bool: return True
def wallet_ready() -> bool: return True
def mpc_ready() -> bool: return True
def identity_ready() -> bool: return True
def policy_ready() -> bool: return True
def audit_ready() -> bool: return True
def notification_ready() -> bool: return True
def aml_ready() -> bool: return True

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


@app.get("/healthz")
async def healthz():
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