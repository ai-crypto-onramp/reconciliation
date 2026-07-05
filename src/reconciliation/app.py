from fastapi import FastAPI

app = FastAPI(title="Reconciliation")


@app.get("/healthz")
async def healthz():
    return {"status": "ok"}