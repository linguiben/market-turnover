from __future__ import annotations

from fastapi import FastAPI

from app.config import settings
from app.web.routes import router as web_router

base_path = settings.BASE_PATH.rstrip("/")
app = FastAPI(title=settings.APP_NAME)

# Serve UI/API under a reverse-proxy prefix like /market-turnover
app.include_router(web_router, prefix=base_path)


@app.get("/healthz")
def healthz():
    return {"ok": True, "app": settings.APP_NAME}


@app.get(f"{base_path}/healthz")
def healthz_prefixed():
    return {"ok": True, "app": settings.APP_NAME, "base_path": base_path}
