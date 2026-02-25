from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path

# Debug aid: allow dumping stack traces via `kill -USR1 <pid>`.
# Safe in production (only triggers when signaled).
import faulthandler
import signal

faulthandler.register(signal.SIGUSR1)

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.services.job_scheduler import start_scheduler, stop_scheduler
from app.web.routes import router as web_router
from app.web.visit_logs import add_visit_logging

base_path = settings.BASE_PATH.rstrip("/")
logger = logging.getLogger(__name__)
favicon_path = Path("app/web/static/favicon.ico")

@asynccontextmanager
async def lifespan(_app: FastAPI):
    if settings.ENABLE_SCHEDULED_JOBS:
        start_scheduler()
    else:
        logger.info("Scheduled jobs disabled by ENABLE_SCHEDULED_JOBS.")

    try:
        yield
    finally:
        stop_scheduler()


app = FastAPI(title=settings.APP_NAME, lifespan=lifespan)
add_visit_logging(app)

# Serve UI/API at root (/) always.
app.include_router(web_router, prefix="")

# Also serve under a reverse-proxy prefix like /market-turnover (backward compatible).
if base_path:
    app.include_router(web_router, prefix=base_path)

# Static test pages (deployed with the container)
# Example: /market-turnover/test/t1.html
app.mount(f"{base_path}/test", StaticFiles(directory="test"), name="test")


# NOTE: we intentionally use a single scheduler (lifespan + ENABLE_SCHEDULED_JOBS)
# to avoid double-triggering jobs.


@app.get("/healthz")
def healthz():
    return {"ok": True, "app": settings.APP_NAME}


@app.get(f"{base_path}/healthz")
def healthz_prefixed():
    return {"ok": True, "app": settings.APP_NAME, "base_path": base_path}


@app.get("/favicon.ico", include_in_schema=False)
def favicon_root():
    return FileResponse(favicon_path)


@app.get(f"{base_path}/favicon.ico", include_in_schema=False)
def favicon_prefixed():
    return FileResponse(favicon_path)
