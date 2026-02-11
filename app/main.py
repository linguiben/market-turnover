from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.db.session import SessionLocal
from app.jobs.tasks import run_job
from app.web.routes import router as web_router
from app.web.visit_logs import add_visit_logging

base_path = settings.BASE_PATH.rstrip("/")
logger = logging.getLogger(__name__)
favicon_path = Path("app/web/static/favicon.ico")

_scheduler: BackgroundScheduler | None = None


def _run_job_with_new_session(job_name: str) -> None:
    db = SessionLocal()
    try:
        run = run_job(db, job_name)
        logger.info("Scheduled job finished: job=%s status=%s id=%s", job_name, run.status, run.id)
    except Exception:
        logger.exception("Scheduled job failed unexpectedly: job=%s", job_name)
    finally:
        db.close()


def _build_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=ZoneInfo(settings.TZ))

    # Intraday snapshot refresh during trading hours (Mon-Fri, every 5 minutes).
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="*/5"),
        kwargs={"job_name": "fetch_intraday_snapshot"},
        id="fetch_intraday_snapshot_interval",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    # Midday and full-day snapshots for turnover cards.
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour=11, minute=35),
        kwargs={"job_name": "fetch_am"},
        id="fetch_am_cron",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour=16, minute=10),
        kwargs={"job_name": "fetch_full"},
        id="fetch_full_cron",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )
    return scheduler


@asynccontextmanager
async def lifespan(_app: FastAPI):
    global _scheduler
    if settings.ENABLE_SCHEDULED_JOBS:
        _scheduler = _build_scheduler()
        _scheduler.start()
        logger.info("Scheduled jobs enabled. timezone=%s", settings.TZ)
    else:
        logger.info("Scheduled jobs disabled by ENABLE_SCHEDULED_JOBS.")

    try:
        yield
    finally:
        if _scheduler is not None:
            _scheduler.shutdown(wait=False)
            _scheduler = None


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


def _run_scheduled_snapshot() -> None:
    db = SessionLocal()
    try:
        run_job(db, "fetch_intraday_snapshot")
    finally:
        db.close()


@app.on_event("startup")
def _startup_scheduler() -> None:
    if not settings.SNAPSHOT_SCHEDULE_ENABLED:
        return

    scheduler = BackgroundScheduler(timezone=settings.TZ)
    scheduler.add_job(
        _run_scheduled_snapshot,
        trigger="interval",
        seconds=int(settings.SNAPSHOT_INTERVAL_SECONDS),
        id="fetch_intraday_snapshot_interval",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    scheduler.start()
    app.state.scheduler = scheduler


@app.on_event("shutdown")
def _shutdown_scheduler() -> None:
    scheduler = getattr(app.state, "scheduler", None)
    if scheduler is not None:
        scheduler.shutdown(wait=False)


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
