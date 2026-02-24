from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from zoneinfo import ZoneInfo

# Debug aid: allow dumping stack traces via `kill -USR1 <pid>`.
# Safe in production (only triggers when signaled).
import faulthandler
import signal

faulthandler.register(signal.SIGUSR1)

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

    # Eastmoney realtime snapshot (stock/get, 11 indices), every 2 minutes in trading hours.
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="*/2"),
        kwargs={"job_name": "fetch_eastmoney_realtime_snapshot"},
        id="fetch_eastmoney_realtime_snapshot_interval",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour=17, minute=0),
        kwargs={"job_name": "fetch_eastmoney_realtime_snapshot"},
        id="fetch_eastmoney_realtime_snapshot_1700",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )

    # Existing intraday snapshot refresh during trading hours (Mon-Fri, every 2 minutes, 09:00-17:00).
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour="9-16", minute="*/2"),
        kwargs={"job_name": "fetch_intraday_snapshot"},
        id="fetch_intraday_snapshot_interval",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )
    # Include 17:00 (avoid scheduling 17:05/17:10/...)
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(day_of_week="mon-fri", hour=17, minute=0),
        kwargs={"job_name": "fetch_intraday_snapshot"},
        id="fetch_intraday_snapshot_1700",
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

    # Daily Tushare index sync at 20:00.
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(hour=20, minute=0),
        kwargs={"job_name": "fetch_tushare_index"},
        id="fetch_tushare_index_cron",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=600,
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

    # Homepage widgets refresh (every 5 minutes).
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(minute="*/5"),
        kwargs={"job_name": "refresh_home_global_quotes"},
        id="refresh_home_global_quotes_cron",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
    )
    scheduler.add_job(
        _run_job_with_new_session,
        CronTrigger(minute="*/5"),
        kwargs={"job_name": "refresh_home_trade_corridor"},
        id="refresh_home_trade_corridor_cron",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
        misfire_grace_time=120,
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
