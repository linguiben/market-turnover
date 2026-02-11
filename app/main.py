from __future__ import annotations

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from apscheduler.schedulers.background import BackgroundScheduler

from app.config import settings
from app.db.session import SessionLocal
from app.jobs.tasks import run_job
from app.web.routes import router as web_router

base_path = settings.BASE_PATH.rstrip("/")
app = FastAPI(title=settings.APP_NAME)

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
