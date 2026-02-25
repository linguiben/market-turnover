from __future__ import annotations

import logging
import threading
from zoneinfo import ZoneInfo

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import JobDefinition, JobSchedule
from app.db.session import SessionLocal
from app.jobs.tasks import run_job

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None
_lock = threading.Lock()


def _run_job_with_new_session(job_name: str) -> None:
    db = SessionLocal()
    try:
        run = run_job(db, job_name)
        logger.info("Scheduled job finished: job=%s status=%s id=%s", job_name, run.status, run.id)
    except Exception:
        logger.exception("Scheduled job failed unexpectedly: job=%s", job_name)
    finally:
        db.close()


def _add_job_to_scheduler(scheduler: BackgroundScheduler, definition: JobDefinition, schedule: JobSchedule) -> None:
    if (schedule.trigger_type or "cron").lower() != "cron":
        logger.warning(
            "Skip unsupported schedule trigger. job=%s schedule=%s trigger_type=%s",
            definition.job_name,
            schedule.schedule_code,
            schedule.trigger_type,
        )
        return

    trigger = CronTrigger(
        timezone=ZoneInfo(schedule.timezone or settings.TZ),
        second=schedule.second or "0",
        minute=schedule.minute or "*",
        hour=schedule.hour or "*",
        day=schedule.day or "*",
        month=schedule.month or "*",
        day_of_week=schedule.day_of_week or "*",
        start_date=schedule.start_date,
        end_date=schedule.end_date,
    )

    scheduler.add_job(
        _run_job_with_new_session,
        trigger,
        kwargs={"job_name": definition.handler_name},
        id=f"job:{definition.job_name}:{schedule.schedule_code}",
        replace_existing=True,
        coalesce=bool(schedule.coalesce),
        max_instances=max(1, int(schedule.max_instances or 1)),
        misfire_grace_time=max(1, int(schedule.misfire_grace_time or 120)),
        jitter=schedule.jitter_seconds,
    )


def build_scheduler_from_db(db: Session) -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=ZoneInfo(settings.TZ))

    definitions = (
        db.query(JobDefinition)
        .filter(JobDefinition.is_active.is_(True))
        .filter(JobDefinition.schedule_enabled.is_(True))
        .order_by(JobDefinition.ui_order.asc(), JobDefinition.job_name.asc())
        .all()
    )
    if not definitions:
        return scheduler

    job_names = [row.job_name for row in definitions]
    schedules = (
        db.query(JobSchedule)
        .filter(JobSchedule.is_active.is_(True))
        .filter(JobSchedule.job_name.in_(job_names))
        .order_by(JobSchedule.job_name.asc(), JobSchedule.schedule_code.asc())
        .all()
    )

    by_job_name: dict[str, list[JobSchedule]] = {}
    for row in schedules:
        by_job_name.setdefault(row.job_name, []).append(row)

    for definition in definitions:
        for schedule in by_job_name.get(definition.job_name, []):
            _add_job_to_scheduler(scheduler, definition, schedule)

    return scheduler


def start_scheduler() -> None:
    global _scheduler
    with _lock:
        if _scheduler is not None:
            return

        db = SessionLocal()
        try:
            _scheduler = build_scheduler_from_db(db)
            _scheduler.start()
            logger.info("Scheduled jobs enabled from DB. timezone=%s", settings.TZ)
        finally:
            db.close()


def stop_scheduler() -> None:
    global _scheduler
    with _lock:
        if _scheduler is None:
            return
        _scheduler.shutdown(wait=False)
        _scheduler = None


def reload_scheduler() -> None:
    if not settings.ENABLE_SCHEDULED_JOBS:
        return

    with _lock:
        global _scheduler

        db = SessionLocal()
        try:
            new_scheduler = build_scheduler_from_db(db)
            new_scheduler.start()
        finally:
            db.close()

        old_scheduler = _scheduler
        _scheduler = new_scheduler
        if old_scheduler is not None:
            old_scheduler.shutdown(wait=False)

        logger.info("Scheduled jobs reloaded from DB.")
