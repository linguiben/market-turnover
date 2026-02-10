from __future__ import annotations

from datetime import date

import sqlalchemy as sa

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models import HsiQuoteFact, TurnoverFact, JobRun
from app.jobs.tasks import run_job

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")

# template helpers
from app.services.formatting import format_hkd_yi, format_hsi_price_x100

templates.env.globals["format_yi"] = format_hkd_yi
templates.env.globals["format_hsi"] = format_hsi_price_x100


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    facts = (
        db.query(TurnoverFact)
        .order_by(TurnoverFact.trade_date.desc(), TurnoverFact.session.asc())
        .limit(10)
        .all()
    )

    keys = {(f.trade_date, f.session) for f in facts}
    quotes = {}
    if keys:
        qs = (
            db.query(HsiQuoteFact)
            .filter(sa.tuple_(HsiQuoteFact.trade_date, HsiQuoteFact.session).in_(list(keys)))
            .all()
        )
        quotes = {(q.trade_date, q.session): q for q in qs}

    return templates.TemplateResponse(
        "dashboard.html",
        {"request": request, "today": date.today().isoformat(), "facts": facts, "quotes": quotes},
    )


@router.get("/recent", response_class=HTMLResponse)
def recent(request: Request, db: Session = Depends(get_db)):
    facts = (
        db.query(TurnoverFact)
        .order_by(TurnoverFact.trade_date.desc(), TurnoverFact.session.asc())
        .limit(100)
        .all()
    )

    keys = {(f.trade_date, f.session) for f in facts}
    quotes = {}
    if keys:
        qs = (
            db.query(HsiQuoteFact)
            .filter(sa.tuple_(HsiQuoteFact.trade_date, HsiQuoteFact.session).in_(list(keys)))
            .all()
        )
        quotes = {(q.trade_date, q.session): q for q in qs}

    return templates.TemplateResponse("recent.html", {"request": request, "facts": facts, "quotes": quotes})


@router.get("/jobs", response_class=HTMLResponse)
def jobs(request: Request, db: Session = Depends(get_db)):
    runs = db.query(JobRun).order_by(JobRun.started_at.desc()).limit(50).all()
    return templates.TemplateResponse("jobs.html", {"request": request, "jobs": runs})


@router.post("/api/jobs/run")
def jobs_run(request: Request, job_name: str = Form(...), db: Session = Depends(get_db)):
    run_job(db, job_name)
    base = (request.scope.get("root_path") or "").rstrip("/")
    return RedirectResponse(url=f"{base}/jobs", status_code=303)
