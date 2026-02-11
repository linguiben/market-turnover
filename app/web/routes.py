from __future__ import annotations

from datetime import date
from datetime import datetime, time
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models import (
    HsiQuoteFact,
    IndexQuoteHistory,
    IndexRealtimeSnapshot,
    JobRun,
    MarketIndex,
    SessionType,
    TurnoverFact,
)
from app.jobs.tasks import run_job

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")

# template helpers
from app.services.formatting import format_amount_b, format_hsi_price_x100

templates.env.globals["format_yi"] = format_amount_b
templates.env.globals["format_hsi"] = format_hsi_price_x100

INDEX_CODES = ("HSI", "SSE", "SZSE")
INDEX_FALLBACK_NAMES = {"HSI": "恒生指数", "SSE": "上证指数", "SZSE": "深证成指"}
AVAILABLE_JOBS: tuple[dict, ...] = (
    {
        "name": "fetch_am",
        "label": "午盘抓取",
        "description": "抓取午盘成交额和 HSI 快照；同时尝试同步最新 Tushare 指数。",
        "targets": ["turnover_source_record", "turnover_fact", "hsi_quote_fact"],
    },
    {
        "name": "fetch_full",
        "label": "全日抓取",
        "description": "抓取全日成交额和 HSI 快照；同时尝试同步最新 Tushare 指数。",
        "targets": ["turnover_source_record", "turnover_fact", "hsi_quote_fact"],
    },
    {
        "name": "fetch_tushare_index",
        "label": "同步最新指数",
        "description": "同步 HSI/SSE/SZSE 的最新一个交易日(日线)数据。",
        "targets": ["index_quote_source_record", "index_quote_history", "index_realtime_snapshot"],
    },
    {
        "name": "fetch_intraday_snapshot",
        "label": "抓取盘中快照",
        "description": "抓取今日盘中快照：HSI(AASTOCKS), SSE/SZSE(EASTMONEY 1min)。",
        "targets": ["index_realtime_snapshot"],
        "params": [
            {"name": "codes", "label": "Index codes (comma)", "type": "text", "placeholder": "HSI,SSE,SZSE"},
            {"name": "force_source", "label": "Force source (optional)", "type": "text", "placeholder": "AASTOCKS/EASTMONEY/TUSHARE"},
        ],
    },
    {
        "name": "fetch_intraday_bars_cn_5m",
        "label": "保存A股 5分钟K线",
        "description": "保存 SSE/SZSE 的 5分钟K线原始bar到 index_intraday_bar（EASTMONEY, lookback=7天）。",
        "targets": ["index_intraday_bar"],
        "params": [
            {"name": "lookback_days", "label": "Lookback days", "type": "number", "placeholder": "7"},
        ],
    },
    {
        "name": "backfill_tushare_index",
        "label": "回填指数1年",
        "description": "回填最近 1 年 HSI/SSE/SZSE 日线数据（跳过已存在记录）。",
        "targets": ["index_quote_source_record", "index_quote_history"],
    },
    {
        "name": "backfill_cn_halfday",
        "label": "回填A股半日成交(90天)",
        "description": "用 Eastmoney 分钟线回填 SSE/SZSE 的半日成交额与全日成交额（用于柱状图和均值）。",
        "targets": ["index_quote_history", "index_quote_source_record"],
    },
    {
        "name": "backfill_hkex",
        "label": "回填HKEX历史",
        "description": "从 HKEX 统计页面回填港股成交额历史（FULL）。",
        "targets": ["turnover_source_record", "turnover_fact"],
    },
)


def _latest_index_history(db: Session, *, index_id: int, session: SessionType) -> IndexQuoteHistory | None:
    return (
        db.query(IndexQuoteHistory)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == session)
        .order_by(IndexQuoteHistory.trade_date.desc())
        .first()
    )


def _latest_index_history_before(
    db: Session,
    *,
    index_id: int,
    session: SessionType,
    before_date: date,
) -> IndexQuoteHistory | None:
    return (
        db.query(IndexQuoteHistory)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == session)
        .filter(IndexQuoteHistory.trade_date < before_date)
        .order_by(IndexQuoteHistory.trade_date.desc())
        .first()
    )


def _today_realtime_snapshot(
    db: Session,
    *,
    index_id: int,
    today: date,
    session: SessionType | None = None,
    updated_before: datetime | None = None,
) -> IndexRealtimeSnapshot | None:
    q = (
        db.query(IndexRealtimeSnapshot)
        .filter(IndexRealtimeSnapshot.index_id == index_id)
        .filter(IndexRealtimeSnapshot.trade_date == today)
    )
    if session is not None:
        q = q.filter(IndexRealtimeSnapshot.session == session)
    if updated_before is not None:
        q = q.filter(IndexRealtimeSnapshot.data_updated_at <= updated_before)
    return q.order_by(IndexRealtimeSnapshot.id.desc()).first()


def _turnover_series(
    db: Session,
    *,
    index_id: int,
    session: SessionType,
    limit: int = 30,
) -> list[int]:
    rows = (
        db.query(IndexQuoteHistory.turnover_amount)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == session)
        .filter(IndexQuoteHistory.turnover_amount.isnot(None))
        .order_by(IndexQuoteHistory.trade_date.desc())
        .limit(limit)
        .all()
    )
    return [int(v) for (v,) in rows if v is not None]


def _close_points_series(db: Session, *, index_id: int, limit: int = 300) -> list[float]:
    rows = (
        db.query(IndexQuoteHistory.last)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == SessionType.FULL)
        .order_by(IndexQuoteHistory.trade_date.desc())
        .limit(limit)
        .all()
    )
    return [round(v / 100.0, 2) for (v,) in rows if v is not None]


def _latest_turnover_fact(db: Session, *, session: SessionType) -> TurnoverFact | None:
    return (
        db.query(TurnoverFact)
        .filter(TurnoverFact.session == session)
        .order_by(TurnoverFact.trade_date.desc())
        .first()
    )


def _latest_turnover_fact_before(db: Session, *, session: SessionType, before_date: date) -> TurnoverFact | None:
    return (
        db.query(TurnoverFact)
        .filter(TurnoverFact.session == session)
        .filter(TurnoverFact.trade_date < before_date)
        .order_by(TurnoverFact.trade_date.desc())
        .first()
    )


def _latest_hsi_quote(db: Session, *, session: SessionType) -> HsiQuoteFact | None:
    return (
        db.query(HsiQuoteFact)
        .filter(HsiQuoteFact.session == session)
        .order_by(HsiQuoteFact.trade_date.desc())
        .first()
    )


def _turnover_fact_series(db: Session, *, session: SessionType, limit: int = 30) -> list[int]:
    rows = (
        db.query(TurnoverFact.turnover_hkd)
        .filter(TurnoverFact.session == session)
        .order_by(TurnoverFact.trade_date.desc())
        .limit(limit)
        .all()
    )
    return [int(v) for (v,) in rows if v is not None]


def _hsi_quote_points_series(db: Session, *, limit: int = 300) -> list[float]:
    rows = (
        db.query(HsiQuoteFact.last)
        .filter(HsiQuoteFact.session == SessionType.FULL)
        .order_by(HsiQuoteFact.trade_date.desc())
        .limit(limit)
        .all()
    )
    return [round(v / 100.0, 2) for (v,) in rows if v is not None]


def _avg(values: list[int], n: int) -> float:
    if not values:
        return 0.0
    top_n = values[:n]
    return round(sum(top_n) / len(top_n) / 1_000_000_000, 2)


def _to_yi(value: int | None) -> float:
    # legacy name: now returns billions (B)
    if value is None:
        return 0.0
    return round(value / 1_000_000_000, 2)


def _fmt_price(value_x100: int | None) -> str:
    if value_x100 is None:
        return "-"
    return f"{value_x100 / 100:,.2f}"


def _fmt_pct(value_x100: int | None) -> str:
    if value_x100 is None:
        return "--"
    return f"{value_x100 / 100:+.2f}%"


def _fmt_sync_time(value: datetime | None) -> str:
    if value is None:
        return "N/A"
    return value.strftime("%Y-%m-%d %H:%M:%S")


@router.get("/", response_class=HTMLResponse)
def dashboard(request: Request, db: Session = Depends(get_db)):
    today = date.today()

    market_indexes = (
        db.query(MarketIndex)
        .filter(MarketIndex.code.in_(INDEX_CODES))
        .all()
    )
    index_by_code = {row.code.upper(): row for row in market_indexes}

    cards: list[dict] = []
    chart_items: list[dict] = []
    ratio_to_peak: dict[str, int | None] = {}
    sync_points: list[datetime] = []

    for code in INDEX_CODES:
        index_row = index_by_code.get(code)
        full = None
        am = None
        snap_full = None
        snap_am = None

        if index_row is not None:
            full = _latest_index_history(db, index_id=index_row.id, session=SessionType.FULL)
            am = _latest_index_history(db, index_id=index_row.id, session=SessionType.AM)

            # Today's turnover/price come from realtime snapshots.
            snap_full = _today_realtime_snapshot(db, index_id=index_row.id, today=today, session=SessionType.FULL)

            # AM turnover: latest snapshot updated at/before 12:30.
            # - CN indices: we persist explicit session=AM rows.
            # - HSI: we may only have session=FULL snapshots; use those as AM when <=12:30.
            am_cutoff = datetime.combine(today, time(12, 30), tzinfo=ZoneInfo("Asia/Shanghai"))
            snap_am = _today_realtime_snapshot(
                db,
                index_id=index_row.id,
                today=today,
                session=SessionType.AM,
                updated_before=am_cutoff,
            )
            if snap_am is None and code == "HSI":
                snap_am = _today_realtime_snapshot(
                    db,
                    index_id=index_row.id,
                    today=today,
                    session=SessionType.FULL,
                    updated_before=am_cutoff,
                )

        # "today" turnover logic:
        # AM: snapshot (<=12:30) -> history latest AM
        # FULL: snapshot (latest today) -> history latest FULL -> (HSI) turnover_fact latest FULL
        am_turnover = (
            snap_am.turnover_amount
            if snap_am is not None and snap_am.turnover_amount is not None
            else (am.turnover_amount if am is not None else None)
        )
        full_turnover = (
            snap_full.turnover_amount
            if snap_full is not None and snap_full.turnover_amount is not None
            else (full.turnover_amount if full is not None else None)
        )

        full_turnover_series = (
            _turnover_series(db, index_id=index_row.id, session=SessionType.FULL) if index_row is not None else []
        )
        am_turnover_series = (
            _turnover_series(db, index_id=index_row.id, session=SessionType.AM) if index_row is not None else []
        )

        # Yesterday turnover (previous trading day in history table)
        yesterday_full_hist = (
            _latest_index_history_before(db, index_id=index_row.id, session=SessionType.FULL, before_date=today)
            if index_row is not None
            else None
        )
        yesterday_am_hist = (
            _latest_index_history_before(db, index_id=index_row.id, session=SessionType.AM, before_date=today)
            if index_row is not None
            else None
        )
        yesterday_full_turnover = yesterday_full_hist.turnover_amount if yesterday_full_hist is not None else None
        yesterday_am_turnover = yesterday_am_hist.turnover_amount if yesterday_am_hist is not None else None

        points_series = _close_points_series(db, index_id=index_row.id) if index_row is not None else []

        # "latest price" on homepage: today's realtime snapshot first; fallback to history.
        full_last = snap_full.last if snap_full is not None else (full.last if full is not None else None)
        price_change_pct = (
            snap_full.change_pct if snap_full is not None and snap_full.change_pct is not None else (full.change_pct if full is not None else None)
        )
        updated_at = (
            snap_full.data_updated_at
            if snap_full is not None
            else (full.asof_ts if full is not None else None)
        )
        if updated_at is None and full is not None:
            updated_at = full.updated_at

        if code == "HSI":
            fallback_quote_full = _latest_hsi_quote(db, session=SessionType.FULL)
            fallback_turnover_full = _latest_turnover_fact(db, session=SessionType.FULL)

            if full_last is None and fallback_quote_full is not None:
                full_last = fallback_quote_full.last
            if price_change_pct is None and fallback_quote_full is not None:
                price_change_pct = fallback_quote_full.change_pct
            if updated_at is None and fallback_quote_full is not None:
                updated_at = fallback_quote_full.asof_ts or fallback_quote_full.updated_at

            # FULL turnover (HSI): if still missing, fallback to turnover_fact FULL.
            if full_turnover is None and fallback_turnover_full is not None:
                full_turnover = fallback_turnover_full.turnover_hkd
            if updated_at is None and fallback_turnover_full is not None:
                updated_at = fallback_turnover_full.updated_at

            # Yesterday FULL turnover (HSI): if missing, fallback to turnover_fact FULL.
            if yesterday_full_turnover is None:
                y_full_fact = _latest_turnover_fact_before(db, session=SessionType.FULL, before_date=today)
                if y_full_fact is not None:
                    yesterday_full_turnover = y_full_fact.turnover_hkd

            if not full_turnover_series:
                full_turnover_series = _turnover_fact_series(db, session=SessionType.FULL)
            if not am_turnover_series:
                am_turnover_series = _turnover_fact_series(db, session=SessionType.AM)
            if not points_series:
                points_series = _hsi_quote_points_series(db)

        today_points = round((full_last / 100.0), 2) if full_last is not None else 0.0
        max_points = max(points_series, default=today_points)
        if max_points <= 0:
            max_points = max(1.0, today_points)

        is_up = price_change_pct is not None and price_change_pct >= 0
        price_class = "text-emerald-500" if is_up else "text-rose-500"
        if price_change_pct is None:
            price_class = "text-slate-300"
        arrow_path = "M6 15l6-6 6 6" if is_up else "M6 9l6 6 6-6"

        if updated_at is not None:
            sync_points.append(updated_at)

        peak_ratio = None
        peak_turnover = max(full_turnover_series) if full_turnover_series else None
        if full_turnover and peak_turnover:
            peak_ratio = round(full_turnover / peak_turnover * 100)
        ratio_to_peak[code] = peak_ratio

        cards.append(
            {
                "code": code,
                "name": index_row.name_zh if index_row is not None else INDEX_FALLBACK_NAMES[code],
                "chart_id": f"{code.lower()}-chart",
                "last_price": _fmt_price(full_last),
                "change_pct": _fmt_pct(price_change_pct),
                "price_class": price_class,
                "change_class": price_class,
                "arrow_path": arrow_path,
                "updated_at": _fmt_sync_time(updated_at),
                "today_turnover_am": format_amount_b(am_turnover),
                "today_turnover_day": format_amount_b(full_turnover),
            }
        )

        chart_items.append(
            {
                "id": f"{code.lower()}-chart",
                "data": {
                    "todayPoints": today_points,
                    "maxPoints": round(max_points, 2),
                    "todayVolAM": _to_yi(am_turnover),
                    "todayVolDay": _to_yi(full_turnover),
                    "yesterdayVolAM": _to_yi(yesterday_am_turnover),
                    "yesterdayVolDay": _to_yi(yesterday_full_turnover),
                    "avgVolAM": _avg(am_turnover_series, 5),
                    "avgVolDay": _avg(full_turnover_series, 5),
                    "tenAvgVolAM": _avg(am_turnover_series, 10),
                    "tenAvgVolDay": _avg(full_turnover_series, 10),
                    "maxVolAM": _to_yi(max(am_turnover_series) if am_turnover_series else None),
                    "maxVolDay": _to_yi(max(full_turnover_series) if full_turnover_series else None),
                },
            }
        )

    last_data_sync = _fmt_sync_time(max(sync_points) if sync_points else None)
    hsi_ratio = ratio_to_peak.get("HSI")
    sse_ratio = ratio_to_peak.get("SSE")
    insight_text = (
        f"当前恒生指数全日成交量约为近期峰值的 {hsi_ratio}% ，"
        f"上证指数约为 {sse_ratio if sse_ratio is not None else '--'}% 。"
        "若指数点位走强且成交量继续放大，趋势延续概率更高；反之需关注量价背离。"
        if hsi_ratio is not None
        else "暂无足够历史数据生成量价分析，请先运行 backfill_tushare_index 或 fetch_tushare_index / fetch_full 同步数据。"
    )

    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "today": today.isoformat(),
            "cards": cards,
            "charts": chart_items,
            "last_data_sync": last_data_sync,
            "insight_text": insight_text,
            "hsi_ratio": hsi_ratio,
            "sse_ratio": sse_ratio,
        },
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
    latest_run_by_name: dict[str, JobRun] = {}
    for row in runs:
        if row.job_name not in latest_run_by_name:
            latest_run_by_name[row.job_name] = row
    return templates.TemplateResponse(
        "jobs.html",
        {
            "request": request,
            "jobs": runs,
            "available_jobs": AVAILABLE_JOBS,
            "latest_run_by_name": latest_run_by_name,
        },
    )


@router.post("/api/jobs/run")
async def jobs_run(request: Request, job_name: str = Form(...), db: Session = Depends(get_db)):
    form = await request.form()

    params: dict = {}
    params_json = (form.get("params_json") or "").strip()
    if params_json:
        import json

        params.update(json.loads(params_json))

    # Collect param_* fields
    for k, v in form.items():
        if not k.startswith("param_"):
            continue
        name = k[len("param_") :]
        if v is None:
            continue
        if isinstance(v, str):
            val = v.strip()
            if val == "":
                continue
        else:
            val = v
        params[name] = val

    run_job(db, job_name, params=params or None)
    base = (request.scope.get("root_path") or "").rstrip("/")
    return RedirectResponse(url=f"{base}/jobs", status_code=303)
