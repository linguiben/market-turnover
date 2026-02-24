from __future__ import annotations

import ipaddress
from datetime import date
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo

import sqlalchemy as sa

from app.config import settings
from fastapi import APIRouter, Depends, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.db.models import (
    AppUser,
    HsiQuoteFact,
    IndexQuoteHistory,
    IndexRealtimeSnapshot,
    IndexRealtimeApiSnapshot,
    JobRun,
    MarketIndex,
    SessionType,
    TurnoverFact,
    UserVisitLog,
)
from app.jobs.tasks import run_job
from app.services.tencent_quote import fetch_quotes
from app.services.trade_corridor import get_trade_corridor_highlights_mock
from app.services.app_cache import get_cache, upsert_cache
from app.web.activity_counter import get_global_visited_count, increment_activity_counter
from app.web.auth import (
    build_login_redirect,
    clear_login_cookie,
    get_current_user,
    hash_password,
    is_valid_email,
    normalize_email,
    safe_next_path,
    set_login_cookie,
    verify_password,
)

router = APIRouter()

templates = Jinja2Templates(directory="app/web/templates")

# template helpers
from app.services.formatting import format_amount_b, format_hsi_price_x100

VISIT_TRACKING_MAX_AGE = 7 * 24 * 3600  # 7 days in seconds
templates.env.globals["format_yi"] = format_amount_b
templates.env.globals["format_hsi"] = format_hsi_price_x100

INDEX_CODES = ("HSI", "SSE", "SZSE")
INDEX_FALLBACK_NAMES = {"HSI": "恒生指数", "SSE": "上证指数", "SZSE": "深证成指"}
INDEX_FALLBACK_NAMES_EN = {"HSI": "Hang Seng Index", "SSE": "Shanghai Composite", "SZSE": "Shenzhen Component"}
AVAILABLE_JOBS: tuple[dict, ...] = (
    {
        "name": "fetch_am",
        "label": "午盘抓取",
        "description": "抓取午盘成交额和 HSI 快照；同时尝试同步最新 Tushare 指数。",
        "schedule": "工作日 11:35",
        "targets": ["turnover_source_record", "turnover_fact", "hsi_quote_fact"],
    },
    {
        "name": "fetch_full",
        "label": "全日抓取",
        "description": "抓取全日成交额和 HSI 快照；同时尝试同步最新 Tushare 指数。",
        "schedule": "工作日 16:10",
        "targets": ["turnover_source_record", "turnover_fact", "hsi_quote_fact"],
    },
    {
        "name": "fetch_tushare_index",
        "label": "同步最新指数",
        "description": "同步 HSI/SSE/SZSE/DJI/IXIC/SPX/FTSE/GDAXI/N225/KS11/CSX5P 的最新一个交易日(日线)数据，使用 Tushare index_global 接口。",
        "schedule": "每日 20:00（定时）+ 手动",
        "targets": ["index_quote_source_record", "index_quote_history", "index_realtime_snapshot"],
    },
    {
        "name": "fetch_eastmoney_realtime_snapshot",
        "label": "抓取EastMoney实时快照(11指数)",
        "description": "使用 push2.eastmoney.com/api/qt/stock/get 抓取11个指数实时快照，落库到 index_realtime_api_snapshot。",
        "schedule": "工作日 09:00-17:00，每2分钟",
        "targets": ["index_realtime_api_snapshot"],
        "params": [
            {"name": "codes", "label": "Index codes (comma)", "type": "text", "placeholder": "HSI,SSE,SZSE,HS11,DJI,IXIC,SPX,N225,UKX,DAX,ESTOXX50E"},
        ],
    },
    {
        "name": "fetch_intraday_snapshot",
        "label": "抓取盘中快照",
        "description": "抓取盘中快照：HSI/SSE/SZSE/HS11(AASTOCKS/EASTMONEY), DJI/IXIC/SPX/N225/UKX/DAX/ESTOXX50E/HS11(Tencent)。默认抓取11个指数。",
        "schedule": "工作日 09:00-17:00，每5分钟",
        "targets": ["index_realtime_snapshot"],
        "params": [
            {"name": "codes", "label": "Index codes (comma)", "type": "text", "placeholder": "HSI,SSE,SZSE,HS11,DJI,IXIC,SPX,N225,UKX,DAX,ESTOXX50E"},
            {"name": "force_source", "label": "Force source (optional)", "type": "text", "placeholder": "AASTOCKS/EASTMONEY/TUSHARE"},
        ],
    },
    {
        "name": "refresh_home_global_quotes",
        "label": "刷新主页-全球股市(Tencent)",
        "description": "刷新主页『全球股市（免費：Tencent 行情）』缓存数据。",
        "schedule": "每5分钟",
        "targets": ["app_cache"],
    },
    {
        "name": "refresh_home_trade_corridor",
        "label": "刷新主页-Trade Corridor(POC)",
        "description": "刷新主页『Most Active Trade Corridor（跨市場資金通道｜POC）』缓存数据（当前为MOCK）。",
        "schedule": "每5分钟",
        "targets": ["app_cache"],
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
        "description": "回填最近 1 年 HSI/SSE/SZSE/DJI/IXIC/SPX/FTSE/GDAXI/N225/KS11/CSX5P 日线数据（跳过已存在记录）。",
        "targets": ["index_quote_source_record", "index_quote_history"],
    },
    {
        "name": "backfill_cn_halfday",
        "label": "回填A股半日成交(90天)",
        "description": "用 Eastmoney 分钟线回填 SSE/SZSE 的半日成交额与全日成交额（用于柱状图和均值）。",
        "targets": ["index_quote_history", "index_quote_source_record"],
    },
    {
        "name": "persist_eastmoney_kline_all",
        "label": "保存Eastmoney分钟K线(可得范围)",
        "description": "把 Eastmoney 当前可返回的 1m/5m 指数K线写入 index_kline_source_record（HSI/SSE/SZSE）。",
        "targets": ["index_kline_source_record"],
        "params": [
            {"name": "lookback_days_1m", "label": "Lookback days (1m)", "type": "number", "placeholder": "365"},
            {"name": "lookback_days_5m", "label": "Lookback days (5m)", "type": "number", "placeholder": "365"}
        ],
    },
    {
        "name": "backfill_hsi_am_from_kline",
        "label": "回填HSI半日成交(由K线聚合)",
        "description": "基于 index_kline_source_record(EASTMONEY,5m) 聚合回填 HSI 的历史 AM turnover 到 index_quote_history。",
        "targets": ["index_kline_source_record", "index_quote_source_record", "index_quote_history"],
        "params": [
            {"name": "date_from", "label": "Date from (YYYY-MM-DD, optional)", "type": "text", "placeholder": "2026-01-12"},
            {"name": "date_to", "label": "Date to (YYYY-MM-DD, optional)", "type": "text", "placeholder": "2026-02-11"}
        ],
    },
    {
        "name": "backfill_hkex",
        "label": "回填HKEX历史",
        "description": "从 HKEX 统计页面回填港股成交额历史（FULL）。",
        "targets": ["turnover_source_record", "turnover_fact"],
    },
    {
        "name": "backfill_hsi_am_yesterday",
        "label": "回填HSI昨日半日成交",
        "description": "从 Eastmoney 1分钟K线聚合回填 HSI 昨日半日成交（<=12:30）。",
        "targets": ["index_realtime_snapshot"],
        "params": [
            {"name": "trade_date", "label": "Trade date (YYYY-MM-DD, optional)", "type": "text", "placeholder": "2026-02-10"},
        ],
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


def _latest_api_snapshot(
    db: Session,
    *,
    index_id: int,
    today: date,
    session: SessionType | None = None,
) -> IndexRealtimeApiSnapshot | None:
    q = (
        db.query(IndexRealtimeApiSnapshot)
        .filter(IndexRealtimeApiSnapshot.index_id == index_id)
        .filter(IndexRealtimeApiSnapshot.trade_date == today)
    )
    if session is not None:
        q = q.filter(IndexRealtimeApiSnapshot.session == session)
    return q.order_by(IndexRealtimeApiSnapshot.id.desc()).first()


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


def _template_context(request: Request, *, current_user: AppUser | None, **kwargs):
    data = {"request": request, "current_user": current_user}
    data.update(kwargs)
    return data


def _extract_client_ip_for_log(request: Request) -> str:
    xff = request.headers.get("x-forwarded-for")
    if xff:
        raw = xff.split(",", 1)[0].strip()
    else:
        raw = (request.headers.get("x-real-ip") or "").strip()
    if not raw and request.client and request.client.host:
        raw = request.client.host
    if not raw:
        return "127.0.0.1"
    try:
        ipaddress.ip_address(raw)
        return raw
    except ValueError:
        return "127.0.0.1"


def _append_auth_visit_log(db: Session, request: Request, *, user_id: int, action_type: str) -> None:
    try:
        row = UserVisitLog(
            user_id=user_id,
            ip_address=_extract_client_ip_for_log(request),
            session_id=None,
            action_type=action_type,
            user_agent=request.headers.get("user-agent"),
            browser_family=None,
            os_family=None,
            device_type=None,
            request_url=str(request.url),
            referer_url=request.headers.get("referer"),
            request_headers=None,
        )
        db.add(row)
        db.commit()
    except Exception:
        db.rollback()


def _dashboard_impl(
    request: Request,
    *,
    db: Session,
    current_user: AppUser | None,
    lang: str,
):
    today = date.today()
    visited_count = get_global_visited_count(db)

    market_indexes = db.query(MarketIndex).filter(MarketIndex.code.in_(INDEX_CODES)).all()
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
        snap_api_full = None

        if index_row is not None:
            full = _latest_index_history(db, index_id=index_row.id, session=SessionType.FULL)
            am = _latest_index_history(db, index_id=index_row.id, session=SessionType.AM)

            # Today's turnover/price come from realtime snapshots.
            snap_full = _today_realtime_snapshot(db, index_id=index_row.id, today=today, session=SessionType.FULL)
            if code == "HSI":
                snap_api_full = _latest_api_snapshot(db, index_id=index_row.id, today=today, session=SessionType.FULL)

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
        # FULL: (HSI) Eastmoney stock/get snapshot -> intraday snapshot -> history latest FULL -> (HSI) turnover_fact latest FULL
        am_turnover = (
            snap_am.turnover_amount
            if snap_am is not None and snap_am.turnover_amount is not None
            else (am.turnover_amount if am is not None else None)
        )
        full_turnover = (
            snap_api_full.turnover_amount
            if code == "HSI" and snap_api_full is not None and snap_api_full.turnover_amount is not None
            else (
                snap_full.turnover_amount
                if snap_full is not None and snap_full.turnover_amount is not None
                else (full.turnover_amount if full is not None else None)
            )
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

        # HSI yesterday AM: allow backfill from realtime snapshots (append-only) when history table has no AM.
        if code == "HSI" and index_row is not None and yesterday_am_turnover is None:
            # Determine yesterday trading date (prefer history FULL date; else calendar yesterday)
            y_date = yesterday_full_hist.trade_date if yesterday_full_hist is not None else (today - timedelta(days=1))
            # Try snapshot session=AM first; fallback to session=FULL snapshot <=12:30
            y_am_snap = (
                db.query(IndexRealtimeSnapshot)
                .filter(IndexRealtimeSnapshot.index_id == index_row.id)
                .filter(IndexRealtimeSnapshot.trade_date == y_date)
                .filter(IndexRealtimeSnapshot.session == SessionType.AM)
                .order_by(IndexRealtimeSnapshot.id.desc())
                .first()
            )
            if y_am_snap is None:
                y_cutoff = datetime.combine(y_date, time(12, 30), tzinfo=ZoneInfo("Asia/Shanghai"))
                y_am_snap = (
                    db.query(IndexRealtimeSnapshot)
                    .filter(IndexRealtimeSnapshot.index_id == index_row.id)
                    .filter(IndexRealtimeSnapshot.trade_date == y_date)
                    .filter(IndexRealtimeSnapshot.session == SessionType.FULL)
                    .filter(IndexRealtimeSnapshot.data_updated_at <= y_cutoff)
                    .order_by(IndexRealtimeSnapshot.id.desc())
                    .first()
                )
            if y_am_snap is not None and y_am_snap.turnover_amount is not None:
                yesterday_am_turnover = int(y_am_snap.turnover_amount)

        points_series = _close_points_series(db, index_id=index_row.id) if index_row is not None else []

        # "latest price" on homepage: today's realtime snapshot first; fallback to history.
        full_last = snap_full.last if snap_full is not None else (full.last if full is not None else None)
        price_change_pct = (
            snap_full.change_pct
            if snap_full is not None and snap_full.change_pct is not None
            else (full.change_pct if full is not None else None)
        )
        updated_at = (
            snap_api_full.data_updated_at
            if code == "HSI" and snap_api_full is not None
            else (
                snap_full.data_updated_at
                if snap_full is not None
                else (full.asof_ts if full is not None else None)
            )
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

        # Peak FULL turnover over history (not limited by series length).
        peak_ratio = None
        peak_turnover = None
        if index_row is not None:
            if code == "HSI":
                peak_turnover = (
                    db.query(sa.func.max(TurnoverFact.turnover_hkd))
                    .filter(TurnoverFact.session == SessionType.FULL)
                    .scalar()
                )
            else:
                peak_turnover = (
                    db.query(sa.func.max(IndexQuoteHistory.turnover_amount))
                    .filter(IndexQuoteHistory.index_id == index_row.id)
                    .filter(IndexQuoteHistory.session == SessionType.FULL)
                    .scalar()
                )

        if full_turnover and peak_turnover:
            peak_ratio = round(full_turnover / peak_turnover * 100)
        ratio_to_peak[code] = peak_ratio

        if lang == "en":
            name = None
            if index_row is not None:
                name = (index_row.name_en or "").strip() or None
            if not name:
                name = INDEX_FALLBACK_NAMES_EN.get(code, code)
        else:
            name = index_row.name_zh if index_row is not None else INDEX_FALLBACK_NAMES[code]

        cards.append(
            {
                "code": code,
                "name": name,
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
                    "maxVolDay": _to_yi(int(peak_turnover) if peak_turnover is not None else None),
                },
            }
        )

    last_data_sync = _fmt_sync_time(max(sync_points) if sync_points else None)
    hsi_ratio = ratio_to_peak.get("HSI")
    sse_ratio = ratio_to_peak.get("SSE")
    szse_ratio = ratio_to_peak.get("SZSE")

    if lang == "en":
        if hsi_ratio is not None:
            insight_text = (
                f"Today’s Hang Seng Index full-day turnover is about {hsi_ratio}% of its recent peak; "
                f"Shanghai Composite is about {sse_ratio if sse_ratio is not None else '--'}%; "
                f"Shenzhen Component is about {szse_ratio if szse_ratio is not None else '--'}%. "
                "If the index level strengthens and turnover keeps expanding, the trend is more likely to continue; "
                "otherwise watch for price-volume divergence."
            )
        else:
            insight_text = (
                "Not enough historical data to generate the analysis yet. "
                "Please run backfill_tushare_index, or run fetch_tushare_index / fetch_full to sync data."
            )
    else:
        insight_text = (
            f"当前恒生指数全日成交量约为近期峰值的 {hsi_ratio}% ，"
            f"上证指数约为 {sse_ratio if sse_ratio is not None else '--'}% ，"
            f"深证成指约为 {szse_ratio if szse_ratio is not None else '--'}% 。"
            "若指数点位走强且成交量继续放大，趋势延续概率更高；反之需关注量价背离。"
            if hsi_ratio is not None
            else "暂无足够历史数据生成量价分析，请先运行 backfill_tushare_index 或 fetch_tushare_index / fetch_full 同步数据。"
        )

    # Global market quotes from database (consistent with top cards)
    global_quotes = []
    try:
        active_indices = (
            db.query(MarketIndex)
            .filter(MarketIndex.is_active.is_(True))
            .order_by(MarketIndex.display_order.asc())
            .all()
        )
        for idx in active_indices:
            snap = (
                db.query(IndexRealtimeSnapshot)
                .filter(IndexRealtimeSnapshot.index_id == idx.id)
                .filter(IndexRealtimeSnapshot.session == SessionType.FULL)
                .order_by(IndexRealtimeSnapshot.id.desc())
                .first()
            )
            if snap:
                # Use name_en if lang=en (assuming lang var is available from context)
                idx_name = idx.name_zh
                if "lang" in locals() and lang == "en" and idx.name_en:
                    idx_name = idx.name_en

                global_quotes.append({
                    "name": idx_name,
                    "last": snap.last / 100.0,
                    "change": snap.change_points / 100.0 if snap.change_points is not None else 0.0,
                    "pct": snap.change_pct / 100.0 if snap.change_pct is not None else 0.0,
                    "asof": _fmt_sync_time(snap.data_updated_at),
                })
    except Exception:
        global_quotes = []

    # Trade corridor highlights (POC: mock)
    corridor = None
    cached_c = get_cache(db, key="homepage:trade_corridor")
    if cached_c is not None and isinstance(cached_c.payload, dict) and cached_c.payload.get("rows"):
        corridor = cached_c.payload
    else:
        try:
            c = get_trade_corridor_highlights_mock()
            corridor = c.__dict__
            corridor["rows"] = [r.__dict__ for r in c.rows]
            if c.highest_turnover is not None:
                corridor["highest_turnover"] = c.highest_turnover.__dict__
            if c.max_net_inflow is not None:
                corridor["max_net_inflow"] = c.max_net_inflow.__dict__
            if c.max_net_outflow is not None:
                corridor["max_net_outflow"] = c.max_net_outflow.__dict__
            if c.max_trades is not None:
                corridor["max_trades"] = c.max_trades.__dict__
            upsert_cache(db, key="homepage:trade_corridor", payload=corridor)
        except Exception:
            corridor = None

    template_name = "dashboard_en.html" if lang == "en" else "dashboard.html"
    return templates.TemplateResponse(
        template_name,
        _template_context(
            request,
            current_user=current_user,
            today=today.isoformat(),
            cards=cards,
            charts=chart_items,
            last_data_sync=last_data_sync,
            visited_count=visited_count,
            insight_text=insight_text,
            hsi_ratio=hsi_ratio,
            sse_ratio=sse_ratio,
            szse_ratio=szse_ratio,
            global_quotes=global_quotes,
            corridor=corridor,
        ),
    )


@router.get("/", response_class=HTMLResponse)
def dashboard_en(
    request: Request,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    return _dashboard_impl(request, db=db, current_user=current_user, lang="en")


@router.get("/cn", response_class=HTMLResponse)
def dashboard_cn(
    request: Request,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    return _dashboard_impl(request, db=db, current_user=current_user, lang="zh")


@router.get("/disclaimer", response_class=HTMLResponse)
def disclaimer(
    request: Request,
    current_user: AppUser | None = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "disclaimer.html",
        _template_context(request, current_user=current_user),
    )


@router.get("/cn/disclaimer", response_class=HTMLResponse)
def disclaimer_cn(
    request: Request,
    current_user: AppUser | None = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "disclaimer.html",
        _template_context(request, current_user=current_user),
    )


@router.get("/contact", response_class=HTMLResponse)
def contact_page(
    request: Request,
    current_user: AppUser | None = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "contact.html",
        _template_context(request, current_user=current_user),
    )


@router.get("/cn/contact", response_class=HTMLResponse)
def contact_page_cn(
    request: Request,
    current_user: AppUser | None = Depends(get_current_user),
):
    return templates.TemplateResponse(
        "contact.html",
        _template_context(request, current_user=current_user),
    )


@router.get("/recent", response_class=HTMLResponse)
def recent(
    request: Request,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is None:
        return build_login_redirect(request)

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

    return templates.TemplateResponse(
        "recent.html",
        _template_context(request, current_user=current_user, facts=facts, quotes=quotes),
    )


@router.get("/jobs", response_class=HTMLResponse)
def jobs(
    request: Request,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is None:
        return build_login_redirect(request)

    runs = db.query(JobRun).order_by(JobRun.started_at.desc()).limit(50).all()
    latest_run_by_name: dict[str, JobRun] = {}
    for row in runs:
        if row.job_name not in latest_run_by_name:
            latest_run_by_name[row.job_name] = row
    return templates.TemplateResponse(
        "jobs.html",
        _template_context(
            request,
            current_user=current_user,
            jobs=runs,
            available_jobs=AVAILABLE_JOBS,
            latest_run_by_name=latest_run_by_name,
        ),
    )


@router.post("/api/jobs/run")
async def jobs_run(
    request: Request,
    job_name: str = Form(...),
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is None:
        target = request.url.path.replace("/api/jobs/run", "/jobs")
        return build_login_redirect(request, next_path=target)

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


@router.get("/register", response_class=HTMLResponse)
def register_page(request: Request, current_user: AppUser | None = Depends(get_current_user)):
    next_path = safe_next_path(request.query_params.get("next"), fallback="/jobs")
    if current_user is not None:
        return RedirectResponse(url=next_path, status_code=303)
    return templates.TemplateResponse(
        "register.html",
        _template_context(
            request,
            current_user=None,
            next_path=next_path,
            email="",
            display_name="",
            error=None,
        ),
    )


@router.post("/register", response_class=HTMLResponse)
def register_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    password_confirm: str = Form(...),
    display_name: str = Form(""),
    next_path: str = Form("/jobs"),
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    safe_next = safe_next_path(next_path, fallback="/jobs")
    if current_user is not None:
        return RedirectResponse(url=safe_next, status_code=303)

    email_n = normalize_email(email)
    display_name = display_name.strip()

    error = None
    if not is_valid_email(email_n):
        error = "邮箱格式无效。"
    elif len(password) < 8:
        error = "密码至少需要 8 位。"
    elif password != password_confirm:
        error = "两次输入的密码不一致。"
    elif db.query(AppUser.id).filter(AppUser.email == email_n).first() is not None:
        error = "该邮箱已注册。"

    if error is not None:
        return templates.TemplateResponse(
            "register.html",
            _template_context(
                request,
                current_user=None,
                next_path=safe_next,
                email=email_n,
                display_name=display_name,
                error=error,
            ),
            status_code=400,
        )

    user = AppUser(
        username=email_n,
        email=email_n,
        password_hash=hash_password(password),
        display_name=display_name or None,
        is_active=False,
        is_superuser=False,
        last_login_at=None,
    )
    db.add(user)
    try:
        db.commit()
        db.refresh(user)
    except IntegrityError:
        db.rollback()
        return templates.TemplateResponse(
            "register.html",
            _template_context(
                request,
                current_user=None,
                next_path=safe_next,
                email=email_n,
                display_name=display_name,
                error="该邮箱已注册。",
            ),
            status_code=400,
        )

    _append_auth_visit_log(db, request, user_id=int(user.id), action_type="register")
    return templates.TemplateResponse(
        "register_pending.html",
        _template_context(
            request,
            current_user=None,
            email=email_n,
        ),
    )


@router.get("/login", response_class=HTMLResponse)
def login_page(request: Request, current_user: AppUser | None = Depends(get_current_user)):
    next_path = safe_next_path(request.query_params.get("next"), fallback="/jobs")
    if current_user is not None:
        return RedirectResponse(url=next_path, status_code=303)
    return templates.TemplateResponse(
        "login.html",
        _template_context(request, current_user=None, next_path=next_path, email="", error=None),
    )


@router.post("/login", response_class=HTMLResponse)
def login_submit(
    request: Request,
    email: str = Form(...),
    password: str = Form(...),
    next_path: str = Form("/jobs"),
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    safe_next = safe_next_path(next_path, fallback="/jobs")
    if current_user is not None:
        return RedirectResponse(url=safe_next, status_code=303)

    email_n = normalize_email(email)
    user = db.query(AppUser).filter(AppUser.email == email_n).first()
    if user is None or not verify_password(password, user.password_hash):
        return templates.TemplateResponse(
            "login.html",
            _template_context(
                request,
                current_user=None,
                next_path=safe_next,
                email=email_n,
                error="邮箱或密码错误。",
            ),
            status_code=401,
        )
    if not user.is_active:
        return templates.TemplateResponse(
            "login.html",
            _template_context(
                request,
                current_user=None,
                next_path=safe_next,
                email=email_n,
                error="用户已被禁用。",
            ),
            status_code=403,
        )

    user.last_login_at = datetime.now(timezone.utc)
    db.add(user)
    db.commit()

    _append_auth_visit_log(db, request, user_id=int(user.id), action_type="login")
    
    # Deduplicate login count using the same cookie logic
    has_tracked_cookie = request.cookies.get("v_tracked") == "1"
    if not has_tracked_cookie:
        increment_activity_counter(db, event="login")
    
    db.commit()
    response = RedirectResponse(url=safe_next, status_code=303)
    set_login_cookie(response, int(user.id))
    
    # Also set v_tracked on login redirect to ensure immediate deduplication
    response.set_cookie(
        key="v_tracked",
        value="1",
        max_age=int(VISIT_TRACKING_MAX_AGE),  # 7 days
        path="/",
        httponly=True,
        samesite="lax",
    )
    return response


@router.post("/logout")
def logout(
    request: Request,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is not None:
        _append_auth_visit_log(db, request, user_id=int(current_user.id), action_type="logout")
    base = (request.scope.get("root_path") or "").rstrip("/")
    response = RedirectResponse(url=f"{base}/", status_code=303)
    clear_login_cookie(response)
    return response
