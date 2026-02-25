from __future__ import annotations

import ipaddress
import json
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
    InsightSnapshot,
    IndexQuoteHistory,
    IndexRealtimeSnapshot,
    IndexRealtimeApiSnapshot,
    JobDefinition,
    JobSchedule,
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
from app.services.insight_service import get_fallback_insight_text, get_latest_insight_snapshot
from app.services.job_scheduler import reload_scheduler
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


def _as_json_array(raw: str | None) -> list:
    if not raw:
        return []
    parsed = json.loads(raw)
    if not isinstance(parsed, list):
        raise ValueError("must be a JSON array")
    return parsed


def _as_json_object(raw: str | None) -> dict:
    if not raw:
        return {}
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError("must be a JSON object")
    return parsed


def _parse_bool_form(form, name: str) -> bool:
    return str(form.get(name, "")).strip().lower() in {"1", "true", "on", "yes"}


def _as_bool(value, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "on", "yes"}


def _parse_job_params(params_schema: list, params: dict) -> dict:
    if not params_schema:
        return params

    normalized = dict(params)
    for item in params_schema:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name") or "").strip()
        if not name:
            continue

        raw = normalized.get(name)
        if raw is None:
            continue
        kind = str(item.get("type") or "text").strip().lower()
        if kind == "number":
            if isinstance(raw, (int, float)):
                normalized[name] = raw
            elif str(raw).strip() != "":
                text = str(raw).strip()
                normalized[name] = float(text) if "." in text else int(text)
        else:
            normalized[name] = str(raw).strip()
    return normalized


def _schedule_summary(rows: list[JobSchedule]) -> str:
    if not rows:
        return "手动"
    active_rows = [r for r in rows if r.is_active]
    if not active_rows:
        return "已禁用"
    parts = [f"{r.day_of_week} {r.hour}:{r.minute}" for r in active_rows]
    return "; ".join(parts[:3]) + (" ..." if len(parts) > 3 else "")


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
    return q.order_by(IndexRealtimeSnapshot.data_updated_at.desc(), IndexRealtimeSnapshot.id.desc()).first()


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
    return q.order_by(IndexRealtimeApiSnapshot.data_updated_at.desc(), IndexRealtimeApiSnapshot.id.desc()).first()


def _order_hsi_realtime_by_data_updated_at(
    snap_realtime: IndexRealtimeSnapshot | None,
    snap_api: IndexRealtimeApiSnapshot | None,
) -> tuple[IndexRealtimeSnapshot | IndexRealtimeApiSnapshot | None, IndexRealtimeSnapshot | IndexRealtimeApiSnapshot | None]:
    if snap_realtime is None and snap_api is None:
        return None, None
    if snap_realtime is None:
        return snap_api, None
    if snap_api is None:
        return snap_realtime, None
    if snap_api.data_updated_at > snap_realtime.data_updated_at:
        return snap_api, snap_realtime
    return snap_realtime, snap_api


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


def _latest_realtime_snapshot_for_kline(db: Session, *, index_id: int) -> IndexRealtimeSnapshot | None:
    return (
        db.query(IndexRealtimeSnapshot)
        .filter(IndexRealtimeSnapshot.index_id == index_id)
        .filter(IndexRealtimeSnapshot.source == "EASTMONEY")
        .order_by(IndexRealtimeSnapshot.id.desc())
        .first()
    )


def _extract_minute_kline_from_payload(payload: dict | None, *, limit: int = 120) -> dict[str, list]:
    if not isinstance(payload, dict):
        return {"times": [], "values": []}

    raw = payload.get("raw") if isinstance(payload.get("raw"), dict) else payload
    resp = raw.get("resp") if isinstance(raw, dict) and isinstance(raw.get("resp"), dict) else raw
    data = resp.get("data") if isinstance(resp, dict) and isinstance(resp.get("data"), dict) else {}

    rows = data.get("klines")
    if not isinstance(rows, list):
        rows = raw.get("klines") if isinstance(raw, dict) else None
    if not isinstance(rows, list):
        return {"times": [], "values": []}

    parsed: list[tuple[str, list[float]]] = []
    for row in rows:
        if isinstance(row, str):
            p = row.split(",")
            if len(p) < 5:
                continue
            ts = p[0].strip()
            try:
                o = float(p[1])
                c = float(p[2])
                h = float(p[3])
                l = float(p[4])
            except ValueError:
                continue
            parsed.append((ts, [o, c, l, h]))
            continue

        if isinstance(row, dict):
            ts = str(row.get("time") or row.get("ts") or row.get("dt") or "").strip()
            if not ts:
                continue
            try:
                o = float(row.get("open"))
                c = float(row.get("close"))
                h = float(row.get("high"))
                l = float(row.get("low"))
            except (TypeError, ValueError):
                continue
            parsed.append((ts, [o, c, l, h]))

    if not parsed:
        return {"times": [], "values": []}

    by_ts: dict[str, list[float]] = {}
    for ts, values in parsed:
        by_ts[ts] = values

    ordered = sorted(by_ts.items(), key=lambda x: x[0])[-limit:]
    times = []
    values = []
    for ts, val in ordered:
        times.append(ts.split(" ")[-1][:5] if " " in ts else ts)
        values.append(val)

    return {"times": times, "values": values}


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
        hsi_primary_full = None
        hsi_secondary_full = None

        if index_row is not None:
            full = _latest_index_history(db, index_id=index_row.id, session=SessionType.FULL)
            am = _latest_index_history(db, index_id=index_row.id, session=SessionType.AM)

            # Today's turnover/price come from realtime snapshots.
            snap_full = _today_realtime_snapshot(db, index_id=index_row.id, today=today, session=SessionType.FULL)
            if code == "HSI":
                snap_api_full = _latest_api_snapshot(db, index_id=index_row.id, today=today, session=SessionType.FULL)
                hsi_primary_full, hsi_secondary_full = _order_hsi_realtime_by_data_updated_at(snap_full, snap_api_full)

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
        # FULL: (HSI) newest(data_updated_at) between realtime_snapshot/api_snapshot -> the other realtime source -> history latest FULL -> turnover_fact latest FULL
        am_turnover = (
            snap_am.turnover_amount
            if snap_am is not None and snap_am.turnover_amount is not None
            else (am.turnover_amount if am is not None else None)
        )
        full_turnover = (
            hsi_primary_full.turnover_amount
            if code == "HSI" and hsi_primary_full is not None and hsi_primary_full.turnover_amount is not None
            else (
                hsi_secondary_full.turnover_amount
                if code == "HSI" and hsi_secondary_full is not None and hsi_secondary_full.turnover_amount is not None
                else (
                    snap_full.turnover_amount
                    if snap_full is not None and snap_full.turnover_amount is not None
                    else (full.turnover_amount if full is not None else None)
                )
            )
            if code == "HSI"
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
                .order_by(IndexRealtimeSnapshot.data_updated_at.desc(), IndexRealtimeSnapshot.id.desc())
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
                    .order_by(IndexRealtimeSnapshot.data_updated_at.desc(), IndexRealtimeSnapshot.id.desc())
                    .first()
                )
            if y_am_snap is not None and y_am_snap.turnover_amount is not None:
                yesterday_am_turnover = int(y_am_snap.turnover_amount)

        points_series = _close_points_series(db, index_id=index_row.id) if index_row is not None else []

        # "latest price" on homepage: today's realtime snapshot first; fallback to history.
        full_last = (
            hsi_primary_full.last
            if code == "HSI" and hsi_primary_full is not None and hsi_primary_full.last is not None
            else (
                hsi_secondary_full.last
                if code == "HSI" and hsi_secondary_full is not None and hsi_secondary_full.last is not None
                else (snap_full.last if snap_full is not None else (full.last if full is not None else None))
            )
        )
        price_change_pct = (
            hsi_primary_full.change_pct
            if code == "HSI" and hsi_primary_full is not None and hsi_primary_full.change_pct is not None
            else (
                hsi_secondary_full.change_pct
                if code == "HSI" and hsi_secondary_full is not None and hsi_secondary_full.change_pct is not None
                else (
                    snap_full.change_pct
                    if snap_full is not None and snap_full.change_pct is not None
                    else (full.change_pct if full is not None else None)
                )
            )
        )
        updated_at = (
            (
                hsi_primary_full.data_updated_at
                if hsi_primary_full is not None
                else (
                    hsi_secondary_full.data_updated_at
                    if hsi_secondary_full is not None
                    else (full.asof_ts if full is not None else None)
                )
            )
            if code == "HSI"
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

        minute_kline = {"times": [], "values": []}
        if index_row is not None:
            latest_any = _latest_realtime_snapshot_for_kline(db, index_id=index_row.id)
            minute_kline = _extract_minute_kline_from_payload(latest_any.payload if latest_any is not None else None)

        cards.append(
            {
                "code": code,
                "name": name,
                "chart_id": f"{code.lower()}-chart",
                "kline_chart_id": f"{code.lower()}-kline-chart",
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
                "kline_id": f"{code.lower()}-kline-chart",
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
                    "minuteKline": minute_kline,
                },
            }
        )

    last_data_sync = _fmt_sync_time(max(sync_points) if sync_points else None)
    hsi_ratio = ratio_to_peak.get("HSI")
    sse_ratio = ratio_to_peak.get("SSE")
    szse_ratio = ratio_to_peak.get("SZSE")
    insight_lang = "en" if lang == "en" else "zh"
    latest_insight = get_latest_insight_snapshot(db, lang=insight_lang)
    insight_text = latest_insight.response if latest_insight is not None else get_fallback_insight_text(insight_lang)

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

    definitions = db.query(JobDefinition).order_by(JobDefinition.ui_order.asc(), JobDefinition.job_name.asc()).all()
    schedules = db.query(JobSchedule).order_by(JobSchedule.job_name.asc(), JobSchedule.schedule_code.asc()).all()
    app_users = db.query(AppUser).order_by(AppUser.created_at.desc()).all()
    schedule_by_job_name: dict[str, list[JobSchedule]] = {}
    for row in schedules:
        schedule_by_job_name.setdefault(row.job_name, []).append(row)

    available_jobs: list[dict] = []
    for row in definitions:
        rows = schedule_by_job_name.get(row.job_name, [])
        schedules_json = [
            {
                "schedule_code": s.schedule_code,
                "trigger_type": s.trigger_type,
                "timezone": s.timezone,
                "second": s.second,
                "minute": s.minute,
                "hour": s.hour,
                "day": s.day,
                "month": s.month,
                "day_of_week": s.day_of_week,
                "jitter_seconds": s.jitter_seconds,
                "misfire_grace_time": s.misfire_grace_time,
                "coalesce": bool(s.coalesce),
                "max_instances": s.max_instances,
                "is_active": bool(s.is_active),
                "description": s.description,
            }
            for s in rows
        ]
        available_jobs.append(
            {
                "name": row.job_name,
                "handler_name": row.handler_name,
                "label": row.label_zh,
                "description": row.description_zh,
                "targets": row.targets or [],
                "params": row.params_schema or [],
                "default_params": row.default_params or {},
                "is_active": bool(row.is_active),
                "manual_enabled": bool(row.manual_enabled),
                "schedule_enabled": bool(row.schedule_enabled),
                "revision": row.revision,
                "schedules": rows,
                "schedules_json": schedules_json,
                "schedule_summary": _schedule_summary(rows),
            }
        )

    return templates.TemplateResponse(
        "jobs.html",
        _template_context(
            request,
            current_user=current_user,
            jobs=runs,
            app_users=app_users,
            available_jobs=available_jobs,
            latest_run_by_name=latest_run_by_name,
        ),
    )


@router.post("/api/job-definitions/save")
async def save_job_definition(
    request: Request,
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is None:
        target = request.url.path.replace("/api/job-definitions/save", "/jobs")
        return build_login_redirect(request, next_path=target)

    form = await request.form()
    job_name = str(form.get("job_name") or "").strip()
    base = (request.scope.get("root_path") or "").rstrip("/")
    if not job_name:
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    row = db.query(JobDefinition).filter(JobDefinition.job_name == job_name).first()
    if row is None:
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    try:
        params_schema = _as_json_array(str(form.get("params_schema_json") or "").strip())
        default_params = _as_json_object(str(form.get("default_params_json") or "").strip())
        schedules = _as_json_array(str(form.get("schedules_json") or "").strip())
    except (ValueError, json.JSONDecodeError):
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    targets = [x.strip() for x in str(form.get("targets_csv") or "").split(",") if x.strip()]

    row.handler_name = str(form.get("handler_name") or row.handler_name).strip() or row.handler_name
    row.label_zh = str(form.get("label_zh") or row.label_zh).strip() or row.label_zh
    row.description_zh = str(form.get("description_zh") or row.description_zh).strip() or row.description_zh
    row.targets = targets
    row.params_schema = params_schema
    row.default_params = default_params
    row.is_active = _parse_bool_form(form, "is_active")
    row.manual_enabled = _parse_bool_form(form, "manual_enabled")
    row.schedule_enabled = _parse_bool_form(form, "schedule_enabled")
    row.revision = int(row.revision or 1) + 1

    db.query(JobSchedule).filter(JobSchedule.job_name == job_name).delete(synchronize_session=False)
    try:
        for item in schedules:
            if not isinstance(item, dict):
                continue
            schedule_code = str(item.get("schedule_code") or "").strip()
            if not schedule_code:
                continue

            db.add(
                JobSchedule(
                    job_name=job_name,
                    schedule_code=schedule_code,
                    trigger_type=str(item.get("trigger_type") or "cron"),
                    timezone=str(item.get("timezone") or settings.TZ),
                    second=str(item.get("second") or "0"),
                    minute=str(item.get("minute") or "*"),
                    hour=str(item.get("hour") or "*"),
                    day=str(item.get("day") or "*"),
                    month=str(item.get("month") or "*"),
                    day_of_week=str(item.get("day_of_week") or "*"),
                    jitter_seconds=int(item["jitter_seconds"]) if item.get("jitter_seconds") not in (None, "") else None,
                    misfire_grace_time=int(item.get("misfire_grace_time") or 120),
                    coalesce=_as_bool(item.get("coalesce"), True),
                    max_instances=max(1, int(item.get("max_instances") or 1)),
                    is_active=_as_bool(item.get("is_active"), True),
                    description=str(item.get("description")).strip() if item.get("description") else None,
                )
            )
    except (TypeError, ValueError):
        db.rollback()
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    db.commit()
    reload_scheduler()
    return RedirectResponse(url=f"{base}/jobs", status_code=303)


@router.post("/api/users/update")
def users_update(
    request: Request,
    user_id: int = Form(...),
    email: str = Form(...),
    display_name: str = Form(""),
    is_active: str | None = Form(None),
    is_superuser: str | None = Form(None),
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is None:
        target = request.url.path.replace("/api/users/update", "/jobs")
        return build_login_redirect(request, next_path=target)

    base = (request.scope.get("root_path") or "").rstrip("/")
    target_user = db.query(AppUser).filter(AppUser.id == user_id).first()
    if target_user is None:
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    email_n = normalize_email(email)
    if not is_valid_email(email_n):
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    exists = db.query(AppUser.id).filter(AppUser.email == email_n, AppUser.id != target_user.id).first()
    if exists is not None:
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    next_active = str(is_active or "").strip().lower() in {"1", "true", "on", "yes"}
    if int(target_user.id) == int(current_user.id) and not next_active:
        next_active = True

    target_user.email = email_n
    target_user.username = email_n
    target_user.display_name = display_name.strip() or None
    target_user.is_active = next_active
    target_user.is_superuser = str(is_superuser or "").strip().lower() in {"1", "true", "on", "yes"}

    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    return RedirectResponse(url=f"{base}/jobs", status_code=303)


@router.get("/api/insights/latest")
def api_latest_insight(
    lang: str = "zh",
    db: Session = Depends(get_db),
    current_user: AppUser | None = Depends(get_current_user),
):
    if current_user is None:
        return {"ok": False, "error": "unauthorized"}

    normalized_lang = "en" if str(lang).strip().lower() == "en" else "zh"
    row: InsightSnapshot | None = get_latest_insight_snapshot(db, lang=normalized_lang)
    if row is None:
        return {
            "ok": True,
            "lang": normalized_lang,
            "status": "fallback",
            "insight_text": get_fallback_insight_text(normalized_lang),
            "provider": None,
            "model": None,
            "asof_ts": None,
            "trade_date": None,
        }
    return {
        "ok": True,
        "lang": row.lang,
        "status": row.status,
        "insight_text": row.response,
        "provider": row.provider,
        "model": row.model,
        "asof_ts": row.asof_ts.isoformat() if row.asof_ts is not None else None,
        "trade_date": row.trade_date.isoformat() if row.trade_date is not None else None,
        "created_at": row.created_at.isoformat() if row.created_at is not None else None,
    }


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

    definition = db.query(JobDefinition).filter(JobDefinition.job_name == job_name).first()
    base = (request.scope.get("root_path") or "").rstrip("/")
    if definition is None or not definition.is_active or not definition.manual_enabled:
        return RedirectResponse(url=f"{base}/jobs", status_code=303)

    form = await request.form()

    params: dict = {}
    params_json = (form.get("params_json") or "").strip()
    if params_json:
        try:
            params.update(json.loads(params_json))
        except json.JSONDecodeError:
            return RedirectResponse(url=f"{base}/jobs", status_code=303)

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

    merged_params = dict(definition.default_params or {})
    merged_params.update(params)
    parsed_params = _parse_job_params(definition.params_schema or [], merged_params)

    run_job(db, definition.handler_name, params=parsed_params or None)
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
