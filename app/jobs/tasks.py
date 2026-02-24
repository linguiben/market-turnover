from __future__ import annotations

import traceback
import random
import time as pytime
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from datetime import date, time

from app.config import settings
from app.db.models import IndexKlineSourceRecord, IndexQuoteSourceRecord, JobRun, HsiQuoteFact, KlineInterval, SessionType, TurnoverSourceRecord, IndexQuoteHistory, IndexRealtimeApiSnapshot
from app.services.index_quote_resolver import (
    add_index_source_record,
    ensure_market_index,
    normalize_index_code,
    upsert_index_history_from_sources,
    upsert_realtime_snapshot,
)
from app.services.resolver import upsert_fact_from_sources
from app.services.intraday_bars import upsert_intraday_bar
from app.sources.hkex import fetch_hkex_latest_table
from app.sources.aastocks import fetch_midday_turnover
from app.sources.aastocks_index import fetch_hsi_snapshot
from app.sources.tushare_index import TushareIndexDaily, daily_row_asof, fetch_index_daily_history, fetch_latest_index_daily
from app.sources.tencent_index import fetch_index_daily_history as fetch_tencent_index_daily_history
from app.sources.eastmoney_index import fetch_minute_kline, aggregate_halfday_and_fullday_amount
from app.sources.eastmoney_intraday import fetch_intraday_snapshot as fetch_eastmoney_intraday_snapshot
from app.sources.eastmoney_realtime import default_codes as eastmoney_realtime_default_codes, fetch_realtime_snapshot as fetch_eastmoney_realtime_snapshot
from app.sources.tushare_kline import fetch_index_kline
from app.services.tencent_quote import fetch_quotes
from app.services.trade_corridor import get_trade_corridor_highlights_mock
from app.services.app_cache import upsert_cache


def _persist_tushare_rows(
    db: Session,
    *,
    rows: list[TushareIndexDaily],
    skip_existing_source: bool,
) -> dict[str, int]:
    if not rows:
        return {"rows": 0, "inserted": 0, "skipped_existing": 0, "facts_updated": 0, "snapshots_updated": 0}

    index_id_cache: dict[str, int] = {}
    for row in rows:
        if row.code not in index_id_cache:
            index_row = ensure_market_index(db, row.code)
            index_id_cache[row.code] = index_row.id

    existing_keys: set[tuple[int, date]] = set()
    if skip_existing_source and index_id_cache:
        min_date = min(row.trade_date for row in rows)
        max_date = max(row.trade_date for row in rows)
        existing_rows = (
            db.query(IndexQuoteSourceRecord.index_id, IndexQuoteSourceRecord.trade_date)
            .filter(IndexQuoteSourceRecord.source == "TUSHARE")
            .filter(IndexQuoteSourceRecord.session == SessionType.FULL)
            .filter(IndexQuoteSourceRecord.index_id.in_(list(index_id_cache.values())))
            .filter(IndexQuoteSourceRecord.trade_date >= min_date)
            .filter(IndexQuoteSourceRecord.trade_date <= max_date)
            .all()
        )
        existing_keys = {(idx, dt) for idx, dt in existing_rows}

    inserted = 0
    skipped_existing = 0
    facts_updated = 0
    snapshots_updated = 0

    for row in rows:
        index_id = index_id_cache[row.code]
        key = (index_id, row.trade_date)
        if skip_existing_source and key in existing_keys:
            skipped_existing += 1
            continue

        asof = daily_row_asof(row.trade_date)
        payload = {
            "ts_code": row.ts_code,
            "turnover_unit": row.turnover_unit,
            "volume": row.volume,
            "raw": row.raw,
        }

        add_index_source_record(
            db,
            index_id=index_id,
            trade_date=row.trade_date,
            session=SessionType.FULL,
            source="TUSHARE",
            last=int(round(row.close * 100)),
            change_points=int(round(row.change * 100)) if row.change is not None else None,
            change_pct=int(round(row.pct_chg * 100)) if row.pct_chg is not None else None,
            turnover_amount=row.turnover_amount,
            turnover_currency=None,
            asof_ts=asof,
            payload=payload,
            ok=True,
        )
        inserted += 1

        fact = upsert_index_history_from_sources(
            db,
            index_id=index_id,
            trade_date=row.trade_date,
            session=SessionType.FULL,
        )
        if fact is not None:
            facts_updated += 1

        upsert_realtime_snapshot(
            db,
            index_id=index_id,
            trade_date=row.trade_date,
            session=SessionType.FULL,
            last=int(round(row.close * 100)),
            change_points=int(round(row.change * 100)) if row.change is not None else None,
            change_pct=int(round(row.pct_chg * 100)) if row.pct_chg is not None else None,
            turnover_amount=row.turnover_amount,
            turnover_currency="HKD" if row.code == "HSI" else "CNY",
            data_updated_at=asof,
            is_closed=True,
            source="TUSHARE",
            payload=payload,
        )
        snapshots_updated += 1

    return {
        "rows": len(rows),
        "inserted": inserted,
        "skipped_existing": skipped_existing,
        "facts_updated": facts_updated,
        "snapshots_updated": snapshots_updated,
    }


def _sync_tushare_index_quotes(db: Session) -> tuple[str, dict]:
    token = (settings.TUSHARE_PRO_TOKEN or "").strip()
    if not token:
        return "skipped", {"enabled": False, "reason": "TUSHARE_PRO_TOKEN is empty"}

    index_map = settings.tushare_index_map()
    if not index_map:
        return "skipped", {"enabled": False, "reason": "TUSHARE_INDEX_CODES is empty"}

    try:
        rows = fetch_latest_index_daily(
            token=token,
            index_map=index_map,
            base_url=settings.TUSHARE_PRO_BASE,
            timeout_seconds=settings.TUSHARE_TIMEOUT_SECONDS,
        )
    except Exception as e:
        return "partial", {"enabled": True, "error": str(e)}

    write_stats = _persist_tushare_rows(db, rows=rows, skip_existing_source=False)
    write_stats["enabled"] = True

    return "success", write_stats


def _backfill_tushare_index_quotes(db: Session, *, lookback_days: int = 90) -> tuple[str, dict]:
    """Backfill index quotes.

    Primary source: Tushare `index_daily`.
    Fallback: Tencent public kline (CN indices only) when Tushare permission is missing.
    """

    token = (settings.TUSHARE_PRO_TOKEN or "").strip()
    index_map = settings.tushare_index_map()

    if not index_map:
        return "skipped", {"enabled": False, "reason": "TUSHARE_INDEX_CODES is empty"}

    # 1) Try Tushare first (if token configured)
    if token:
        try:
            rows = fetch_index_daily_history(
                token=token,
                index_map=index_map,
                base_url=settings.TUSHARE_PRO_BASE,
                timeout_seconds=settings.TUSHARE_TIMEOUT_SECONDS,
                lookback_days=lookback_days,
            )
            write_stats = _persist_tushare_rows(db, rows=rows, skip_existing_source=True)
            write_stats["enabled"] = True
            write_stats["lookback_days"] = lookback_days
            unique_dates = sorted({row.trade_date for row in rows})
            if unique_dates:
                write_stats["date_from"] = str(unique_dates[0])
                write_stats["date_to"] = str(unique_dates[-1])
            return "success", write_stats
        except Exception as e:
            err = str(e)
            # Permission error: fallback to Tencent
            if "没有接口访问权限" not in err and "doc_id=108" not in err:
                return "partial", {"enabled": True, "lookback_days": lookback_days, "error": err}

    # 2) Tencent fallback (CN indices only)
    try:
        tencent_rows = fetch_tencent_index_daily_history(
            index_map=index_map,
            lookback_days=max(lookback_days, 15),
            timeout_seconds=settings.TUSHARE_TIMEOUT_SECONDS,
        )
    except Exception as e:
        return "partial", {"enabled": True, "fallback": "TENCENT", "lookback_days": lookback_days, "error": str(e)}

    if not tencent_rows:
        return "partial", {"enabled": True, "fallback": "TENCENT", "lookback_days": lookback_days, "rows": 0}

    index_id_cache: dict[str, int] = {}
    inserted = 0
    facts_updated = 0
    snapshots_updated = 0

    # Keep one row per (code, trade_date)
    latest: dict[tuple[str, date], object] = {}
    for row in tencent_rows:
        latest[(row.code, row.trade_date)] = row

    for (code, trade_date), row in sorted(latest.items(), key=lambda x: (x[0][0], x[0][1])):
        if code not in index_id_cache:
            index_row = ensure_market_index(db, code)
            index_id_cache[code] = index_row.id

        index_id = index_id_cache[code]
        asof = daily_row_asof(trade_date)

        add_index_source_record(
            db,
            index_id=index_id,
            trade_date=trade_date,
            session=SessionType.FULL,
            source="TENCENT",
            last=int(round(float(row.close) * 100)),
            change_points=int(round(float(row.change) * 100)) if row.change is not None else None,
            change_pct=int(round(float(row.pct_chg) * 100)) if row.pct_chg is not None else None,
            # Tencent kline provides a `volume` field; we persist it as turnover_amount so
            # dashboard bars (today/avg/max) are non-empty.
            turnover_amount=int(round(float(row.volume))) if row.volume is not None else None,
            turnover_currency="CNY",
            asof_ts=asof,
            payload={"symbol": row.symbol, "raw": row.raw, "volume": row.volume},
            ok=True,
        )
        inserted += 1

        fact = upsert_index_history_from_sources(db, index_id=index_id, trade_date=trade_date, session=SessionType.FULL)
        if fact is not None:
            facts_updated += 1

        upsert_realtime_snapshot(
            db,
            index_id=index_id,
            trade_date=trade_date,
            session=SessionType.FULL,
            last=int(round(float(row.close) * 100)),
            change_points=int(round(float(row.change) * 100)) if row.change is not None else None,
            change_pct=int(round(float(row.pct_chg) * 100)) if row.pct_chg is not None else None,
            turnover_amount=int(round(float(row.volume))) if row.volume is not None else None,
            turnover_currency="CNY",
            data_updated_at=asof,
            is_closed=True,
            source="TENCENT",
            payload={"symbol": row.symbol, "raw": row.raw, "volume": row.volume},
        )
        snapshots_updated += 1

    unique_dates = sorted({d for (_, d) in latest.keys()})

    return "success", {
        "enabled": True,
        "fallback": "TENCENT",
        "lookback_days": lookback_days,
        "rows": len(tencent_rows),
        "inserted": inserted,
        "facts_updated": facts_updated,
        "snapshots_updated": snapshots_updated,
        "date_from": str(unique_dates[0]) if unique_dates else None,
        "date_to": str(unique_dates[-1]) if unique_dates else None,
    }


def _backfill_eastmoney_cn_halfday(db: Session, *, lookback_days: int = 90) -> tuple[str, dict]:
    """Backfill CN index half-day and full-day turnover amounts using Eastmoney minute kline.

    This is used to populate AM turnover bars and 5/10-day averages for SSE/SZSE.
    """

    index_map = settings.tushare_index_map()
    if not index_map:
        return "skipped", {"enabled": False, "reason": "TUSHARE_INDEX_CODES is empty"}

    target = {k: v for k, v in index_map.items() if k.upper() in {"SSE", "SZSE"}}
    if not target:
        return "skipped", {"enabled": False, "reason": "No CN indices configured (need SSE/SZSE)"}

    inserted_source = 0
    facts_updated = 0

    date_min = None
    date_max = None

    for code, ts_code in target.items():
        bars = fetch_minute_kline(
            ts_code=ts_code,
            lookback_days=lookback_days,
            timeout_seconds=settings.HKEX_TIMEOUT_SECONDS,
            klt="5",
        )
        agg = aggregate_halfday_and_fullday_amount(bars=bars)

        index_row = ensure_market_index(db, code)

        for d, item in agg.items():
            date_min = d if date_min is None or d < date_min else date_min
            date_max = d if date_max is None or d > date_max else date_max

            # AM session
            if item.get("am_close") is not None and item.get("am_amount") is not None:
                add_index_source_record(
                    db,
                    index_id=index_row.id,
                    trade_date=d,
                    session=SessionType.AM,
                    source="EASTMONEY",
                    last=int(round(float(item["am_close"]) * 100)),
                    change_points=None,
                    change_pct=None,
                    turnover_amount=int(item["am_amount"]),
                    turnover_currency="CNY",
                    asof_ts=datetime.combine(d, time(11, 30), tzinfo=None),
                    payload={"ts_code": ts_code, "bars": item.get("bars")},
                    ok=True,
                )
                inserted_source += 1
                if upsert_index_history_from_sources(db, index_id=index_row.id, trade_date=d, session=SessionType.AM):
                    facts_updated += 1

            # FULL session
            if item.get("full_close") is not None and item.get("full_amount") is not None:
                add_index_source_record(
                    db,
                    index_id=index_row.id,
                    trade_date=d,
                    session=SessionType.FULL,
                    source="EASTMONEY",
                    last=int(round(float(item["full_close"]) * 100)),
                    change_points=None,
                    change_pct=None,
                    turnover_amount=int(item["full_amount"]),
                    turnover_currency="CNY",
                    asof_ts=daily_row_asof(d),
                    payload={"ts_code": ts_code, "bars": item.get("bars")},
                    ok=True,
                )
                inserted_source += 1
                if upsert_index_history_from_sources(db, index_id=index_row.id, trade_date=d, session=SessionType.FULL):
                    facts_updated += 1

    return "success", {
        "enabled": True,
        "source": "EASTMONEY",
        "lookback_days": lookback_days,
        "inserted_source": inserted_source,
        "facts_updated": facts_updated,
        "date_from": str(date_min) if date_min else None,
        "date_to": str(date_max) if date_max else None,
    }


def _persist_eastmoney_kline_rows(
    db: Session,
    *,
    code: str,
    ts_code: str,
    klt: str,
    lookback_days: int,
) -> dict[str, int | str | None]:
    if klt not in {"1", "5"}:
        raise ValueError(f"unsupported klt: {klt}")

    bars = fetch_minute_kline(
        ts_code=ts_code,
        lookback_days=lookback_days,
        timeout_seconds=settings.HKEX_TIMEOUT_SECONDS,
        klt=klt,
    )
    if not bars:
        return {
            "rows": 0,
            "inserted": 0,
            "interval": "1m" if klt == "1" else "5m",
            "date_from": None,
            "date_to": None,
        }

    index_row = ensure_market_index(db, code)
    interval = KlineInterval.M1 if klt == "1" else KlineInterval.M5
    cn_tz = timezone(timedelta(hours=8))

    values: list[dict] = []
    for bar in bars:
        bar_time = bar.dt.replace(tzinfo=cn_tz)
        values.append(
            {
                "index_id": index_row.id,
                "interval": interval.value,
                "bar_time": bar_time,
                "trade_date": bar.trade_date,
                "source": "EASTMONEY",
                "open": int(round(float(bar.open) * 100)) if bar.open is not None else None,
                "high": int(round(float(bar.high) * 100)) if bar.high is not None else None,
                "low": int(round(float(bar.low) * 100)) if bar.low is not None else None,
                "close": int(round(float(bar.close) * 100)) if bar.close is not None else None,
                "volume": int(round(float(bar.volume))) if bar.volume is not None else None,
                "turnover_amount": int(round(float(bar.amount))) if bar.amount is not None else None,
                "turnover_currency": "HKD" if code.upper() == "HSI" else "CNY",
                "asof_ts": bar_time,
                "payload": {"ts_code": ts_code, "klt": klt, "raw": bar.raw},
                "ok": True,
                "error": None,
            }
        )

    stmt = pg_insert(IndexKlineSourceRecord.__table__).values(values)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["index_id", "interval", "bar_time", "source"],
    ).returning(IndexKlineSourceRecord.id)
    inserted_ids = list(db.execute(stmt).scalars())
    db.commit()

    dates = sorted({bar.trade_date for bar in bars})
    return {
        "rows": len(values),
        "inserted": len(inserted_ids),
        "interval": interval.value,
        "date_from": str(dates[0]) if dates else None,
        "date_to": str(dates[-1]) if dates else None,
    }


def _persist_tushare_kline_rows(
    db: Session,
    *,
    code: str,
    ts_code: str,
    freq: str,
    start_date: str,
    end_date: str,
) -> dict[str, int | str | None]:
    token = (settings.TUSHARE_PRO_TOKEN or "").strip()
    if not token:
        raise RuntimeError("TUSHARE_PRO_TOKEN is empty")

    bars = fetch_index_kline(
        token=token,
        ts_code=ts_code,
        freq=freq,
        start_date=start_date,
        end_date=end_date,
        timeout_seconds=settings.TUSHARE_TIMEOUT_SECONDS,
    )

    if not bars:
        return {"rows": 0, "inserted": 0, "interval": freq, "date_from": None, "date_to": None}

    index_row = ensure_market_index(db, code)
    interval = KlineInterval.M1 if freq == "1min" else KlineInterval.M5
    tz8 = timezone(timedelta(hours=8))

    values: list[dict] = []
    for bar in bars:
        bar_time = bar.trade_time.replace(tzinfo=tz8)
        values.append(
            {
                "index_id": index_row.id,
                "interval": interval.value,
                "bar_time": bar_time,
                "trade_date": bar_time.date(),
                "source": "TUSHARE",
                "open": int(round(float(bar.open) * 100)) if bar.open is not None else None,
                "high": int(round(float(bar.high) * 100)) if bar.high is not None else None,
                "low": int(round(float(bar.low) * 100)) if bar.low is not None else None,
                "close": int(round(float(bar.close) * 100)) if bar.close is not None else None,
                "volume": int(round(float(bar.vol))) if bar.vol is not None else None,
                "turnover_amount": int(round(float(bar.amount))) if bar.amount is not None else None,
                "turnover_currency": "HKD" if code.upper() == "HSI" else "CNY",
                "asof_ts": bar_time,
                "payload": {"ts_code": ts_code, "freq": freq, "raw": bar.raw},
                "ok": True,
                "error": None,
            }
        )

    stmt = pg_insert(IndexKlineSourceRecord.__table__).values(values)
    stmt = stmt.on_conflict_do_nothing(
        index_elements=["index_id", "interval", "bar_time", "source"],
    ).returning(IndexKlineSourceRecord.id)
    inserted_ids = list(db.execute(stmt).scalars())
    db.commit()

    dates = sorted({v["trade_date"] for v in values})
    return {
        "rows": len(values),
        "inserted": len(inserted_ids),
        "interval": interval.value,
        "date_from": str(dates[0]) if dates else None,
        "date_to": str(dates[-1]) if dates else None,
    }


def _backfill_intraday_kline_source(db: Session, *, lookback_days_5m: int = 90, lookback_days_1m: int = 2) -> tuple[str, dict]:
    index_map = settings.tushare_index_map()
    if not index_map:
        return "skipped", {"enabled": False, "reason": "TUSHARE_INDEX_CODES is empty"}

    # NOTE: we persist multiple sources:
    # - EASTMONEY for SSE/SZSE (stable public API)
    # - TUSHARE for SSE/SZSE (subject to strict rate limits)
    #   (HSI minute-kline on Tushare is too rate-limited; skip for now)
    target = {k.upper(): v for k, v in index_map.items() if k.upper() in {"SSE", "SZSE"}}

    written = 0
    rows = 0
    details: dict[str, dict] = {}
    errors: dict[str, str] = {}

    today = date.today()
    # Tushare uses YYYYMMDD
    d1_start = (today - timedelta(days=lookback_days_1m)).strftime("%Y%m%d")
    d5_start = (today - timedelta(days=lookback_days_5m)).strftime("%Y%m%d")
    d_end = today.strftime("%Y%m%d")

    for code, ts_code in target.items():
        per_code: dict[str, dict] = {}

        # EASTMONEY (CN only)
        if code in {"SSE", "SZSE"}:
            for klt, days in (("1", lookback_days_1m), ("5", lookback_days_5m)):
                key = f"EASTMONEY:{'1m' if klt == '1' else '5m'}"
                try:
                    stats = _persist_eastmoney_kline_rows(
                        db,
                        code=code,
                        ts_code=ts_code,
                        klt=klt,
                        lookback_days=days,
                    )
                    per_code[key] = stats
                    written += int(stats.get("inserted") or 0)
                    rows += int(stats.get("rows") or 0)
                except Exception as e:
                    errors[f"{code}:{key}"] = str(e)

        # TUSHARE (SSE/SZSE only; keep calls minimal to avoid rate limits)
        for freq, start in (("5min", d5_start),):
            key = "TUSHARE:5m"
            try:
                stats = _persist_tushare_kline_rows(
                    db,
                    code=code,
                    ts_code=ts_code,
                    freq=freq,
                    start_date=start,
                    end_date=d_end,
                )
                per_code[key] = stats
                written += int(stats.get("inserted") or 0)
                rows += int(stats.get("rows") or 0)
            except Exception as e:
                errors[f"{code}:{key}"] = str(e)

        details[code] = per_code

    status = "success" if not errors else ("partial" if written > 0 else "failed")
    return status, {
        "enabled": True,
        "sources": ["EASTMONEY", "TUSHARE"],
        "rows": rows,
        "inserted": written,
        "lookback_days_1m": lookback_days_1m,
        "lookback_days_5m": lookback_days_5m,
        "details": details,
        "errors": errors,
    }


def _refresh_home_global_quotes(db: Session) -> tuple[str, dict]:
    """Refresh homepage global quotes cache.

    Disabled by default because some deployment environments may have blocked/slow DNS,
    which would cause this job to hang and potentially affect server responsiveness.

    You can re-enable it once outbound DNS/HTTPS is confirmed working.
    """
    symbols = ["r_hkHSI", "usDJI", "usIXIC", "s_sh000001", "s_sz399001"]
    return "partial", {"symbols": symbols, "disabled": True, "reason": "outbound DNS/HTTPS may be blocked"}


def _refresh_home_trade_corridor(db: Session) -> tuple[str, dict]:
    try:
        highlights = get_trade_corridor_highlights_mock()
        payload = asdict(highlights)
        upsert_cache(db, key="homepage:trade_corridor", payload=payload)
        return "success", {"rows": len(highlights.rows) if highlights.rows else 0, "source": "MOCK"}
    except Exception as e:
        return "partial", {"error": str(e)}


def run_job(db: Session, job_name: str, params: dict | None = None) -> JobRun:
    run = JobRun(job_name=job_name, status="running", summary={"params": params} if params else None)
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        if job_name == "refresh_home_global_quotes":
            status, summary = _refresh_home_global_quotes(db)

        elif job_name == "refresh_home_trade_corridor":
            status, summary = _refresh_home_trade_corridor(db)

        elif job_name == "fetch_am":
            # Use HK timezone trading date when possible; fallback to local date.
            today = date.today()

            # 1) Turnover (best-effort)
            try:
                mid = fetch_midday_turnover()
                t_source = "AASTOCKS"
                turnover = mid.turnover_hkd
                asof = mid.asof
                if asof is not None:
                    today = asof.date()
                t_payload = {"raw": mid.raw_turnover_text}
                t_status = "success"
            except Exception as e:
                # POC fallback: generate a visible data point even when scraping fails.
                t_source = "MOCK"
                turnover = 133_823_000_000  # 1338.23 億（示例）
                asof = None
                t_payload = {"fallback": True, "error": str(e)}
                t_status = "partial"

            rec = TurnoverSourceRecord(
                trade_date=today,
                session=SessionType.AM,
                source=t_source,
                turnover_hkd=turnover,
                asof_ts=asof,
                payload=t_payload,
                ok=True,
            )
            db.add(rec)
            db.commit()
            upsert_fact_from_sources(db, today, SessionType.AM)

            # 2) HSI price snapshot
            try:
                snap = fetch_hsi_snapshot()
                if snap.asof is not None:
                    today = snap.asof.date()
                hsi = HsiQuoteFact(
                    trade_date=today,
                    session=SessionType.AM,
                    last=int(round(snap.last * 100)),
                    change=int(round(snap.change * 100)) if snap.change is not None else None,
                    change_pct=int(round(snap.change_pct * 100)) if snap.change_pct is not None else None,
                    turnover_hkd=snap.turnover_hkd,
                    asof_ts=snap.asof,
                    source="AASTOCKS",
                    payload={"raw": snap.raw},
                )
                db.add(hsi)
                db.commit()
                h_status = "success"
            except Exception as e:
                h_status = "partial"
                # do not fail the whole job

            status = "success" if (t_status == "success" and h_status == "success") else "partial"
            ts_status, ts_summary = _sync_tushare_index_quotes(db)
            if ts_status == "partial" and status == "success":
                status = "partial"
            summary = {
                "today": str(today),
                "turnover_hkd": turnover,
                "turnover_source": t_source,
                "turnover_asof": str(asof),
                "hsi_status": h_status,
                "tushare": ts_summary,
            }

        elif job_name == "backfill_hkex":
            """Backfill HK market FULL-day turnover from HKEX official statistics archive.

            This job must be accurate: it will NOT generate mock data.
            """

            rows: list = []
            try:
                rows = fetch_hkex_latest_table()
            except Exception as e:
                summary = {"mode": "hkex", "error": str(e)}
                status = "failed"
                raise

            if not rows:
                summary = {"mode": "hkex", "rows": 0}
                status = "failed"
                raise RuntimeError("HKEX archive returned 0 rows")

            # only keep latest ~1 year trading days (approx 252) for UI/history
            rows = rows[-260:]

            inserted = 0
            updated = 0

            for r in rows:
                rec = TurnoverSourceRecord(
                    trade_date=r.trade_date,
                    session=SessionType.FULL,
                    source="HKEX",
                    turnover_hkd=r.turnover_hkd,
                    payload={"is_half_day": r.is_half_day},
                    ok=True,
                )
                db.add(rec)
                inserted += 1
            db.commit()

            for r in rows:
                if upsert_fact_from_sources(db, r.trade_date, SessionType.FULL):
                    updated += 1

            summary = {
                "mode": "hkex",
                "rows": len(rows),
                "inserted": inserted,
                "facts_updated": updated,
                "date_from": str(rows[0].trade_date),
                "date_to": str(rows[-1].trade_date),
            }
            status = "success"

        elif job_name == "fetch_full":
            # Use HK timezone trading date when possible; fallback to local date.
            today = date.today()

            # For POC: use the same AASTOCKS index feed turnover as end-of-day turnover proxy.
            # (Not official; HKEX backfill remains the official baseline when available.)
            try:
                snap = fetch_hsi_snapshot()
                turnover = snap.turnover_hkd
                source = "AASTOCKS"
                asof = snap.asof
                if asof is not None:
                    today = asof.date()
                payload = {"raw": snap.raw}
                status = "success"
            except Exception as e:
                source = "MOCK"
                turnover = 233_823_000_000
                asof = None
                payload = {"fallback": True, "error": str(e)}
                status = "partial"

            rec = TurnoverSourceRecord(
                trade_date=today,
                session=SessionType.FULL,
                source=source,
                turnover_hkd=turnover,
                asof_ts=asof,
                payload=payload,
                ok=True,
            )
            db.add(rec)
            db.commit()
            upsert_fact_from_sources(db, today, SessionType.FULL)

            # HSI price snapshot (close-ish)
            if source == "AASTOCKS":
                try:
                    hsi = HsiQuoteFact(
                        trade_date=today,
                        session=SessionType.FULL,
                        last=int(round(snap.last * 100)),
                        change=int(round(snap.change * 100)) if snap.change is not None else None,
                        change_pct=int(round(snap.change_pct * 100)) if snap.change_pct is not None else None,
                        turnover_hkd=snap.turnover_hkd,
                        asof_ts=snap.asof,
                        source="AASTOCKS",
                        payload={"raw": snap.raw},
                    )
                    db.add(hsi)
                    db.commit()
                except Exception:
                    pass

            ts_status, ts_summary = _sync_tushare_index_quotes(db)
            if ts_status == "partial" and status == "success":
                status = "partial"
            summary = {"today": str(today), "turnover_hkd": turnover, "source": source, "asof": str(asof), "tushare": ts_summary}

        elif job_name == "fetch_tushare_index":
            ts_status, ts_summary = _sync_tushare_index_quotes(db)
            status = "success" if ts_status in {"success", "skipped"} else "partial"
            summary = {"tushare": ts_summary}

        elif job_name == "fetch_intraday_bars_cn_5m":
            # Persist CN index 5-minute kline bars (raw snapshots)
            index_map = settings.tushare_index_map()
            written = 0
            errors: dict[str, str] = {}

            lookback_days = 7
            try:
                if params and params.get("lookback_days") is not None:
                    lookback_days = int(params.get("lookback_days"))
            except Exception:
                lookback_days = 7

            for code in ("SSE", "SZSE"):
                try:
                    ts_code = (index_map.get(code) or "").strip()
                    if not ts_code:
                        raise RuntimeError("missing ts_code in TUSHARE_INDEX_CODES")

                    # klt=5 minute bars, best-effort range
                    bars = fetch_minute_kline(
                        ts_code=ts_code,
                        lookback_days=lookback_days,
                        timeout_seconds=settings.HKEX_TIMEOUT_SECONDS,
                        klt="5",
                    )
                    index_row = ensure_market_index(db, code)

                    for bar in bars:
                        upsert_intraday_bar(
                            db,
                            index_id=index_row.id,
                            interval_min=5,
                            bar_ts=bar.dt,
                            tz="Asia/Shanghai",
                            open_x100=int(round(bar.open * 100)) if bar.open is not None else None,
                            high_x100=int(round(bar.high * 100)) if bar.high is not None else None,
                            low_x100=int(round(bar.low * 100)) if bar.low is not None else None,
                            close_x100=int(round(bar.close * 100)),
                            volume=int(round(bar.volume)) if bar.volume is not None else None,
                            amount=int(round(bar.amount)) if bar.amount is not None else None,
                            currency="CNY",
                            source="EASTMONEY",
                            payload={"raw": bar.raw, "ts_code": ts_code},
                        )
                        written += 1
                except Exception as e:
                    errors[code] = str(e)

            status = "success" if not errors else ("partial" if written else "failed")
            summary = {"written": written, "errors": errors, "interval_min": 5, "source": "EASTMONEY", "lookback_days": lookback_days}

        elif job_name == "fetch_eastmoney_realtime_snapshot":

            # Fetch realtime snapshot from Eastmoney stock/get for all 11 indices (or provided codes)
            codes = eastmoney_realtime_default_codes()
            if params and params.get("codes"):
                codes = [c.strip().upper() for c in str(params.get("codes")).split(",") if c.strip()]

            written = 0
            errors: dict[str, str] = {}

            for code in codes:
                try:
                    jitter_seconds = random.randint(1, 30)
                    pytime.sleep(jitter_seconds)

                    snap = fetch_eastmoney_realtime_snapshot(code=code, timeout_seconds=settings.HKEX_TIMEOUT_SECONDS)
                    index_row = ensure_market_index(db, code)

                    row = IndexRealtimeApiSnapshot(
                        index_id=index_row.id,
                        code=code,
                        secid=snap.secid,
                        trade_date=snap.asof.date(),
                        session=SessionType.FULL,
                        last=int(round(float(snap.last) * 100)) if snap.last is not None else None,
                        change_points=int(round(float(snap.change) * 100)) if snap.change is not None else None,
                        change_pct=int(round(float(snap.pct_chg) * 100)) if snap.pct_chg is not None else None,
                        turnover_amount=int(round(float(snap.amount))) if snap.amount is not None else None,
                        turnover_currency="HKD" if code == "HSI" else "CNY",
                        volume=int(round(float(snap.volume))) if snap.volume is not None else None,
                        data_updated_at=snap.asof,
                        source="EASTMONEY_STOCK_GET",
                        payload={"raw": snap.raw, "secid": snap.secid},
                    )
                    db.add(row)
                    db.commit()
                    written += 1
                except Exception as e:
                    db.rollback()
                    errors[code] = str(e)

            status = "success" if not errors else ("partial" if written else "failed")
            summary = {
                "source": "EASTMONEY_STOCK_GET",
                "codes": codes,
                "written": written,
                "errors": errors,
            }

        elif job_name == "fetch_intraday_snapshot":

            # jitter: random wait 1-30 seconds before each run to reduce burst risk
            jitter_seconds = random.randint(1, 30)
            pytime.sleep(jitter_seconds)

            # Intraday snapshot for indices (default: all 11 indices)
            index_map = settings.tushare_index_map()
            written = 0
            errors: dict[str, str] = {}

            # Default: HSI, SSE, SZSE, HS11 (Korea), DJI, IXIC, SPX, N225, UKX, DAX, ESTOXX50E
            codes = ["HSI", "SSE", "SZSE", "HS11", "DJI", "IXIC", "SPX", "N225", "UKX", "DAX", "ESTOXX50E"]
            if params and params.get("codes"):
                codes = [c.strip().upper() for c in str(params.get("codes")).split(",") if c.strip()]

            force_source = (str(params.get("force_source")).strip().upper() if params and params.get("force_source") else "")

            # 1) HSI
            if "HSI" in codes:
                try:
                    index_row = ensure_market_index(db, "HSI")

                    if not force_source or force_source == "EASTMONEY":
                        # Prefer Eastmoney for more precise last (2 decimals) and stable access.
                        em = fetch_eastmoney_intraday_snapshot(ts_code="HSI", timeout_seconds=settings.HKEX_TIMEOUT_SECONDS)
                        upsert_realtime_snapshot(
                            db,
                            index_id=index_row.id,
                            trade_date=em.trade_date,
                            session=SessionType.FULL,
                            last=int(round(float(em.last) * 100)),
                            change_points=int(round(float(em.change) * 100)) if em.change is not None else None,
                            change_pct=int(round(float(em.pct_chg) * 100)) if em.pct_chg is not None else None,
                            turnover_amount=int(round(float(em.amount))) if em.amount is not None else None,
                            turnover_currency="HKD",
                            data_updated_at=em.asof,
                            is_closed=False,
                            source="EASTMONEY",
                            payload={"raw": em.raw, "ts_code": "HSI"},
                        )
                        written += 1
                    else:
                        if force_source != "AASTOCKS":
                            raise RuntimeError(f"HSI snapshot only supports EASTMONEY/AASTOCKS (force_source={force_source})")

                        snap = fetch_hsi_snapshot()
                        if snap.asof is not None:
                            trade_date = snap.asof.date()
                            asof = snap.asof
                        else:
                            trade_date = date.today()
                            asof = datetime.now(timezone.utc)

                        upsert_realtime_snapshot(
                            db,
                            index_id=index_row.id,
                            trade_date=trade_date,
                            session=SessionType.FULL,
                            last=int(round(float(snap.last) * 100)),
                            change_points=int(round(float(snap.change) * 100)) if snap.change is not None else None,
                            change_pct=int(round(float(snap.change_pct) * 100)) if snap.change_pct is not None else None,
                            turnover_amount=int(snap.turnover_hkd) if snap.turnover_hkd is not None else None,
                            turnover_currency="HKD",
                            data_updated_at=asof,
                            is_closed=False,
                            source="AASTOCKS",
                            payload={"raw": snap.raw},
                        )
                        written += 1

                except Exception as e:
                    errors["HSI"] = str(e)

            # 2) SSE/SZSE from Eastmoney intraday minute kline
            for code in ("SSE", "SZSE"):
                if code not in codes:
                    continue
                try:
                    if force_source and force_source != "EASTMONEY":
                        raise RuntimeError(f"{code} snapshot only supports EASTMONEY currently (force_source={force_source})")

                    ts_code = (index_map.get(code) or "").strip()
                    if not ts_code:
                        raise RuntimeError("missing ts_code in TUSHARE_INDEX_CODES")
                    snap = fetch_eastmoney_intraday_snapshot(ts_code=ts_code, timeout_seconds=settings.HKEX_TIMEOUT_SECONDS)
                    index_row = ensure_market_index(db, code)

                    # FULL snapshot (latest)
                    upsert_realtime_snapshot(
                        db,
                        index_id=index_row.id,
                        trade_date=snap.trade_date,
                        session=SessionType.FULL,
                        last=int(round(float(snap.last) * 100)),
                        change_points=int(round(float(snap.change) * 100)) if snap.change is not None else None,
                        change_pct=int(round(float(snap.pct_chg) * 100)) if snap.pct_chg is not None else None,
                        turnover_amount=int(round(float(snap.amount))) if snap.amount is not None else None,
                        turnover_currency="CNY",
                        data_updated_at=snap.asof,
                        is_closed=False,
                        source="EASTMONEY",
                        payload={"raw": snap.raw, "ts_code": ts_code, "scope": "FULL"},
                    )
                    written += 1

                    # AM snapshot (<=12:30), for dashboard AM turnover selection
                    if snap.am_amount is not None and snap.am_asof is not None:
                        upsert_realtime_snapshot(
                            db,
                            index_id=index_row.id,
                            trade_date=snap.trade_date,
                            session=SessionType.AM,
                            last=int(round(float(snap.am_last) * 100)) if snap.am_last is not None else int(round(float(snap.last) * 100)),
                            change_points=int(round(float(snap.change) * 100)) if snap.change is not None else None,
                            change_pct=int(round(float(snap.pct_chg) * 100)) if snap.pct_chg is not None else None,
                            turnover_amount=int(round(float(snap.am_amount))),
                            turnover_currency="CNY",
                            data_updated_at=snap.am_asof,
                            is_closed=False,
                            source="EASTMONEY",
                            payload={"raw": snap.raw, "ts_code": ts_code, "scope": "AM", "cutoff": "12:30"},
                        )
                        written += 1
                except Exception as e:
                    errors[code] = str(e)

            # 3) DJI/IXIC from Tencent quotes
            us_codes = [c for c in ("DJI", "IXIC") if c in codes]
            if us_codes:
                try:
                    # Mapping for Tencent Quote symbols
                    us_symbol_map = {"DJI": "usDJI", "IXIC": "usIXIC"}
                    fetch_syms = [us_symbol_map[c] for c in us_codes]
                    quotes = fetch_quotes(fetch_syms)
                    quote_by_code = {q.symbol.replace("us", ""): q for q in quotes}

                    for code in us_codes:
                        q = quote_by_code.get(code)
                        if not q:
                            continue
                        
                        index_row = ensure_market_index(db, code)
                        asof_dt = datetime.now(timezone.utc)
                        if q.asof:
                            try:
                                # US asof format: 2024-05-10 16:00:00 (EST/EDT)
                                # For simplicity, parse and use current date if needed
                                asof_dt = datetime.strptime(q.asof, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo("America/New_York"))
                            except Exception:
                                pass

                        upsert_realtime_snapshot(
                            db,
                            index_id=index_row.id,
                            trade_date=asof_dt.date(),
                            session=SessionType.FULL,
                            last=int(round(q.last * 100)),
                            change_points=int(round(q.change * 100)),
                            change_pct=int(round(q.pct * 100)),
                            turnover_amount=None,  # Tencent quote might not have accurate US turnover in simple format
                            turnover_currency="USD",
                            data_updated_at=asof_dt,
                            is_closed=False,
                            source="TENCENT",
                            payload={"raw": vars(q), "symbol": us_symbol_map[code]},
                        )
                        written += 1
                except Exception as e:
                    errors["US_INDICES"] = str(e)

            # 4) SPX, N225, UKX, DAX, ESTOXX50E, HS11 from Tencent quotes
            global_codes = [c for c in ("SPX", "N225", "UKX", "DAX", "ESTOXX50E", "HS11") if c in codes]
            if global_codes:
                try:
                    # Mapping for Tencent Quote symbols
                    global_symbol_map = {
                        "SPX": "usSPX",
                        "N225": "jpN225",
                        "UKX": "ukUKX",
                        "DAX": "euDAX",
                        "ESTOXX50E": "euESTOXX50E",
                        "HS11": "krHS11",
                    }
                    currency_map = {
                        "SPX": "USD",
                        "N225": "JPY",
                        "UKX": "GBP",
                        "DAX": "EUR",
                        "ESTOXX50E": "EUR",
                        "HS11": "KRW",
                    }
                    timezone_map = {
                        "SPX": "America/New_York",
                        "N225": "Asia/Tokyo",
                        "UKX": "Europe/London",
                        "DAX": "Europe/Berlin",
                        "ESTOXX50E": "Europe/Berlin",
                        "HS11": "Asia/Seoul",
                    }
                    fetch_syms = [global_symbol_map[c] for c in global_codes]
                    quotes = fetch_quotes(fetch_syms)
                    # Tencent returns symbol with prefix, e.g. "usSPX", "jpN225"
                    quote_by_code = {}
                    for q in quotes:
                        for code, sym in global_symbol_map.items():
                            if q.symbol == sym:
                                quote_by_code[code] = q
                                break

                    missing_codes: list[str] = []
                    for code in global_codes:
                        q = quote_by_code.get(code)
                        if not q:
                            missing_codes.append(code)
                            continue

                        index_row = ensure_market_index(db, code)
                        asof_dt = datetime.now(timezone.utc)
                        if q.asof:
                            try:
                                asof_dt = datetime.strptime(q.asof, "%Y-%m-%d %H:%M:%S").replace(tzinfo=ZoneInfo(timezone_map.get(code, "UTC")))
                            except Exception:
                                pass

                        upsert_realtime_snapshot(
                            db,
                            index_id=index_row.id,
                            trade_date=asof_dt.date(),
                            session=SessionType.FULL,
                            last=int(round(q.last * 100)),
                            change_points=int(round(q.change * 100)),
                            change_pct=int(round(q.pct * 100)),
                            turnover_amount=None,
                            turnover_currency=currency_map.get(code, "USD"),
                            data_updated_at=asof_dt,
                            is_closed=False,
                            source="TENCENT",
                            payload={"raw": vars(q), "symbol": global_symbol_map[code]},
                        )
                        written += 1

                    # Tencent does not reliably return all global symbols.
                    # Fallback to Tushare index_global for missing codes.
                    if missing_codes:
                        token = (settings.TUSHARE_PRO_TOKEN or "").strip()
                        if not token:
                            for code in missing_codes:
                                errors[code] = "No quote returned from Tencent; TUSHARE_PRO_TOKEN is empty"
                        else:
                            tushare_defaults = {
                                "HSI": "HSI",
                                "SSE": "000001.SH",
                                "SZSE": "399001.SZ",
                                "DJI": "DJI",
                                "IXIC": "IXIC",
                                "SPX": "SPX",
                                "N225": "N225",
                                "FTSE": "FTSE",
                                "GDAXI": "GDAXI",
                                "CSX5P": "CSX5P",
                                "KS11": "KS11",
                            }
                            cfg_map = settings.tushare_index_map()
                            for k, v in tushare_defaults.items():
                                cfg_map.setdefault(k, v)

                            display_to_tushare = {
                                "HS11": "KS11",
                                "UKX": "FTSE",
                                "DAX": "GDAXI",
                                "ESTOXX50E": "CSX5P",
                            }
                            code_currency = {
                                "SPX": "USD",
                                "N225": "JPY",
                                "UKX": "GBP",
                                "DAX": "EUR",
                                "ESTOXX50E": "EUR",
                                "HS11": "KRW",
                            }

                            fetch_map: dict[str, str] = {}
                            request_map: dict[str, str] = {}
                            for code in missing_codes:
                                display_code = normalize_index_code(code)
                                ts_key = display_to_tushare.get(display_code, display_code)
                                ts_code = cfg_map.get(ts_key)
                                if not ts_code:
                                    errors[display_code] = f"Missing Tushare mapping for {ts_key}"
                                    continue
                                fetch_map[ts_key] = ts_code
                                request_map[ts_key] = display_code

                            if fetch_map:
                                try:
                                    rows = fetch_latest_index_daily(
                                        token=token,
                                        index_map=fetch_map,
                                        base_url=settings.TUSHARE_PRO_BASE,
                                        timeout_seconds=settings.TUSHARE_TIMEOUT_SECONDS,
                                    )
                                    row_by_code = {r.code: r for r in rows}
                                    for ts_key, display_code in request_map.items():
                                        row = row_by_code.get(ts_key)
                                        if row is None:
                                            errors[display_code] = "No quote returned from Tencent/Tushare"
                                            continue
                                        index_row = ensure_market_index(db, display_code)
                                        upsert_realtime_snapshot(
                                            db,
                                            index_id=index_row.id,
                                            trade_date=row.trade_date,
                                            session=SessionType.FULL,
                                            last=int(round(float(row.close) * 100)),
                                            change_points=int(round(float(row.change) * 100)) if row.change is not None else None,
                                            change_pct=int(round(float(row.pct_chg) * 100)) if row.pct_chg is not None else None,
                                            turnover_amount=row.turnover_amount,
                                            turnover_currency=code_currency.get(display_code, index_row.currency),
                                            data_updated_at=daily_row_asof(row.trade_date),
                                            is_closed=True,
                                            source="TUSHARE",
                                            payload={"ts_code": row.ts_code, "fallback": "tencent_missing"},
                                        )
                                        written += 1
                                except Exception as e:
                                    for code in missing_codes:
                                        errors[code] = f"Tushare fallback failed: {e}"
                except Exception as e:
                    errors["GLOBAL_INDICES"] = str(e)

            status = "success" if not errors else ("partial" if written else "failed")
            summary = {"written": written, "errors": errors, "codes": codes, "force_source": force_source or None}

        elif job_name == "backfill_tushare_index":
            ts_status, ts_summary = _backfill_tushare_index_quotes(db, lookback_days=365)
            status = "success" if ts_status in {"success", "skipped"} else "partial"
            summary = {"tushare": ts_summary}

        elif job_name == "backfill_cn_halfday":
            em_status, em_summary = _backfill_eastmoney_cn_halfday(db, lookback_days=90)
            status = "success" if em_status in {"success", "skipped"} else "partial"
            summary = {"eastmoney": em_summary}

        elif job_name == "backfill_intraday_kline":
            k_status, k_summary = _backfill_intraday_kline_source(db, lookback_days_5m=90, lookback_days_1m=2)
            status = "success" if k_status in {"success", "skipped"} else ("partial" if k_status == "partial" else "failed")
            summary = {"kline": k_summary}

        elif job_name == "persist_eastmoney_kline_all":
            index_map = settings.tushare_index_map()
            if not index_map:
                status = "skipped"
                summary = {"enabled": False, "reason": "TUSHARE_INDEX_CODES is empty"}
            else:
                lookback_days_1m = 365
                lookback_days_5m = 365
                try:
                    if params and params.get("lookback_days_1m") is not None:
                        lookback_days_1m = int(params.get("lookback_days_1m"))
                    if params and params.get("lookback_days_5m") is not None:
                        lookback_days_5m = int(params.get("lookback_days_5m"))
                except Exception:
                    pass

                target = {k.upper(): v for k, v in index_map.items() if k.upper() in {"HSI", "SSE", "SZSE"}}
                written = 0
                rows = 0
                details: dict[str, dict] = {}
                errors: dict[str, str] = {}

                for code, ts_code in target.items():
                    per_code: dict[str, dict] = {}
                    for klt, days in (("1", lookback_days_1m), ("5", lookback_days_5m)):
                        key = "1m" if klt == "1" else "5m"
                        try:
                            stats = _persist_eastmoney_kline_rows(
                                db,
                                code=code,
                                ts_code=ts_code if code != "HSI" else "HSI",
                                klt=klt,
                                lookback_days=days,
                            )
                            per_code[key] = stats
                            written += int(stats.get("inserted") or 0)
                            rows += int(stats.get("rows") or 0)
                        except Exception as e:
                            errors[f"{code}:{key}"] = str(e)
                    details[code] = per_code

                status = "success" if not errors else ("partial" if written else "failed")
                summary = {
                    "enabled": True,
                    "source": "EASTMONEY",
                    "rows": rows,
                    "inserted": written,
                    "lookback_days_1m": lookback_days_1m,
                    "lookback_days_5m": lookback_days_5m,
                    "details": details,
                    "errors": errors,
                }

        elif job_name == "backfill_hsi_am_from_kline":
            # Aggregate HSI AM turnover from persisted kline rows (index_kline_source_record)
            idx = ensure_market_index(db, "HSI")

            date_from = None
            date_to = None
            if params and params.get("date_from"):
                date_from = date.fromisoformat(str(params.get("date_from")).strip())
            if params and params.get("date_to"):
                date_to = date.fromisoformat(str(params.get("date_to")).strip())

            q = (
                db.query(IndexKlineSourceRecord)
                .filter(IndexKlineSourceRecord.index_id == idx.id)
                .filter(IndexKlineSourceRecord.source == "EASTMONEY")
                .filter(IndexKlineSourceRecord.interval == KlineInterval.M5)
                .filter(IndexKlineSourceRecord.ok.is_(True))
            )
            if date_from is not None:
                q = q.filter(IndexKlineSourceRecord.trade_date >= date_from)
            if date_to is not None:
                q = q.filter(IndexKlineSourceRecord.trade_date <= date_to)
            rows = q.order_by(IndexKlineSourceRecord.trade_date.asc(), IndexKlineSourceRecord.bar_time.asc()).all()

            by_day: dict[date, list[IndexKlineSourceRecord]] = {}
            for r in rows:
                by_day.setdefault(r.trade_date, []).append(r)

            cutoff = time(12, 30)
            updated = 0
            skipped = 0
            details: dict[str, dict] = {}

            for d, bars in sorted(by_day.items()):
                # skip if already has AM history with turnover
                existing = (
                    db.query(IndexQuoteHistory)
                    .filter(IndexQuoteHistory.index_id == idx.id)
                    .filter(IndexQuoteHistory.trade_date == d)
                    .filter(IndexQuoteHistory.session == SessionType.AM)
                    .one_or_none()
                )
                if existing is not None and existing.turnover_amount is not None:
                    skipped += 1
                    continue

                am_amount = 0
                have_amount = False
                am_close = None
                am_asof = None

                for r in bars:
                    bt = r.bar_time
                    try:
                        bt_local = bt.astimezone(timezone(timedelta(hours=8)))
                    except Exception:
                        bt_local = bt
                    if bt_local.time() <= cutoff:
                        if r.turnover_amount is not None:
                            am_amount += int(r.turnover_amount)
                            have_amount = True
                        if r.close is not None:
                            am_close = int(r.close)
                        am_asof = bt

                if not have_amount or am_close is None:
                    continue

                add_index_source_record(
                    db,
                    index_id=idx.id,
                    trade_date=d,
                    session=SessionType.AM,
                    source="EASTMONEY",
                    last=am_close,
                    change_points=None,
                    change_pct=None,
                    turnover_amount=am_amount,
                    turnover_currency="HKD",
                    asof_ts=am_asof,
                    payload={"from": "index_kline_source_record", "interval": "5m", "cutoff": "12:30"},
                    ok=True,
                )
                fact = upsert_index_history_from_sources(db, index_id=idx.id, trade_date=d, session=SessionType.AM)
                if fact is not None:
                    updated += 1
                    details[str(d)] = {"turnover_amount": am_amount}

            status = "success"
            summary = {"updated": updated, "skipped": skipped, "days": len(by_day), "details": details}

        elif job_name == "backfill_hsi_am_yesterday":

            # Backfill yesterday HSI AM turnover snapshot from Eastmoney minute kline (secid=100.HSI)
            from datetime import date as _date

            trade_date = _date.today() - timedelta(days=1)
            if params and params.get("trade_date"):
                trade_date = _date.fromisoformat(str(params.get("trade_date")).strip())

            beg = trade_date.strftime("%Y%m%d")
            end = beg
            # NOTE: For HSI, Eastmoney klt=1 often only returns the latest trading day.
            # Use 5-minute bars to reliably cover the previous day.
            bars = fetch_minute_kline(ts_code="HSI", lookback_days=2, timeout_seconds=settings.HKEX_TIMEOUT_SECONDS, klt="5", beg=beg, end=end)

            cutoff = time(12, 30)
            am_amount = 0.0
            have_amount = False
            am_close = None
            am_asof = None
            for bar in bars:
                if bar.trade_date != trade_date:
                    continue
                if bar.dt.time() <= cutoff:
                    if bar.amount is not None:
                        am_amount += float(bar.amount)
                        have_amount = True
                    am_close = bar.close
                    am_asof = bar.dt

            if not have_amount:
                raise RuntimeError("Eastmoney HSI AM: no amount rows")
            if am_asof is None:
                # fallback use cutoff timestamp
                am_asof = datetime.combine(trade_date, cutoff)

            index_row = ensure_market_index(db, "HSI")
            upsert_realtime_snapshot(
                db,
                index_id=index_row.id,
                trade_date=trade_date,
                session=SessionType.AM,
                last=int(round(float(am_close) * 100)) if am_close is not None else 0,
                change_points=None,
                change_pct=None,
                turnover_amount=int(round(am_amount)),
                turnover_currency="HKD",
                data_updated_at=am_asof.replace(tzinfo=timezone(timedelta(hours=8))),
                is_closed=True,
                source="EASTMONEY",
                payload={"klt": "5", "beg": beg, "end": end, "cutoff": "12:30", "bars": len(bars)},
            )

            status = "success"
            summary = {"trade_date": str(trade_date), "turnover_amount": int(round(am_amount)), "bars": len(bars), "source": "EASTMONEY"}

        else:
            raise ValueError(f"Unknown job_name: {job_name}")

        run.summary = summary
        run.status = status
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(run)
        return run

    except Exception as e:
        run.status = "failed"
        run.error = f"{e}\n\n{traceback.format_exc()}"
        run.finished_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(run)
        return run
