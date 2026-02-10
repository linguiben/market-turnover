from __future__ import annotations

import traceback
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from datetime import date, time

from app.config import settings
from app.db.models import IndexQuoteSourceRecord, JobRun, HsiQuoteFact, SessionType, TurnoverSourceRecord
from app.services.index_quote_resolver import (
    add_index_source_record,
    ensure_market_index,
    upsert_index_history_from_sources,
    upsert_realtime_snapshot,
)
from app.services.resolver import upsert_fact_from_sources
from app.sources.hkex import fetch_hkex_latest_table
from app.sources.aastocks import fetch_midday_turnover
from app.sources.aastocks_index import fetch_hsi_snapshot
from app.sources.tushare_index import TushareIndexDaily, daily_row_asof, fetch_index_daily_history, fetch_latest_index_daily
from app.sources.tencent_index import fetch_index_daily_history as fetch_tencent_index_daily_history
from app.sources.eastmoney_index import fetch_minute_kline, aggregate_halfday_and_fullday_amount


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


def run_job(db: Session, job_name: str) -> JobRun:
    run = JobRun(job_name=job_name, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        if job_name == "fetch_am":
            # Use HK timezone trading date when possible; fallback to local date.
            today = date.today()

            # 1) Turnover (best-effort)
            try:
                mid = fetch_midday_turnover()
                t_source = "AASTOCKS"
                turnover = mid.turnover_hkd
                asof = mid.asof
                if asof is not None:
                    today = asof.astimezone(timezone(timedelta(hours=8))).date()
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
                    today = snap.asof.astimezone(timezone(timedelta(hours=8))).date()
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
            rows = []
            try:
                rows = fetch_hkex_latest_table()
            except Exception:
                rows = []

            inserted = 0
            updated = 0

            if rows:
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

                for r in rows[-80:]:
                    if upsert_fact_from_sources(db, r.trade_date, SessionType.FULL):
                        updated += 1

                summary = {"mode": "hkex", "rows": len(rows), "inserted": inserted, "facts_updated": updated}
                status = "success"

            else:
                # POC seed: create last ~35 weekdays FULL turnover so UI has data.
                from datetime import timedelta
                import random

                random.seed(42)
                d = date.today()
                seeded = 0
                while seeded < 35:
                    if d.weekday() < 5:
                        turnover = random.randint(140_000_000_000, 360_000_000_000)
                        rec = TurnoverSourceRecord(
                            trade_date=d,
                            session=SessionType.FULL,
                            source="MOCK",
                            turnover_hkd=turnover,
                            payload={"seed": True},
                            ok=True,
                        )
                        db.add(rec)
                        db.commit()
                        upsert_fact_from_sources(db, d, SessionType.FULL)
                        seeded += 1
                    d = d - timedelta(days=1)

                summary = {"mode": "mock_seed", "seeded_days": seeded}
                status = "partial"

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
                    today = asof.astimezone(timezone(timedelta(hours=8))).date()
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

        elif job_name == "backfill_tushare_index":
            ts_status, ts_summary = _backfill_tushare_index_quotes(db, lookback_days=90)
            status = "success" if ts_status in {"success", "skipped"} else "partial"
            summary = {"tushare": ts_summary}

        elif job_name == "backfill_cn_halfday":
            em_status, em_summary = _backfill_eastmoney_cn_halfday(db, lookback_days=90)
            status = "success" if em_status in {"success", "skipped"} else "partial"
            summary = {"eastmoney": em_summary}

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
