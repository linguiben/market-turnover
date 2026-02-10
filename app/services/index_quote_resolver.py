from __future__ import annotations

from datetime import date, datetime

from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import (
    IndexQuoteHistory,
    IndexQuoteSourceRecord,
    IndexRealtimeSnapshot,
    MarketIndex,
    Quality,
    SessionType,
)


INDEX_META = {
    "HSI": {"name_zh": "恒生指数", "name_en": "Hang Seng Index", "market": "HK", "exchange": "HKEX", "currency": "HKD", "timezone": "Asia/Hong_Kong", "display_order": 10},
    "SSE": {"name_zh": "上证指数", "name_en": "SSE Composite Index", "market": "CN", "exchange": "SSE", "currency": "CNY", "timezone": "Asia/Shanghai", "display_order": 20},
    "SZSE": {"name_zh": "深证成指", "name_en": "SZSE Component Index", "market": "CN", "exchange": "SZSE", "currency": "CNY", "timezone": "Asia/Shanghai", "display_order": 30},
}


def ensure_market_index(db: Session, code: str) -> MarketIndex:
    upper_code = code.upper()
    existing = db.query(MarketIndex).filter(MarketIndex.code == upper_code).one_or_none()
    if existing is not None:
        return existing

    meta = INDEX_META.get(
        upper_code,
        {
            "name_zh": upper_code,
            "name_en": upper_code,
            "market": "UNKNOWN",
            "exchange": "UNKNOWN",
            "currency": "HKD",
            "timezone": "Asia/Shanghai",
            "display_order": 100,
        },
    )
    row = MarketIndex(code=upper_code, is_active=True, **meta)
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def add_index_source_record(
    db: Session,
    *,
    index_id: int,
    trade_date: date,
    session: SessionType,
    source: str,
    last: int | None,
    change_points: int | None,
    change_pct: int | None,
    turnover_amount: int | None,
    turnover_currency: str | None,
    asof_ts: datetime | None,
    payload: dict | None,
    ok: bool = True,
    error: str | None = None,
) -> IndexQuoteSourceRecord:
    row = IndexQuoteSourceRecord(
        index_id=index_id,
        trade_date=trade_date,
        session=session,
        source=source,
        last=last,
        change_points=change_points,
        change_pct=change_pct,
        turnover_amount=turnover_amount,
        turnover_currency=turnover_currency,
        asof_ts=asof_ts,
        payload=payload,
        ok=ok,
        error=error,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def upsert_index_history_from_sources(
    db: Session,
    *,
    index_id: int,
    trade_date: date,
    session: SessionType,
) -> IndexQuoteHistory | None:
    priorities = [item.strip().upper() for item in settings.SOURCE_PRIORITY.split(",") if item.strip()]

    records = (
        db.query(IndexQuoteSourceRecord)
        .filter(IndexQuoteSourceRecord.index_id == index_id)
        .filter(IndexQuoteSourceRecord.trade_date == trade_date)
        .filter(IndexQuoteSourceRecord.session == session)
        .filter(IndexQuoteSourceRecord.ok.is_(True))
        .filter(IndexQuoteSourceRecord.last.isnot(None))
        .order_by(IndexQuoteSourceRecord.fetched_at.desc())
        .all()
    )
    if not records:
        return None

    best = None
    for source in priorities:
        for row in records:
            if row.source.upper() == source:
                best = row
                break
        if best is not None:
            break
    if best is None:
        best = records[0]

    index_meta = db.query(MarketIndex).filter(MarketIndex.id == index_id).one()
    turnover_currency = best.turnover_currency or index_meta.currency
    quality = Quality.OFFICIAL if best.source.upper() == "HKEX" else Quality.PROVISIONAL

    fact = (
        db.query(IndexQuoteHistory)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.trade_date == trade_date)
        .filter(IndexQuoteHistory.session == session)
        .one_or_none()
    )

    if fact is None:
        fact = IndexQuoteHistory(
            index_id=index_id,
            trade_date=trade_date,
            session=session,
            last=int(best.last),
            change_points=best.change_points,
            change_pct=best.change_pct,
            turnover_amount=best.turnover_amount,
            turnover_currency=turnover_currency,
            best_source=best.source,
            quality=quality,
            source_count=len(records),
            asof_ts=best.asof_ts,
            payload=best.payload,
        )
        db.add(fact)
    else:
        fact.last = int(best.last)
        fact.change_points = best.change_points
        fact.change_pct = best.change_pct
        fact.turnover_amount = best.turnover_amount
        fact.turnover_currency = turnover_currency
        fact.best_source = best.source
        fact.quality = quality
        fact.source_count = len(records)
        fact.asof_ts = best.asof_ts
        fact.payload = best.payload

    db.commit()
    db.refresh(fact)
    return fact


def upsert_realtime_snapshot(
    db: Session,
    *,
    index_id: int,
    trade_date: date,
    session: SessionType,
    last: int,
    change_points: int | None,
    change_pct: int | None,
    turnover_amount: int | None,
    turnover_currency: str,
    data_updated_at: datetime,
    is_closed: bool,
    source: str,
    payload: dict | None,
) -> IndexRealtimeSnapshot:
    row = (
        db.query(IndexRealtimeSnapshot)
        .filter(IndexRealtimeSnapshot.index_id == index_id)
        .filter(IndexRealtimeSnapshot.trade_date == trade_date)
        .one_or_none()
    )
    if row is None:
        row = IndexRealtimeSnapshot(
            index_id=index_id,
            trade_date=trade_date,
            session=session,
            last=last,
            change_points=change_points,
            change_pct=change_pct,
            turnover_amount=turnover_amount,
            turnover_currency=turnover_currency,
            data_updated_at=data_updated_at,
            is_closed=is_closed,
            source=source,
            payload=payload,
        )
        db.add(row)
    else:
        row.session = session
        row.last = last
        row.change_points = change_points
        row.change_pct = change_pct
        row.turnover_amount = turnover_amount
        row.turnover_currency = turnover_currency
        row.data_updated_at = data_updated_at
        row.is_closed = is_closed
        row.source = source
        row.payload = payload

    db.commit()
    db.refresh(row)
    return row
