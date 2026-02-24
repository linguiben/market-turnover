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
    "DJI": {"name_zh": "道琼斯指数", "name_en": "Dow Jones", "market": "US", "exchange": "NYSE", "currency": "USD", "timezone": "America/New_York", "display_order": 40},
    "IXIC": {"name_zh": "纳斯达克", "name_en": "NASDAQ", "market": "US", "exchange": "NASDAQ", "currency": "USD", "timezone": "America/New_York", "display_order": 50},
    "SPX": {"name_zh": "标普500", "name_en": "S&P 500", "market": "US", "exchange": "NYSE", "currency": "USD", "timezone": "America/New_York", "display_order": 55},
    "N225": {"name_zh": "日经225", "name_en": "Nikkei 225", "market": "JP", "exchange": "TSE", "currency": "JPY", "timezone": "Asia/Tokyo", "display_order": 60},
    "FTSE": {"name_zh": "富时100", "name_en": "FTSE 100", "market": "UK", "exchange": "LSE", "currency": "GBP", "timezone": "Europe/London", "display_order": 70},
    "GDAXI": {"name_zh": "德国DAX", "name_en": "DAX", "market": "DE", "exchange": "Xetra", "currency": "EUR", "timezone": "Europe/Berlin", "display_order": 80},
    "CSX5P": {"name_zh": "欧洲斯托克50", "name_en": "Euro Stoxx 50", "market": "EU", "exchange": "EUREX", "currency": "EUR", "timezone": "Europe/Berlin", "display_order": 90},
    "KS11": {"name_zh": "韩国综合指数", "name_en": "KOSPI", "market": "KR", "exchange": "KRX", "currency": "KRW", "timezone": "Asia/Seoul", "display_order": 35},
    # Aliases for display
    "UKX": {"name_zh": "富时100", "name_en": "FTSE 100", "market": "UK", "exchange": "LSE", "currency": "GBP", "timezone": "Europe/London", "display_order": 70},
    "DAX": {"name_zh": "德国DAX", "name_en": "DAX", "market": "DE", "exchange": "Xetra", "currency": "EUR", "timezone": "Europe/Berlin", "display_order": 80},
    "ESTOXX50E": {"name_zh": "欧洲斯托克50", "name_en": "Euro Stoxx 50", "market": "EU", "exchange": "EUREX", "currency": "EUR", "timezone": "Europe/Berlin", "display_order": 90},
    "HS11": {"name_zh": "韩国综合指数", "name_en": "KOSPI", "market": "KR", "exchange": "KRX", "currency": "KRW", "timezone": "Asia/Seoul", "display_order": 35},
}

CODE_ALIASES = {
    # Tushare index_global codes -> dashboard display codes
    "KS11": "HS11",
    "FTSE": "UKX",
    "GDAXI": "DAX",
    "CSX5P": "ESTOXX50E",
}


def normalize_index_code(code: str) -> str:
    upper_code = code.upper()
    return CODE_ALIASES.get(upper_code, upper_code)


def ensure_market_index(db: Session, code: str) -> MarketIndex:
    upper_code = normalize_index_code(code)
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
    """Append-only insert for realtime snapshot.

    Historical snapshots are preserved; homepage should query latest by (index_id, trade_date, id desc).

    Function name kept for backward compatibility with existing call sites.
    """

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
    db.commit()
    db.refresh(row)
    return row
