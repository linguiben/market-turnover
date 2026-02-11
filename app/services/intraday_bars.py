from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from app.db.models import IndexIntradayBar


def _as_tz(dt: datetime, tz: str) -> datetime:
    """Ensure dt is timezone-aware."""

    if dt.tzinfo is not None:
        return dt
    return dt.replace(tzinfo=ZoneInfo(tz))


def upsert_intraday_bar(
    db: Session,
    *,
    index_id: int,
    interval_min: int,
    bar_ts: datetime,
    tz: str,
    open_x100: int | None,
    high_x100: int | None,
    low_x100: int | None,
    close_x100: int,
    volume: int | None,
    amount: int | None,
    currency: str,
    source: str,
    payload: dict | None,
    fetched_at: datetime | None = None,
) -> IndexIntradayBar:
    bar_ts = _as_tz(bar_ts, tz)
    if fetched_at is not None:
        fetched_at = _as_tz(fetched_at, tz)

    row = (
        db.query(IndexIntradayBar)
        .filter(IndexIntradayBar.index_id == index_id)
        .filter(IndexIntradayBar.interval_min == interval_min)
        .filter(IndexIntradayBar.bar_ts == bar_ts)
        .filter(IndexIntradayBar.source == source)
        .one_or_none()
    )

    if row is None:
        row = IndexIntradayBar(
            index_id=index_id,
            interval_min=interval_min,
            bar_ts=bar_ts,
            open=open_x100,
            high=high_x100,
            low=low_x100,
            close=close_x100,
            volume=volume,
            amount=amount,
            currency=currency,
            source=source,
            payload=payload,
        )
        if fetched_at is not None:
            row.fetched_at = fetched_at
        db.add(row)
    else:
        row.open = open_x100
        row.high = high_x100
        row.low = low_x100
        row.close = close_x100
        row.volume = volume
        row.amount = amount
        row.currency = currency
        row.payload = payload
        if fetched_at is not None:
            row.fetched_at = fetched_at

    db.commit()
    db.refresh(row)
    return row
