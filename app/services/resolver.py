from __future__ import annotations

from datetime import date, time

from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import Quality, SessionType, TurnoverFact, TurnoverSourceRecord


def upsert_fact_from_sources(
    db: Session,
    trade_date: date,
    session_type: SessionType,
    cutoff_time: time | None = None,
) -> TurnoverFact | None:
    """Pick best available source record according to SOURCE_PRIORITY.

    MVP: choose first ok record with non-null turnover.
    """
    priorities = [s.strip().upper() for s in settings.SOURCE_PRIORITY.split(",") if s.strip()]

    q = (
        db.query(TurnoverSourceRecord)
        .filter(TurnoverSourceRecord.trade_date == trade_date)
        .filter(TurnoverSourceRecord.session == session_type)
        .filter(TurnoverSourceRecord.ok.is_(True))
        .filter(TurnoverSourceRecord.turnover_hkd.isnot(None))
        .order_by(TurnoverSourceRecord.fetched_at.desc())
    )
    records = q.all()
    if not records:
        return None

    best = None
    for src in priorities:
        for r in records:
            if r.source.upper() == src:
                best = r
                break
        if best:
            break
    if best is None:
        best = records[0]

    quality = Quality.OFFICIAL if best.source.upper() == "HKEX" else Quality.PROVISIONAL

    fact = (
        db.query(TurnoverFact)
        .filter(TurnoverFact.trade_date == trade_date)
        .filter(TurnoverFact.session == session_type)
        .one_or_none()
    )

    if fact is None:
        fact = TurnoverFact(
            trade_date=trade_date,
            session=session_type,
            turnover_hkd=int(best.turnover_hkd),
            cutoff_time=cutoff_time,
            best_source=best.source,
            quality=quality,
            is_half_day_market=False,
        )
        db.add(fact)
    else:
        fact.turnover_hkd = int(best.turnover_hkd)
        fact.cutoff_time = cutoff_time
        fact.best_source = best.source
        fact.quality = quality

    db.commit()
    db.refresh(fact)
    return fact
