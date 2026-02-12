from __future__ import annotations

from datetime import datetime, timezone

from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from app.db.models import AppCache


def upsert_cache(db: Session, *, key: str, payload: object | None) -> None:
    stmt = pg_insert(AppCache).values(key=key, payload=payload)
    stmt = stmt.on_conflict_do_update(
        index_elements=[AppCache.key],
        set_={"payload": payload, "updated_at": datetime.now(timezone.utc)},
    )
    db.execute(stmt)
    db.commit()


def get_cache(db: Session, *, key: str) -> AppCache | None:
    return db.query(AppCache).filter(AppCache.key == key).first()
