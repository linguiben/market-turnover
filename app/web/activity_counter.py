from __future__ import annotations

from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy.orm import Session


def increment_activity_counter(db: Session, *, event: str, at: datetime | None = None) -> None:
    """Increment global counters in user_activity_counter and user_activity_counter_daily.

    event: "visit" or "login"
    """
    if event not in {"visit", "login"}:
        return

    ts = at or datetime.now(timezone.utc)
    day = ts.date()
    visit_inc = 1 if event == "visit" else 0
    login_inc = 1 if event == "login" else 0

    try:
        # Ensure singleton row exists.
        db.execute(
            sa.text(
                """
                INSERT INTO user_activity_counter
                  (id, visit_count, login_count, last_visit_at, last_login_at, created_at, updated_at)
                VALUES
                  (1, 0, 0, NULL, NULL, :ts, :ts)
                ON CONFLICT (id) DO NOTHING
                """
            ),
            {"ts": ts},
        )

        if event == "visit":
            db.execute(
                sa.text(
                    """
                    UPDATE user_activity_counter
                    SET visit_count = visit_count + 1,
                        last_visit_at = :ts,
                        updated_at = :ts
                    WHERE id = 1
                    """
                ),
                {"ts": ts},
            )
        else:
            db.execute(
                sa.text(
                    """
                    UPDATE user_activity_counter
                    SET login_count = login_count + 1,
                        last_login_at = :ts,
                        updated_at = :ts
                    WHERE id = 1
                    """
                ),
                {"ts": ts},
            )

        db.execute(
            sa.text(
                """
                INSERT INTO user_activity_counter_daily
                  (stat_date, visit_count, login_count, created_at, updated_at)
                VALUES
                  (:day, :visit_inc, :login_inc, :ts, :ts)
                ON CONFLICT (stat_date) DO UPDATE
                SET visit_count = user_activity_counter_daily.visit_count + EXCLUDED.visit_count,
                    login_count = user_activity_counter_daily.login_count + EXCLUDED.login_count,
                    updated_at = EXCLUDED.updated_at
                """
            ),
            {
                "day": day,
                "visit_inc": visit_inc,
                "login_inc": login_inc,
                "ts": ts,
            },
        )
    except Exception:
        # Keep request flow resilient if table is not ready.
        db.rollback()


def get_global_visited_count(db: Session) -> int:
    """Homepage display value: global visit count."""
    try:
        value = db.execute(sa.text("SELECT visit_count FROM user_activity_counter WHERE id = 1")).scalar()
        return int(value or 0)
    except Exception:
        db.rollback()
        try:
            fallback = db.execute(sa.text("SELECT COUNT(*) FROM user_visit_logs WHERE action_type = 'visit'")).scalar()
            return int(fallback or 0)
        except Exception:
            db.rollback()
            return 0
