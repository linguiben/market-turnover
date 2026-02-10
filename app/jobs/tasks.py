from __future__ import annotations

import traceback
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from datetime import date

from app.db.models import JobRun, HsiQuoteFact, SessionType, TurnoverSourceRecord
from app.services.resolver import upsert_fact_from_sources
from app.sources.hkex import fetch_hkex_latest_table
from app.sources.aastocks import fetch_midday_turnover
from app.sources.aastocks_index import fetch_hsi_snapshot


def run_job(db: Session, job_name: str) -> JobRun:
    run = JobRun(job_name=job_name, status="running")
    db.add(run)
    db.commit()
    db.refresh(run)

    try:
        if job_name == "fetch_am":
            today = date.today()

            # 1) Turnover (best-effort)
            try:
                mid = fetch_midday_turnover()
                t_source = "AASTOCKS"
                turnover = mid.turnover_hkd
                asof = mid.asof
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
            summary = {
                "today": str(today),
                "turnover_hkd": turnover,
                "turnover_source": t_source,
                "turnover_asof": str(asof),
                "hsi_status": h_status,
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
            today = date.today()

            # For POC: use the same AASTOCKS index feed turnover as end-of-day turnover proxy.
            # (Not official; HKEX backfill remains the official baseline when available.)
            try:
                snap = fetch_hsi_snapshot()
                turnover = snap.turnover_hkd
                source = "AASTOCKS"
                asof = snap.asof
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

            summary = {"today": str(today), "turnover_hkd": turnover, "source": source, "asof": str(asof)}

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
