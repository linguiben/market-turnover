from __future__ import annotations

import json
from datetime import date, datetime, time
from zoneinfo import ZoneInfo

import httpx
import sqlalchemy as sa
from sqlalchemy.orm import Session

from app.config import settings
from app.db.models import (
    IndexQuoteHistory,
    IndexRealtimeApiSnapshot,
    IndexRealtimeSnapshot,
    InsightSnapshot,
    InsightSysPrompt,
    MarketIndex,
    SessionType,
    TurnoverFact,
)

TARGET_CODES = ("HSI", "SSE", "SZSE")
PROMPT_KEY = "market_insight"
PROMPT_VERSION = "v1"

FALLBACK_INSIGHT_TEXT = {
    "zh": "当前智能分析暂不可用，系统已切换为基础模式。请稍后刷新重试。",
    "en": "Insights are temporarily unavailable. The system has switched to basic mode. Please refresh later.",
}


def get_fallback_insight_text(lang: str) -> str:
    return FALLBACK_INSIGHT_TEXT.get(lang, FALLBACK_INSIGHT_TEXT["en"])


def _latest_history(
    db: Session,
    *,
    index_id: int,
    session: SessionType,
) -> IndexQuoteHistory | None:
    return (
        db.query(IndexQuoteHistory)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == session)
        .order_by(IndexQuoteHistory.trade_date.desc())
        .first()
    )


def _latest_history_before(
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


def _latest_realtime_today(
    db: Session,
    *,
    index_id: int,
    today: date,
    session: SessionType,
    updated_before: datetime | None = None,
) -> IndexRealtimeSnapshot | None:
    q = (
        db.query(IndexRealtimeSnapshot)
        .filter(IndexRealtimeSnapshot.index_id == index_id)
        .filter(IndexRealtimeSnapshot.trade_date == today)
        .filter(IndexRealtimeSnapshot.session == session)
    )
    if updated_before is not None:
        q = q.filter(IndexRealtimeSnapshot.data_updated_at <= updated_before)
    return q.order_by(IndexRealtimeSnapshot.data_updated_at.desc(), IndexRealtimeSnapshot.id.desc()).first()


def _latest_api_today(
    db: Session,
    *,
    index_id: int,
    today: date,
    session: SessionType,
) -> IndexRealtimeApiSnapshot | None:
    return (
        db.query(IndexRealtimeApiSnapshot)
        .filter(IndexRealtimeApiSnapshot.index_id == index_id)
        .filter(IndexRealtimeApiSnapshot.trade_date == today)
        .filter(IndexRealtimeApiSnapshot.session == session)
        .order_by(IndexRealtimeApiSnapshot.data_updated_at.desc(), IndexRealtimeApiSnapshot.id.desc())
        .first()
    )


def _close_series(db: Session, *, index_id: int, n: int) -> list[int]:
    rows = (
        db.query(IndexQuoteHistory.last)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == SessionType.FULL)
        .order_by(IndexQuoteHistory.trade_date.desc())
        .limit(n)
        .all()
    )
    return [int(v) for (v,) in rows if v is not None]


def _avg(values: list[int]) -> float | None:
    if not values:
        return None
    return round(sum(values) / len(values), 2)


def _turnover_series(db: Session, *, index_id: int, session: SessionType, n: int) -> list[int]:
    rows = (
        db.query(IndexQuoteHistory.turnover_amount)
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == session)
        .filter(IndexQuoteHistory.turnover_amount.isnot(None))
        .order_by(IndexQuoteHistory.trade_date.desc())
        .limit(n)
        .all()
    )
    return [int(v) for (v,) in rows if v is not None]


def _hsi_turnover_peak(db: Session, *, session: SessionType) -> int | None:
    v = (
        db.query(sa.func.max(TurnoverFact.turnover_hkd))
        .filter(TurnoverFact.session == session)
        .scalar()
    )
    return int(v) if v is not None else None


def _index_turnover_peak(db: Session, *, index_id: int, session: SessionType) -> int | None:
    v = (
        db.query(sa.func.max(IndexQuoteHistory.turnover_amount))
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == session)
        .scalar()
    )
    return int(v) if v is not None else None


def _historical_price_high(db: Session, *, index_id: int) -> float | None:
    v = (
        db.query(sa.func.max(IndexQuoteHistory.last))
        .filter(IndexQuoteHistory.index_id == index_id)
        .filter(IndexQuoteHistory.session == SessionType.FULL)
        .scalar()
    )
    if v is None:
        return None
    return round(int(v) / 100.0, 2)


def build_insight_snapshot_payload(db: Session) -> tuple[dict, date, datetime]:
    today = date.today()
    cutoff = datetime.combine(today, time(12, 30), tzinfo=ZoneInfo("Asia/Shanghai"))

    market_indexes = db.query(MarketIndex).filter(MarketIndex.code.in_(TARGET_CODES)).all()
    index_by_code = {row.code.upper(): row for row in market_indexes}

    payload: dict[str, dict] = {}
    asof_points: list[datetime] = []

    for code in TARGET_CODES:
        idx = index_by_code.get(code)
        if idx is None:
            payload[code] = {"missing": True}
            continue

        full_hist = _latest_history(db, index_id=idx.id, session=SessionType.FULL)
        am_hist = _latest_history(db, index_id=idx.id, session=SessionType.AM)
        y_full = _latest_history_before(db, index_id=idx.id, session=SessionType.FULL, before_date=today)
        y_am = _latest_history_before(db, index_id=idx.id, session=SessionType.AM, before_date=today)

        snap_full = _latest_realtime_today(db, index_id=idx.id, today=today, session=SessionType.FULL)
        snap_am = _latest_realtime_today(
            db,
            index_id=idx.id,
            today=today,
            session=SessionType.AM,
            updated_before=cutoff,
        )
        api_full = _latest_api_today(db, index_id=idx.id, today=today, session=SessionType.FULL) if code == "HSI" else None

        full_source = snap_full
        if api_full is not None and (
            full_source is None or api_full.data_updated_at > full_source.data_updated_at
        ):
            full_source = api_full

        if snap_am is None and code == "HSI":
            snap_am = _latest_realtime_today(
                db,
                index_id=idx.id,
                today=today,
                session=SessionType.FULL,
                updated_before=cutoff,
            )

        full_turnover = (
            int(full_source.turnover_amount)
            if full_source is not None and full_source.turnover_amount is not None
            else (int(full_hist.turnover_amount) if full_hist is not None and full_hist.turnover_amount is not None else None)
        )
        am_turnover = (
            int(snap_am.turnover_amount)
            if snap_am is not None and snap_am.turnover_amount is not None
            else (int(am_hist.turnover_amount) if am_hist is not None and am_hist.turnover_amount is not None else None)
        )
        current_price = (
            round(full_source.last / 100.0, 2)
            if full_source is not None and full_source.last is not None
            else (round(full_hist.last / 100.0, 2) if full_hist is not None and full_hist.last is not None else None)
        )

        y_close = round(y_full.last / 100.0, 2) if y_full is not None and y_full.last is not None else None
        y_full_turnover = int(y_full.turnover_amount) if y_full is not None and y_full.turnover_amount is not None else None
        y_am_turnover = int(y_am.turnover_amount) if y_am is not None and y_am.turnover_amount is not None else None

        close5 = _close_series(db, index_id=idx.id, n=5)
        close10 = _close_series(db, index_id=idx.id, n=10)
        am5 = _turnover_series(db, index_id=idx.id, session=SessionType.AM, n=5)
        am10 = _turnover_series(db, index_id=idx.id, session=SessionType.AM, n=10)
        full5 = _turnover_series(db, index_id=idx.id, session=SessionType.FULL, n=5)
        full10 = _turnover_series(db, index_id=idx.id, session=SessionType.FULL, n=10)

        if code == "HSI":
            am_peak = _hsi_turnover_peak(db, session=SessionType.AM)
            full_peak = _hsi_turnover_peak(db, session=SessionType.FULL)
            if not am5:
                rows = (
                    db.query(TurnoverFact.turnover_hkd)
                    .filter(TurnoverFact.session == SessionType.AM)
                    .order_by(TurnoverFact.trade_date.desc())
                    .limit(5)
                    .all()
                )
                am5 = [int(v) for (v,) in rows if v is not None]
            if not am10:
                rows = (
                    db.query(TurnoverFact.turnover_hkd)
                    .filter(TurnoverFact.session == SessionType.AM)
                    .order_by(TurnoverFact.trade_date.desc())
                    .limit(10)
                    .all()
                )
                am10 = [int(v) for (v,) in rows if v is not None]
            if not full5:
                rows = (
                    db.query(TurnoverFact.turnover_hkd)
                    .filter(TurnoverFact.session == SessionType.FULL)
                    .order_by(TurnoverFact.trade_date.desc())
                    .limit(5)
                    .all()
                )
                full5 = [int(v) for (v,) in rows if v is not None]
            if not full10:
                rows = (
                    db.query(TurnoverFact.turnover_hkd)
                    .filter(TurnoverFact.session == SessionType.FULL)
                    .order_by(TurnoverFact.trade_date.desc())
                    .limit(10)
                    .all()
                )
                full10 = [int(v) for (v,) in rows if v is not None]
        else:
            am_peak = _index_turnover_peak(db, index_id=idx.id, session=SessionType.AM)
            full_peak = _index_turnover_peak(db, index_id=idx.id, session=SessionType.FULL)

        updated_at = None
        if full_source is not None:
            updated_at = full_source.data_updated_at
        elif full_hist is not None:
            updated_at = full_hist.asof_ts or full_hist.updated_at
        if updated_at is not None:
            asof_points.append(updated_at)

        payload[code] = {
            "current_price": current_price,
            "historical_price_high": _historical_price_high(db, index_id=idx.id),
            "half_day_turnover": am_turnover,
            "full_day_turnover": full_turnover,
            "half_day_turnover_peak": am_peak,
            "full_day_turnover_peak": full_peak,
            "yesterday_close_price": y_close,
            "yesterday_half_day_turnover": y_am_turnover,
            "yesterday_full_day_turnover": y_full_turnover,
            "avg_5d_close_price": round((_avg(close5) or 0) / 100.0, 2) if close5 else None,
            "high_5d_close_price": round(max(close5) / 100.0, 2) if close5 else None,
            "low_5d_close_price": round(min(close5) / 100.0, 2) if close5 else None,
            "avg_5d_half_day_turnover": int(round(_avg(am5))) if am5 else None,
            "avg_5d_full_day_turnover": int(round(_avg(full5))) if full5 else None,
            "avg_10d_close_price": round((_avg(close10) or 0) / 100.0, 2) if close10 else None,
            "high_10d_close_price": round(max(close10) / 100.0, 2) if close10 else None,
            "low_10d_close_price": round(min(close10) / 100.0, 2) if close10 else None,
            "avg_10d_half_day_turnover": int(round(_avg(am10))) if am10 else None,
            "avg_10d_full_day_turnover": int(round(_avg(full10))) if full10 else None,
            "turnover_currency": idx.currency,
            "last_updated_at": updated_at.isoformat() if updated_at is not None else None,
        }

    asof_ts = max(asof_points) if asof_points else datetime.now(ZoneInfo("Asia/Shanghai"))
    final_payload = {
        "trade_date": today.isoformat(),
        "asof_ts": asof_ts.isoformat(),
        "peak_policy": "all_time",
        "historical_price_high_needs_verification": True,
        "indices": payload,
    }
    return final_payload, today, asof_ts


def get_active_system_prompt(db: Session, *, lang: str) -> InsightSysPrompt | None:
    return (
        db.query(InsightSysPrompt)
        .filter(InsightSysPrompt.lang == lang)
        .filter(InsightSysPrompt.prompt_key == PROMPT_KEY)
        .filter(InsightSysPrompt.is_active.is_(True))
        .order_by(InsightSysPrompt.updated_at.desc(), InsightSysPrompt.id.desc())
        .first()
    )


def compose_user_prompt(*, lang: str, payload: dict) -> str:
    if lang == "zh":
        header = (
            "请基于以下 JSON 快照生成监控导向的市场 Insights。"
            "注意 historical_price_high 可能不完整，需优先联网核验；"
            "若无法核验请明确提示风险。"
        )
    else:
        header = (
            "Generate monitoring-oriented market insights from the JSON snapshot below. "
            "historical_price_high may be incomplete; attempt web verification first. "
            "If not verifiable, explicitly add a risk note."
        )
    return f"{header}\n\nJSON:\n{json.dumps(payload, ensure_ascii=False, indent=2)}"


def _call_openai(*, system_prompt: str, user_prompt: str) -> str:
    api_key = (settings.INSIGHT_OPENAI_API_KEY or "").strip()
    if not api_key:
        raise RuntimeError("INSIGHT_OPENAI_API_KEY is empty")
    url = settings.INSIGHT_OPENAI_BASE_URL.rstrip("/") + "/chat/completions"
    body = {
        "model": settings.INSIGHT_OPENAI_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": settings.INSIGHT_LLM_TEMPERATURE,
        "max_tokens": settings.INSIGHT_LLM_MAX_TOKENS,
    }
    with httpx.Client(timeout=settings.INSIGHT_LLM_TIMEOUT_SECONDS) as client:
        r = client.post(url, headers={"Authorization": f"Bearer {api_key}"}, json=body)
        r.raise_for_status()
        data = r.json()
    return str((((data.get("choices") or [{}])[0]).get("message") or {}).get("content") or "").strip()


def _call_gemini(*, system_prompt: str, user_prompt: str) -> str:
    api_key = (settings.INSIGHT_GEMINI_API_KEY or "").strip()
    if not api_key:
        raise RuntimeError("INSIGHT_GEMINI_API_KEY is empty")
    model = (settings.INSIGHT_GEMINI_MODEL or "").strip()
    if not model:
        raise RuntimeError("INSIGHT_GEMINI_MODEL is empty")
    url = f"{settings.INSIGHT_GEMINI_BASE_URL.rstrip('/')}/models/{model}:generateContent?key={api_key}"
    body = {
        "systemInstruction": {"parts": [{"text": system_prompt}]},
        "contents": [{"parts": [{"text": user_prompt}]}],
        "generationConfig": {
            "temperature": settings.INSIGHT_LLM_TEMPERATURE,
            "maxOutputTokens": settings.INSIGHT_LLM_MAX_TOKENS,
        },
    }
    with httpx.Client(timeout=settings.INSIGHT_LLM_TIMEOUT_SECONDS) as client:
        r = client.post(url, json=body)
        r.raise_for_status()
        data = r.json()
    candidates = data.get("candidates") or []
    if not candidates:
        return ""
    parts = ((candidates[0].get("content") or {}).get("parts") or [])
    text = "".join(str(p.get("text") or "") for p in parts)
    return text.strip()


def call_insight_llm(*, system_prompt: str, user_prompt: str) -> tuple[str, str, str]:
    provider = (settings.INSIGHT_LLM_PROVIDER or "openai").strip().lower()
    if provider == "openai":
        return _call_openai(system_prompt=system_prompt, user_prompt=user_prompt), "openai", settings.INSIGHT_OPENAI_MODEL
    if provider == "gemini":
        return _call_gemini(system_prompt=system_prompt, user_prompt=user_prompt), "gemini", settings.INSIGHT_GEMINI_MODEL
    raise RuntimeError(f"Unsupported INSIGHT_LLM_PROVIDER: {provider}")


def create_insight_snapshot_row(
    db: Session,
    *,
    lang: str,
    payload: dict,
    trade_date: date,
    asof_ts: datetime,
    prompt: str,
    response: str,
    provider: str,
    model: str,
    status: str,
    error_message: str | None,
    prompt_version: str = PROMPT_VERSION,
) -> InsightSnapshot:
    row = InsightSnapshot(
        trade_date=trade_date,
        asof_ts=asof_ts,
        lang=lang,
        peak_policy="all_time",
        provider=provider,
        model=model,
        prompt_version=prompt_version,
        payload=payload,
        prompt=prompt,
        response=response,
        status=status,
        error_message=error_message,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return row


def get_latest_insight_snapshot(db: Session, *, lang: str) -> InsightSnapshot | None:
    return (
        db.query(InsightSnapshot)
        .filter(InsightSnapshot.lang == lang)
        .filter(InsightSnapshot.status.in_(["success", "fallback"]))
        .order_by(InsightSnapshot.created_at.desc(), InsightSnapshot.id.desc())
        .first()
    )
