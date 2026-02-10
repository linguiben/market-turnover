from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

import httpx


@dataclass
class TushareIndexDaily:
    code: str
    ts_code: str
    trade_date: date
    close: float
    change: float | None
    pct_chg: float | None
    turnover_amount: int | None
    turnover_unit: str
    volume: float | None
    raw: dict


def _parse_trade_date(raw_value: str) -> date:
    return datetime.strptime(str(raw_value), "%Y%m%d").date()


def _request_tushare(
    *,
    base_url: str,
    token: str,
    api_name: str,
    params: dict,
    fields: str,
    timeout_seconds: int,
) -> list[dict]:
    payload = {
        "api_name": api_name,
        "token": token,
        "params": params,
        "fields": fields,
    }
    with httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"}) as client:
        response = client.post(base_url, json=payload)
        response.raise_for_status()
        data = response.json()

    if data.get("code") != 0:
        raise RuntimeError(f"Tushare API error: {data.get('msg') or data.get('code')}")

    body = data.get("data") or {}
    fields_out = body.get("fields") or []
    items = body.get("items") or []
    return [dict(zip(fields_out, row)) for row in items]


def _build_daily_row(*, code: str, ts_code: str, raw_row: dict) -> TushareIndexDaily:
    trade_date = _parse_trade_date(str(raw_row.get("trade_date")))
    close = float(raw_row.get("close"))
    change = float(raw_row["change"]) if raw_row.get("change") is not None else None
    pct_chg = float(raw_row["pct_chg"]) if raw_row.get("pct_chg") is not None else None

    amount = raw_row.get("amount")
    turnover_amount = int(round(float(amount) * 1000)) if amount is not None else None
    volume = float(raw_row["vol"]) if raw_row.get("vol") is not None else None

    return TushareIndexDaily(
        code=code.upper(),
        ts_code=ts_code.upper(),
        trade_date=trade_date,
        close=close,
        change=change,
        pct_chg=pct_chg,
        turnover_amount=turnover_amount,
        turnover_unit="thousand",
        volume=volume,
        raw=raw_row,
    )


def fetch_index_daily_history(
    *,
    token: str,
    index_map: dict[str, str],
    base_url: str = "https://api.tushare.pro",
    timeout_seconds: int = 15,
    lookback_days: int = 90,
) -> list[TushareIndexDaily]:
    if not token:
        raise ValueError("tushare token is empty")
    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    start_date = (date.today() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end_date = date.today().strftime("%Y%m%d")
    # index_daily provides amount/vol; index_global typically provides only price/change.
    fields_index_daily = "ts_code,trade_date,close,change,pct_chg,amount,vol"
    fields_index_global = "ts_code,trade_date,close,change,pct_chg"
    results: list[TushareIndexDaily] = []

    for code, ts_code in index_map.items():
        ts_code = ts_code.strip()
        # CN indices (e.g. 000001.SH) use index_daily; global indices like HSI use index_global.
        if "." in ts_code:
            api_name = "index_daily"
            params = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
            fields = fields_index_daily
        else:
            api_name = "index_global"
            params = {"ts_code": ts_code, "start_date": start_date, "end_date": end_date}
            fields = fields_index_global

        rows = _request_tushare(
            base_url=base_url,
            token=token,
            api_name=api_name,
            params=params,
            fields=fields,
            timeout_seconds=timeout_seconds,
        )
        if not rows:
            continue

        # Keep one row per trade_date.
        latest_per_day: dict[date, dict] = {}
        for raw_row in rows:
            d = _parse_trade_date(str(raw_row.get("trade_date")))
            latest_per_day[d] = raw_row

        for trade_date in sorted(latest_per_day):
            results.append(_build_daily_row(code=code, ts_code=ts_code, raw_row=latest_per_day[trade_date]))

    return results


def fetch_latest_index_daily(
    *,
    token: str,
    index_map: dict[str, str],
    base_url: str = "https://api.tushare.pro",
    timeout_seconds: int = 15,
    lookback_days: int = 20,
) -> list[TushareIndexDaily]:
    if not token:
        raise ValueError("tushare token is empty")

    results: list[TushareIndexDaily] = []
    history_rows = fetch_index_daily_history(
        token=token,
        index_map=index_map,
        base_url=base_url,
        timeout_seconds=timeout_seconds,
        lookback_days=lookback_days,
    )
    by_code: dict[str, TushareIndexDaily] = {}
    for row in history_rows:
        existing = by_code.get(row.code)
        if existing is None or row.trade_date > existing.trade_date:
            by_code[row.code] = row
    for code in index_map:
        item = by_code.get(code.upper())
        if item is not None:
            results.append(item)

    return results


def daily_row_asof(value_date: date) -> datetime:
    return datetime.combine(value_date, time(16, 0), tzinfo=timezone(timedelta(hours=8)))
