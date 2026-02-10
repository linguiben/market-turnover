from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

import httpx


@dataclass
class EastmoneyMinuteBar:
    trade_date: date
    dt: datetime
    close: float
    amount: float | None  # 成交额 (yuan)
    raw: str


def _secid_from_ts_code(ts_code: str) -> str:
    """Convert ts_code like 000001.SH / 399001.SZ to Eastmoney secid."""

    ts_code = ts_code.strip().upper()
    if ts_code.endswith(".SH"):
        return "1." + ts_code.split(".")[0]
    if ts_code.endswith(".SZ"):
        return "0." + ts_code.split(".")[0]
    raise ValueError(f"Unsupported ts_code for Eastmoney: {ts_code}")


def _parse_kline_rows(rows: list[str]) -> list[EastmoneyMinuteBar]:
    out: list[EastmoneyMinuteBar] = []
    for row in rows:
        # Typical format (klt=1):
        # "YYYY-MM-DD HH:MM,open,close,high,low,vol,amount,..."
        parts = str(row).split(",")
        if not parts:
            continue
        dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
        close = float(parts[2])
        amount = None
        # With fields2=f51..f58, amount is usually the 7th column (index 6).
        if len(parts) >= 7 and parts[6] not in (None, ""):
            try:
                amount = float(parts[6])
            except Exception:
                amount = None
        out.append(EastmoneyMinuteBar(trade_date=dt.date(), dt=dt, close=close, amount=amount, raw=row))
    return out


def fetch_minute_kline(
    *,
    ts_code: str,
    lookback_days: int = 30,
    timeout_seconds: int = 20,
) -> list[EastmoneyMinuteBar]:
    """Fetch minute kline for an index from Eastmoney public API.

    Endpoint: https://push2his.eastmoney.com/api/qt/stock/kline/get
    Params:
      - secid: 1.000001 / 0.399001
      - klt=1 (1 minute)
      - fqt=0
      - beg/end: YYYYMMDD
      - fields2: include amount

    Returns minute bars (best-effort)."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    secid = _secid_from_ts_code(ts_code)
    beg = (date.today() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    end = date.today().strftime("%Y%m%d")

    params = {
        "secid": secid,
        "klt": "1",  # 1-minute
        "fqt": "0",
        "beg": beg,
        "end": end,
        # Eastmoney requires `ut` + fields to return kline data reliably.
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        # fields2 controls the kline string columns
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
    }

    with httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"}) as client:
        resp = client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params)
        resp.raise_for_status()
        data = resp.json()

    raw_rows = (((data or {}).get("data") or {}).get("klines")) or []
    return _parse_kline_rows(raw_rows)


def aggregate_halfday_and_fullday_amount(
    *,
    bars: list[EastmoneyMinuteBar],
    am_end: time = time(11, 30),
) -> dict[date, dict]:
    """Aggregate minute bars into per-day AM and FULL turnover.

    - AM: sum(amount) for bars with time <= 11:30
    - FULL: sum(amount) for all bars that day

    Also returns am_close (last close <= 11:30) and full_close (last close)."""

    by_day: dict[date, list[EastmoneyMinuteBar]] = {}
    for bar in bars:
        by_day.setdefault(bar.trade_date, []).append(bar)

    out: dict[date, dict] = {}
    for d, day_bars in by_day.items():
        day_bars.sort(key=lambda x: x.dt)

        am_amount = 0.0
        full_amount = 0.0
        am_close = None
        full_close = None

        for bar in day_bars:
            if bar.amount is not None:
                full_amount += float(bar.amount)
                if bar.dt.time() <= am_end:
                    am_amount += float(bar.amount)
            if bar.dt.time() <= am_end:
                am_close = bar.close
            full_close = bar.close

        out[d] = {
            "am_amount": int(round(am_amount)) if am_amount > 0 else None,
            "full_amount": int(round(full_amount)) if full_amount > 0 else None,
            "am_close": am_close,
            "full_close": full_close,
            "bars": len(day_bars),
        }

    return out
