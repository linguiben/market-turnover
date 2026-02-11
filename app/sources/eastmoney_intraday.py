from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

import httpx


def _secid_from_ts_code(ts_code: str) -> str:
    ts_code = ts_code.strip().upper()
    if ts_code.endswith(".SH"):
        return "1." + ts_code.split(".")[0]
    if ts_code.endswith(".SZ"):
        return "0." + ts_code.split(".")[0]
    raise ValueError(f"Unsupported ts_code for Eastmoney: {ts_code}")


@dataclass
class EastmoneyIntradaySnapshot:
    trade_date: date
    asof: datetime
    last: float
    change: float | None
    pct_chg: float | None
    amount: float | None  # yuan
    volume: float | None
    raw: dict


def fetch_intraday_snapshot(
    *,
    ts_code: str,
    timeout_seconds: int = 15,
) -> EastmoneyIntradaySnapshot:
    """Fetch latest intraday snapshot using Eastmoney minute kline endpoint.

    Uses klt=1 and beg=end=today to get today's 1-min klines; uses the last bar.
    preKPrice from response is used to compute change/pct.
    """

    secid = _secid_from_ts_code(ts_code)
    today = date.today().strftime("%Y%m%d")

    params = {
        "secid": secid,
        "klt": "1",
        "fqt": "0",
        "beg": today,
        "end": today,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
    }

    with httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"}) as client:
        resp = client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params)
        resp.raise_for_status()
        data = resp.json()

    node = (data or {}).get("data") or {}
    klines = node.get("klines") or []
    if not klines:
        raise RuntimeError("Eastmoney intraday: empty klines")

    last_row = str(klines[-1])
    parts = last_row.split(",")
    # dt,open,close,high,low,vol,amount,...
    asof = datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
    last = float(parts[2])
    volume = float(parts[5]) if len(parts) > 5 and parts[5] else None
    amount = float(parts[6]) if len(parts) > 6 and parts[6] else None

    pre_close = node.get("preKPrice")
    change = None
    pct = None
    try:
        if pre_close is not None:
            pre_close = float(pre_close)
            change = last - pre_close
            if pre_close != 0:
                pct = (change / pre_close) * 100
    except Exception:
        change = None
        pct = None

    return EastmoneyIntradaySnapshot(
        trade_date=asof.date(),
        asof=asof,
        last=last,
        change=change,
        pct_chg=pct,
        amount=amount,
        volume=volume,
        raw={"resp": data, "row": last_row},
    )
