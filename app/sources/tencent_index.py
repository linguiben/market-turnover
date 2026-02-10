from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import httpx


@dataclass
class TencentIndexDaily:
    code: str          # logical code: SSE/SZSE
    symbol: str        # tencent symbol: sh000001
    trade_date: date
    close: float
    change: float | None
    pct_chg: float | None
    volume: float | None
    raw: dict


def _symbol_from_ts_code(ts_code: str) -> str:
    ts_code = ts_code.strip().upper()
    if ts_code.endswith(".SH"):
        return "sh" + ts_code.split(".")[0]
    if ts_code.endswith(".SZ"):
        return "sz" + ts_code.split(".")[0]
    raise ValueError(f"Unsupported ts_code for Tencent kline: {ts_code}")


def fetch_index_daily_history(
    *,
    index_map: dict[str, str],
    lookback_days: int = 15,
    timeout_seconds: int = 15,
) -> list[TencentIndexDaily]:
    """Fetch CN index kline data from Tencent (web.ifzq.gtimg.cn).

    Returns daily bars; change/pct_chg are computed from previous close.

    API: https://web.ifzq.gtimg.cn/appstock/app/fqkline/get
    param format: <symbol>,day,<start>,<end>,640,qfq
    day row format: [YYYY-MM-DD, open, close, high, low, volume]
    """

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    start = (date.today() - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    end = date.today().strftime("%Y-%m-%d")

    results: list[TencentIndexDaily] = []

    with httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"}) as client:
        for code, ts_code in index_map.items():
            # Only CN indices have a Tencent symbol. Skip others like HSI.
            try:
                symbol = _symbol_from_ts_code(ts_code)
            except Exception:
                continue

            params = {"param": f"{symbol},day,{start},{end},640,qfq"}
            resp = client.get("https://web.ifzq.gtimg.cn/appstock/app/fqkline/get", params=params)
            resp.raise_for_status()
            data = resp.json()

            if data.get("code") != 0:
                raise RuntimeError(f"Tencent kline error: {data.get('msg') or data.get('code')}")

            node = (data.get("data") or {}).get(symbol) or {}
            day_rows = node.get("day") or []
            if not day_rows:
                continue

            prev_close: float | None = None
            for row in day_rows:
                # row: [date, open, close, high, low, volume]
                d = datetime.strptime(str(row[0]), "%Y-%m-%d").date()
                close = float(row[2])
                vol = float(row[5]) if len(row) > 5 and row[5] is not None else None

                change = None
                pct = None
                if prev_close is not None and prev_close != 0:
                    change = close - prev_close
                    pct = (change / prev_close) * 100

                results.append(
                    TencentIndexDaily(
                        code=code.upper(),
                        symbol=symbol,
                        trade_date=d,
                        close=close,
                        change=change,
                        pct_chg=pct,
                        volume=vol,
                        raw={"row": row, "symbol": symbol},
                    )
                )
                prev_close = close

    return results
