from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from time import sleep

import tushare as ts


@dataclass
class TushareKlineBar:
    trade_time: datetime
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    vol: float | None
    amount: float | None
    raw: dict


def fetch_index_kline(
    *,
    token: str,
    ts_code: str,
    freq: str = "5min",
    start_date: str | None = None,
    end_date: str | None = None,
    timeout_seconds: int = 15,
    max_retries: int = 2,
    retry_sleep_seconds: float = 1.2,
) -> list[TushareKlineBar]:
    """Fetch index kline bars via Tushare Pro SDK (pro_bar).

    Notes:
      - pro_bar is documented as SDK-only; we already ship tushare in requirements.
      - Tushare has strict rate limits for some symbols (e.g. HSI). This function
        retries a couple times and returns empty on persistent failures.

    Args:
      - ts_code: e.g. 000001.SH / 399001.SZ / HSI
      - freq: '1min' / '5min' / ...
      - start_date/end_date: YYYYMMDD

    Returns a list of bars (descending by trade_time in tushare output; we re-sort ascending).
    """

    if not token:
        raise ValueError("tushare token is empty")

    # SDK uses global socket timeout; keep best-effort.
    pro = ts.pro_api(token)

    last_err: Exception | None = None
    for i in range(max_retries + 1):
        try:
            df = ts.pro_bar(
                api=pro,
                ts_code=str(ts_code).strip().upper(),
                asset="I",
                freq=freq,
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or df.empty:
                return []

            # trade_time like '2026-02-10 15:00:00'
            rows = df.to_dict(orient="records")
            out: list[TushareKlineBar] = []
            for r in rows:
                tt = r.get("trade_time")
                if not tt:
                    continue
                trade_time = datetime.strptime(str(tt), "%Y-%m-%d %H:%M:%S")
                out.append(
                    TushareKlineBar(
                        trade_time=trade_time,
                        open=float(r["open"]) if r.get("open") is not None else None,
                        high=float(r["high"]) if r.get("high") is not None else None,
                        low=float(r["low"]) if r.get("low") is not None else None,
                        close=float(r["close"]) if r.get("close") is not None else None,
                        vol=float(r["vol"]) if r.get("vol") is not None else None,
                        amount=float(r["amount"]) if r.get("amount") is not None else None,
                        raw=r,
                    )
                )

            out.sort(key=lambda x: x.trade_time)
            return out
        except Exception as e:
            last_err = e
            if i < max_retries:
                sleep(retry_sleep_seconds)

    # bubble up last error
    raise RuntimeError(str(last_err) if last_err else "tushare kline fetch failed")
