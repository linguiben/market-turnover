from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta

import httpx

from app.config import settings


EM_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Referer": "https://quote.eastmoney.com/",
    "Origin": "https://quote.eastmoney.com",
    "Connection": "keep-alive",
}


@dataclass
class EastmoneyMinuteBar:
    trade_date: date
    dt: datetime
    open: float | None
    close: float
    high: float | None
    low: float | None
    volume: float | None
    amount: float | None  # 成交额 (yuan)
    raw: str


@dataclass
class EastmoneyIntradaySnapshot:
    trade_date: date
    asof: datetime
    last: float
    change: float | None
    pct_chg: float | None
    amount: float | None  # 成交额 (yuan)
    volume: float | None
    raw: dict


_SECID_CACHE: dict[str, str] = {}


def _client_kwargs(timeout_seconds: int) -> dict:
    proxy = (settings.EASTMONEY_PROXY_URL or "").strip() or None
    kwargs = {"timeout": timeout_seconds, "headers": EM_HEADERS}
    if proxy:
        kwargs["proxy"] = proxy
    return kwargs


def _secid_from_ts_code(ts_code: str, *, timeout_seconds: int = 20) -> str:
    """Convert symbol to Eastmoney secid.

    Supported:
      - CN index ts_code like 000001.SH / 399001.SZ -> 1.000001 / 0.399001
      - HSI -> resolved via Eastmoney suggest API -> e.g. 100.HSI
    """

    ts_code = ts_code.strip().upper()

    if ts_code in _SECID_CACHE:
        return _SECID_CACHE[ts_code]

    if ts_code.endswith(".SH"):
        secid = "1." + ts_code.split(".")[0]
        _SECID_CACHE[ts_code] = secid
        return secid
    if ts_code.endswith(".SZ"):
        secid = "0." + ts_code.split(".")[0]
        _SECID_CACHE[ts_code] = secid
        return secid

    # HSI (HK index) - resolve by keyword
    if ts_code == "HSI":
        with httpx.Client(**_client_kwargs(timeout_seconds)) as client:
            resp = client.get(
                "https://searchapi.eastmoney.com/api/suggest/get",
                params={"input": "HSI", "type": "14", "count": "10"},
            )
            resp.raise_for_status()
            data = resp.json() or {}
        rows = (((data.get("QuotationCodeTable") or {}).get("Data")) or [])
        for row in rows:
            if (row or {}).get("Code") == "HSI":
                quote_id = (row or {}).get("QuoteID")
                if quote_id:
                    _SECID_CACHE[ts_code] = str(quote_id)
                    return str(quote_id)
        raise ValueError("Eastmoney suggest did not return QuoteID for HSI")

    raise ValueError(f"Unsupported symbol for Eastmoney: {ts_code}")


def _to_float(v: str | None) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _parse_kline_rows(rows: list[str]) -> list[EastmoneyMinuteBar]:
    out: list[EastmoneyMinuteBar] = []
    for row in rows:
        # Typical format:
        # "YYYY-MM-DD HH:MM,open,close,high,low,vol,amount,..."
        parts = str(row).split(",")
        if not parts:
            continue
        dt = datetime.strptime(parts[0], "%Y-%m-%d %H:%M")

        open_ = _to_float(parts[1]) if len(parts) > 1 else None
        close = float(parts[2]) if len(parts) > 2 and parts[2] not in (None, "") else None
        high = _to_float(parts[3]) if len(parts) > 3 else None
        low = _to_float(parts[4]) if len(parts) > 4 else None
        volume = _to_float(parts[5]) if len(parts) > 5 else None
        amount = _to_float(parts[6]) if len(parts) > 6 else None

        if close is None:
            continue

        out.append(
            EastmoneyMinuteBar(
                trade_date=dt.date(),
                dt=dt,
                open=open_,
                close=float(close),
                high=high,
                low=low,
                volume=volume,
                amount=amount,
                raw=row,
            )
        )
    return out


def fetch_minute_kline(
    *,
    ts_code: str,
    lookback_days: int = 30,
    timeout_seconds: int = 20,
    klt: str = "5",
    beg: str | None = None,
    end: str | None = None,
) -> list[EastmoneyMinuteBar]:
    """Fetch intraday kline for an index from Eastmoney public API.

    Endpoint: https://push2his.eastmoney.com/api/qt/stock/kline/get

    Note: 1-minute (`klt=1`) often only returns the latest trading day.
    For historical backfills, use 5-minute bars (`klt=5`, default).

    Params:
      - secid: 1.000001 / 0.399001
      - klt: 1/5/15/30/60 ... (minutes)
      - fqt=0
      - beg/end: YYYYMMDD
      - fields2: include amount

    Returns intraday bars (best-effort)."""

    if lookback_days <= 0:
        raise ValueError("lookback_days must be positive")

    secid = _secid_from_ts_code(ts_code, timeout_seconds=timeout_seconds)
    if beg is None:
        beg = (date.today() - timedelta(days=lookback_days)).strftime("%Y%m%d")
    if end is None:
        end = date.today().strftime("%Y%m%d")

    params = {
        "secid": secid,
        "klt": str(klt),
        "fqt": "0",
        "beg": beg,
        "end": end,
        # Eastmoney requires `ut` + fields to return kline data reliably.
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        # fields2 controls the kline string columns
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58",
    }

    with httpx.Client(**_client_kwargs(timeout_seconds)) as client:
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
