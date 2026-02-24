from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

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

_SECID_CACHE: dict[str, str] = {}


def _client_kwargs(timeout_seconds: int) -> dict:
    proxy = (settings.EASTMONEY_PROXY_URL or "").strip() or None
    kwargs = {"timeout": timeout_seconds, "headers": EM_HEADERS}
    if proxy:
        kwargs["proxy"] = proxy
    return kwargs


def _secid_from_ts_code(ts_code: str, *, timeout_seconds: int = 15) -> str:
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

    raise ValueError(f"Unsupported ts_code for Eastmoney: {ts_code}")


@dataclass
class EastmoneyIntradaySnapshot:
    trade_date: date
    asof: datetime
    last: float
    change: float | None
    pct_chg: float | None
    amount: float | None  # full-day cumulative (sum of minute bars), yuan
    volume: float | None  # full-day cumulative (sum of minute bars)
    am_asof: datetime | None
    am_last: float | None
    am_amount: float | None  # AM cumulative (<=12:30), yuan
    am_volume: float | None
    raw: dict


def fetch_intraday_snapshot(
    *,
    ts_code: str,
    timeout_seconds: int = 15,
    am_cutoff_hhmm: str = "12:30",
) -> EastmoneyIntradaySnapshot:
    """Fetch intraday snapshot using Eastmoney minute kline endpoint.

    - Price snapshot: last 1-min bar.
    - Turnover: sum of per-bar amount/volume for the day.
    - AM turnover: sum for bars with time <= am_cutoff_hhmm (default 12:30).

    preKPrice from response is used to compute change/pct.
    """

    secid = _secid_from_ts_code(ts_code, timeout_seconds=timeout_seconds)
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

    with httpx.Client(**_client_kwargs(timeout_seconds)) as client:
        resp = client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params)
        resp.raise_for_status()
        data = resp.json()

    node = (data or {}).get("data") or {}
    klines = node.get("klines") or []
    if not klines:
        raise RuntimeError("Eastmoney intraday: empty klines")

    # Use the last 1-min bar as the latest price snapshot.
    last_row = str(klines[-1])
    parts = last_row.split(",")
    # dt,open,close,high,low,vol,amount,...
    asof = datetime.strptime(parts[0], "%Y-%m-%d %H:%M")
    last = float(parts[2])

    # Aggregate turnover (Eastmoney minute kline amount/vol is per-bar).
    volume_sum = 0.0
    amount_sum = 0.0
    have_volume = False
    have_amount = False

    am_volume_sum = 0.0
    am_amount_sum = 0.0
    have_am_volume = False
    have_am_amount = False
    am_asof: datetime | None = None
    am_last: float | None = None

    cutoff_h, cutoff_m = [int(x) for x in am_cutoff_hhmm.split(":", 1)]

    for row in klines:
        p = str(row).split(",")
        if not p or not p[0]:
            continue
        try:
            dt = datetime.strptime(p[0], "%Y-%m-%d %H:%M")
        except Exception:
            continue

        is_am = (dt.hour < cutoff_h) or (dt.hour == cutoff_h and dt.minute <= cutoff_m)

        if len(p) > 5 and p[5]:
            try:
                v = float(p[5])
                volume_sum += v
                have_volume = True
                if is_am:
                    am_volume_sum += v
                    have_am_volume = True
            except Exception:
                pass

        if len(p) > 6 and p[6]:
            try:
                a = float(p[6])
                amount_sum += a
                have_amount = True
                if is_am:
                    am_amount_sum += a
                    have_am_amount = True
            except Exception:
                pass

        if is_am and len(p) > 2 and p[2]:
            try:
                am_last = float(p[2])
                am_asof = dt
            except Exception:
                pass

    volume = volume_sum if have_volume else None
    amount = amount_sum if have_amount else None
    am_volume = am_volume_sum if have_am_volume else None
    am_amount = am_amount_sum if have_am_amount else None

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
        am_asof=am_asof,
        am_last=am_last,
        am_amount=am_amount,
        am_volume=am_volume,
        raw={"resp": data, "row": last_row},
    )
