from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

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


_CODE_TO_SECID = {
    "SSE": "1.000001",
    "SZSE": "0.399001",
    "HSI": "100.HSI",
}

# Dashboard 11 indices (same logical codes as fetch_intraday_snapshot)
_DEFAULT_CODES = ["HSI", "SSE", "SZSE", "HS11", "DJI", "IXIC", "SPX", "N225", "UKX", "DAX", "ESTOXX50E"]

# Alias -> Eastmoney symbol candidates used in suggest API (Code field)
_CODE_CANDIDATES = {
    "HS11": ["KS11", "HS11"],
    "UKX": ["FTSE", "UKX"],
    "DAX": ["GDAXI", "DAX"],
    "ESTOXX50E": ["CSX5P", "ESTOXX50E"],
}

_SECID_CACHE: dict[str, str] = {}


def _client_kwargs(timeout_seconds: int) -> dict:
    proxy = (settings.EASTMONEY_PROXY_URL or "").strip() or None
    kwargs = {"timeout": timeout_seconds, "headers": EM_HEADERS}
    if proxy:
        kwargs["proxy"] = proxy
    return kwargs


@dataclass
class EastmoneyRealtimeSnapshot:
    code: str
    secid: str
    asof: datetime
    last: float | None
    change: float | None
    pct_chg: float | None
    amount: float | None
    volume: float | None
    raw: dict


def _resolve_secid_by_suggest(code: str, timeout_seconds: int = 20) -> str:
    code = code.upper().strip()
    if code in _SECID_CACHE:
        return _SECID_CACHE[code]

    if code in _CODE_TO_SECID:
        secid = _CODE_TO_SECID[code]
        _SECID_CACHE[code] = secid
        return secid

    candidates = _CODE_CANDIDATES.get(code, [code])

    with httpx.Client(**_client_kwargs(timeout_seconds)) as client:
        for key in candidates:
            resp = client.get(
                "https://searchapi.eastmoney.com/api/suggest/get",
                params={"input": key, "type": "14", "count": "10"},
            )
            resp.raise_for_status()
            data = resp.json() or {}
            rows = (((data.get("QuotationCodeTable") or {}).get("Data")) or [])
            for row in rows:
                row_code = str((row or {}).get("Code") or "").upper()
                quote_id = (row or {}).get("QuoteID")
                if quote_id and row_code == key.upper():
                    secid = str(quote_id)
                    _SECID_CACHE[code] = secid
                    return secid

    raise ValueError(f"Eastmoney suggest did not return QuoteID for {code}")


def fetch_realtime_snapshot(*, code: str, timeout_seconds: int = 20) -> EastmoneyRealtimeSnapshot:
    code = code.upper().strip()
    secid = _resolve_secid_by_suggest(code, timeout_seconds=timeout_seconds)

    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "invt": "2",
        "fltt": "2",
        # f43 latest, f169 change, f170 pct, f47 volume, f48 amount, f86 quote ts(sec)
        "fields": "f43,f47,f48,f57,f58,f60,f86,f169,f170",
    }

    with httpx.Client(**_client_kwargs(timeout_seconds)) as client:
        resp = client.get("https://push2.eastmoney.com/api/qt/stock/get", params=params)
        resp.raise_for_status()
        raw = resp.json() or {}

    node = (raw.get("data") or {})
    ts = node.get("f86")
    if ts:
        try:
            asof = datetime.fromtimestamp(int(ts), tz=ZoneInfo("Asia/Shanghai"))
        except Exception:
            asof = datetime.now(tz=ZoneInfo("Asia/Shanghai"))
    else:
        asof = datetime.now(tz=ZoneInfo("Asia/Shanghai"))

    def _f(k: str) -> float | None:
        v = node.get(k)
        if v in (None, "", "-"):
            return None
        try:
            return float(v)
        except Exception:
            return None

    return EastmoneyRealtimeSnapshot(
        code=code,
        secid=secid,
        asof=asof,
        last=_f("f43"),
        change=_f("f169"),
        pct_chg=_f("f170"),
        amount=_f("f48"),
        volume=_f("f47"),
        raw=raw,
    )


def default_codes() -> list[str]:
    return list(_DEFAULT_CODES)
