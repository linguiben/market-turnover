from __future__ import annotations

import re
from dataclasses import dataclass

import httpx


@dataclass
class Quote:
    symbol: str
    name: str
    last: float
    change: float
    pct: float
    asof: str | None
    currency: str | None


def _parse_line(line: str) -> tuple[str, list[str]] | None:
    # v_usDJI="...";
    m = re.match(r"^v_([^=]+)=\"(.*)\";?$", line.strip())
    if not m:
        return None
    sym = m.group(1)
    fields = m.group(2).split("~")
    return sym, fields


def fetch_quotes(symbols: list[str], timeout_seconds: int = 10) -> list[Quote]:
    if not symbols:
        return []
    url = "https://qt.gtimg.cn/q=" + ",".join(symbols)
    r = httpx.get(url, timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"})
    r.raise_for_status()
    text = r.content.decode("gbk", "ignore")

    out: list[Quote] = []
    for line in text.strip().splitlines():
        parsed = _parse_line(line)
        if not parsed:
            continue
        sym, f = parsed
        # Common format (US): f[1]=name f[3]=last f[31]=chg f[32]=pct f[30]=asof
        # HK index format also matches those positions.
        try:
            name = f[1]
            last = float(f[3])
            chg = float(f[31])
            pct = float(f[32])
            asof = f[30] if len(f) > 30 else None
            currency = f[15] if len(f) > 15 and f[15] else None
            out.append(Quote(symbol=sym, name=name, last=last, change=chg, pct=pct, asof=asof, currency=currency))
            continue
        except Exception:
            pass

        # Simple index format: v_s_sh000001="1~上证指数~000001~4137.06~5.08~0.12~..."
        try:
            name = f[1]
            last = float(f[3])
            chg = float(f[4])
            pct = float(f[5])
            out.append(Quote(symbol=sym, name=name, last=last, change=chg, pct=pct, asof=None, currency=None))
        except Exception:
            continue

    return out
