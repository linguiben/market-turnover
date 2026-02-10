from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx


AASTOCKS_HK_INDEX_FEED_URL = "https://www.aastocks.com/tc/resources/datafeed/getstockindex.ashx?type=5"


@dataclass
class HsiSnapshot:
    last: float
    change: float | None
    change_pct: float | None
    turnover_hkd: int | None
    asof: datetime | None
    raw: dict


def _parse_turnover_to_hkd(text: str) -> int:
    """Parse strings like '1,338.23億' -> HKD int."""
    t = (text or "").strip().replace(",", "")
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*億", t)
    if m:
        return int(float(m.group(1)) * 100_000_000)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*萬億", t)
    if m:
        return int(float(m.group(1)) * 1_000_000_000_000)
    digits = re.sub(r"\D", "", t)
    if digits:
        return int(digits)
    raise ValueError(f"Cannot parse turnover from: {text}")


def fetch_hsi_snapshot(timeout_seconds: int = 10) -> HsiSnapshot:
    """Fetch HSI price & turnover from AASTOCKS public JSON feed."""
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers={"User-Agent": "market-turnover/0.1"}) as client:
        r = client.get(AASTOCKS_HK_INDEX_FEED_URL)
        r.raise_for_status()
        data = r.json()

    # data is a list of dicts
    row = None
    if isinstance(data, list):
        for it in data:
            if str(it.get("symbol") or "").upper() == "HSI":
                row = it
                break

    if not row:
        raise RuntimeError("HSI not found in AASTOCKS index feed")

    last_s = str(row.get("last") or "").replace(",", "").strip()
    if not last_s:
        raise RuntimeError("HSI last price missing")
    last = float(last_s)

    change = None
    change_s = str(row.get("change") or "").replace(",", "").strip()
    if change_s and re.match(r"^-?[0-9]+(?:\.[0-9]+)?$", change_s):
        change = float(change_s)

    change_pct = None
    p = str(row.get("changeper") or "").strip().replace("%", "")
    if p and re.match(r"^-?[0-9]+(?:\.[0-9]+)?$", p):
        change_pct = float(p)

    turnover_hkd = None
    try:
        turnover_hkd = _parse_turnover_to_hkd(str(row.get("turnover") or ""))
    except Exception:
        turnover_hkd = None

    asof = None
    lastupdate = str(row.get("lastupdate") or "").strip()  # 'YYYY/MM/DD HH:MM'
    if lastupdate:
        try:
            dt = datetime.strptime(lastupdate, "%Y/%m/%d %H:%M")
            asof = dt.replace(tzinfo=timezone(timedelta(hours=8)))
        except Exception:
            asof = None

    return HsiSnapshot(
        last=last,
        change=change,
        change_pct=change_pct,
        turnover_hkd=turnover_hkd,
        asof=asof,
        raw=row,
    )
