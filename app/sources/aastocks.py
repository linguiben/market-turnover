from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

import httpx
from selectolax.parser import HTMLParser


# AASTOCKS pages change; this is a best-effort POC scraper.
AASTOCKS_HSI_LOCAL_INDEX_URL = "https://www.aastocks.com/tc/stocks/market/index/hk-index-con.aspx"


@dataclass
class AastocksMidday:
    turnover_hkd: int
    asof: datetime | None
    raw_turnover_text: str


def _parse_hk_turnover_to_hkd(text: str) -> int:
    """Parse strings like '1,338.23億' -> HKD int."""
    t = text.strip().replace(",", "")
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*億", t)
    if m:
        return int(float(m.group(1)) * 100_000_000)
    m = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*萬億", t)
    if m:
        return int(float(m.group(1)) * 1_000_000_000_000)
    # fallback: digits only = HKD
    digits = re.sub(r"\D", "", t)
    if digits:
        return int(digits)
    raise ValueError(f"Cannot parse turnover from: {text}")


def fetch_midday_turnover(timeout_seconds: int = 10) -> AastocksMidday:
    with httpx.Client(timeout=timeout_seconds, follow_redirects=True, headers={"User-Agent": "market-turnover/0.1"}) as client:
        r = client.get(AASTOCKS_HSI_LOCAL_INDEX_URL)
        r.raise_for_status()

    doc = HTMLParser(r.text)

    # Heuristic: find text containing '成交金額' and take nearby number with 億
    page_text = doc.text()
    # Try a compact regex first
    m = re.search(r"成交金額[^0-9]{0,20}([0-9][0-9,\.]*\s*(?:億|萬億))", page_text)
    if not m:
        raise RuntimeError("AASTOCKS turnover not found (regex)")

    raw = m.group(1)
    turnover_hkd = _parse_hk_turnover_to_hkd(raw)

    # Try parse last update time if present
    asof = None
    m2 = re.search(r"最後更新於\s*：\s*(\d{4}/\d{2}/\d{2}\s*\d{2}:\d{2})", page_text)
    if m2:
        try:
            asof = datetime.strptime(m2.group(1), "%Y/%m/%d %H:%M")
        except Exception:
            asof = None

    return AastocksMidday(turnover_hkd=turnover_hkd, asof=asof, raw_turnover_text=raw)
