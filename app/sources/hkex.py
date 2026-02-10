from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import httpx
from selectolax.parser import HTMLParser


HKEX_ARCHIVE_URL = "https://www.hkex.com.hk/Market-Data/Statistics/Consolidated-Reports/Securities-Statistics-Archive/Trading_Value_Volume_And_Number_Of_Deals?sc_lang=zh-HK"


@dataclass
class HkexDayRow:
    trade_date: date
    turnover_hkd: int
    is_half_day: bool


def _parse_hkex_date(s: str) -> date:
    # Accept: YYYY/MM/DD
    y, m, d = s.strip().split("/")
    return date(int(y), int(m), int(d))


def fetch_hkex_latest_table(timeout_seconds: int = 20) -> list[HkexDayRow]:
    """Fetches current archive page table (whatever date range HKEX currently serves).

    Note: HKEX archive is often delayed; this still provides an official baseline.
    """
    with httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"}) as client:
        r = client.get(HKEX_ARCHIVE_URL)
        r.raise_for_status()

    html = r.text
    doc = HTMLParser(html)

    # Find the first table under the main content.
    table = doc.css_first("table")
    if table is None:
        raise RuntimeError("HKEX table not found")

    rows: list[HkexDayRow] = []
    for tr in table.css("tbody tr"):
        tds = tr.css("td")
        if len(tds) < 2:
            continue

        date_text = tds[0].text().strip()
        if not re.match(r"^\d{4}/\d{2}/\d{2}\*?$", date_text):
            continue

        is_half_day = date_text.endswith("*")
        if is_half_day:
            date_text = date_text[:-1]

        val_text = tds[1].text().strip()
        val_text = val_text.replace(",", "")
        if not val_text.isdigit():
            continue

        rows.append(
            HkexDayRow(
                trade_date=_parse_hkex_date(date_text),
                turnover_hkd=int(val_text),
                is_half_day=is_half_day,
            )
        )

    # Sort ascending
    rows.sort(key=lambda x: x.trade_date)
    return rows
