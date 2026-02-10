from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date

import httpx


# HKEX provides the statistics archive as JSON tables; this is the most reliable
# source for historical daily trading value (turnover).
# Example file: https://www.hkex.com.hk/eng/stat/smstat/mthbull/rpt_data_statistics_archive_trading_data_2025_2029.json
HKEX_ARCHIVE_JSON_TEMPLATE = "https://www.hkex.com.hk/eng/stat/smstat/mthbull/rpt_data_statistics_archive_trading_data_{start}_{end}.json"


@dataclass
class HkexDayRow:
    trade_date: date
    turnover_hkd: int
    is_half_day: bool


def _parse_hkex_date(s: str) -> date:
    # Accept: YYYY/MM/DD
    y, m, d = s.strip().split("/")
    return date(int(y), int(m), int(d))


def _hkex_json_url_for_date(d: date) -> str:
    # HKEX publishes in 5-year buckets: e.g. 2025_2029
    start = (d.year // 5) * 5
    end = start + 4
    return HKEX_ARCHIVE_JSON_TEMPLATE.format(start=start, end=end)


def fetch_hkex_latest_table(timeout_seconds: int = 20) -> list[HkexDayRow]:
    """Fetch HKEX securities statistics archive (daily total trading value).

    This parses HKEX's published JSON table, which is stable and accurate.
    """

    url = _hkex_json_url_for_date(date.today())

    with httpx.Client(timeout=timeout_seconds, headers={"User-Agent": "market-turnover/0.1"}) as client:
        r = client.get(url)
        r.raise_for_status()
        payload = r.json()

    tables = payload.get("tables") or []
    if not tables:
        raise RuntimeError("HKEX archive JSON missing tables")

    # The first table contains the daily rows.
    body = (tables[0].get("body") or [])

    # Convert sparse cell list to row dict
    by_row: dict[int, dict[int, str]] = {}
    for cell in body:
        try:
            row = int(cell.get("row"))
            col = int(cell.get("col"))
        except Exception:
            continue
        by_row.setdefault(row, {})[col] = str(cell.get("text") or "").strip()

    rows: list[HkexDayRow] = []
    for r, cols in by_row.items():
        # col0=date, col1='*' or '', col2=value
        date_text = (cols.get(0) or "").strip()
        if not re.match(r"^\d{4}/\d{2}/\d{2}$", date_text):
            continue

        is_half_day = (cols.get(1) or "").strip() == "*"

        val_text = (cols.get(2) or "").replace(",", "").strip()
        if not val_text.isdigit():
            continue

        rows.append(
            HkexDayRow(
                trade_date=_parse_hkex_date(date_text),
                turnover_hkd=int(val_text),
                is_half_day=is_half_day,
            )
        )

    rows.sort(key=lambda x: x.trade_date)
    return rows
