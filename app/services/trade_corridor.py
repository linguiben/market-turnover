from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass
class CorridorStat:
    name: str
    turnover_hkd: int | None  # 交易額
    net_inflow_hkd: int | None  # 淨流入（可為負）
    trades_count: int | None  # 交易筆數
    asof: str | None
    source: str


@dataclass
class CorridorHighlights:
    highest_turnover: CorridorStat | None
    max_net_inflow: CorridorStat | None
    max_net_outflow: CorridorStat | None
    max_trades: CorridorStat | None
    rows: list[CorridorStat]


def get_trade_corridor_highlights_mock() -> CorridorHighlights:
    """POC placeholder.

    TODO: replace with real data sources (e.g., Stock Connect northbound/southbound)
    providing turnover, net inflow/outflow, and trades count.
    """

    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    rows = [
        CorridorStat(
            name="南向通（港股通）",
            turnover_hkd=98_500_000_000,
            net_inflow_hkd=+12_300_000_000,
            trades_count=1_820_000,
            asof=now,
            source="MOCK",
        ),
        CorridorStat(
            name="北向通（沪深股通）",
            turnover_hkd=112_200_000_000,
            net_inflow_hkd=-8_600_000_000,
            trades_count=2_150_000,
            asof=now,
            source="MOCK",
        ),
    ]

    highest_turnover = max(rows, key=lambda r: r.turnover_hkd or -1)
    max_net_inflow = max(rows, key=lambda r: r.net_inflow_hkd or -10**30)
    max_net_outflow = min(rows, key=lambda r: r.net_inflow_hkd or 10**30)
    max_trades = max(rows, key=lambda r: r.trades_count or -1)

    return CorridorHighlights(
        highest_turnover=highest_turnover,
        max_net_inflow=max_net_inflow,
        max_net_outflow=max_net_outflow,
        max_trades=max_trades,
        rows=rows,
    )
