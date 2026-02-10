from __future__ import annotations


def format_hkd(n: int) -> str:
    """Human-readable HKD turnover. Uses 億/萬億 units."""
    if n is None:
        return "-"
    yi = 100_000_000
    wan_yi = 10_000 * yi  # 1 萬億
    if n >= wan_yi:
        return f"{n / wan_yi:.2f} 萬億"
    if n >= yi:
        return f"{n / yi:.2f} 億"
    return f"{n:,}"


def format_hkd_yi(n: int) -> str:
    """Format turnover always in 億 (1e8 HKD), per UI requirement."""
    if n is None:
        return "-"
    yi = 100_000_000
    return f"{n / yi:.2f} 億"


def format_hsi_price_x100(n: int | None) -> str:
    if n is None:
        return "-"
    return f"{n / 100:.2f}"
