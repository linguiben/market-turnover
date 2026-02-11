from __future__ import annotations


def format_amount_b(n: int | None) -> str:
    """Format amount as B (billions, 1e9).

    We use B consistently across HK/CN cards to avoid mixing 亿/億.
    """

    if n is None:
        return "-"
    b = 1_000_000_000
    return f"{n / b:.2f} B"


# Backwards-compatible alias used by templates/routes.
format_hkd_yi = format_amount_b


def format_hsi_price_x100(n: int | None) -> str:
    if n is None:
        return "-"
    return f"{n / 100:.2f}"
