from __future__ import annotations

from dataclasses import dataclass
from statistics import mean, median


@dataclass
class DistStats:
    n: int
    avg: float
    med: float
    min_value: int
    max_value: int


def compute_dist(values: list[int]) -> DistStats:
    if not values:
        raise ValueError("values is empty")
    return DistStats(
        n=len(values),
        avg=float(mean(values)),
        med=float(median(values)),
        min_value=min(values),
        max_value=max(values),
    )


def compute_rank_percentile(values: list[int], today: int) -> tuple[int, float]:
    """Rank 1 = highest. Percentile = share <= today."""
    if not values:
        raise ValueError("values is empty")
    rank = 1 + sum(1 for v in values if v > today)
    percentile = sum(1 for v in values if v <= today) / len(values)
    return rank, float(percentile)
