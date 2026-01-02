from __future__ import annotations

from .fixed import SIZE_SCALE


def sweep_cost(
    asks: list[tuple[int, int]], target_units: int, size_scale: int = SIZE_SCALE
) -> tuple[int, int | None, bool]:
    if target_units <= 0:
        return 0, None, False
    remaining = target_units
    cost_micro = 0
    worst_price = None
    for price_micro, size_units in asks:
        if remaining <= 0:
            break
        if size_units <= 0:
            continue
        take = size_units if size_units <= remaining else remaining
        cost_micro += (price_micro * take + size_scale - 1) // size_scale
        worst_price = price_micro
        remaining -= take
    ok = remaining == 0
    return cost_micro, worst_price, ok
