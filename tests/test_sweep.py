from pm_arb.sweep import sweep_cost
from pm_arb.fixed import SIZE_SCALE


def test_sweep_cost_monotonic():
    asks = [
        (500_000, 5 * SIZE_SCALE),
        (600_000, 5 * SIZE_SCALE),
    ]
    cost_small, _, ok_small = sweep_cost(asks, 2 * SIZE_SCALE)
    cost_large, _, ok_large = sweep_cost(asks, 6 * SIZE_SCALE)
    assert ok_small and ok_large
    assert cost_small <= cost_large


def test_sweep_cost_with_more_liquidity():
    asks_base = [
        (500_000, 1 * SIZE_SCALE),
        (700_000, 1 * SIZE_SCALE),
    ]
    asks_more = [
        (500_000, 2 * SIZE_SCALE),
        (700_000, 1 * SIZE_SCALE),
    ]
    cost_base, _, ok_base = sweep_cost(asks_base, 2 * SIZE_SCALE)
    cost_more, _, ok_more = sweep_cost(asks_more, 2 * SIZE_SCALE)
    assert ok_base and ok_more
    assert cost_more <= cost_base
