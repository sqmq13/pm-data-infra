from pm_data.runtime.allocator import Allocator, AllocatorConfig
from pm_data.runtime.intents import PlaceOrderIntent
from pm_data.runtime.state import GlobalState


def _intent(market_id: str, side: str, price_e6: int) -> PlaceOrderIntent:
    return PlaceOrderIntent(
        market_id=market_id,
        side=side,
        price_e6=price_e6,
        size_e6=1_000_000,
        order_type="limit",
        tif="gtc",
        urgency="maker",
        max_slippage_bps=None,
        tag="s",
        expires_at=None,
    )


def test_allocator_determinism():
    state = GlobalState()
    intents_a = [_intent("m1", "buy", 900_000), _intent("m2", "sell", 1_100_000)]
    intents_b = [_intent("m3", "buy", 800_000)]
    allocator = Allocator()
    first = allocator.merge_intents({"b": intents_b, "a": intents_a}, state)
    second = allocator.merge_intents({"a": intents_a, "b": intents_b}, state)
    assert first == second
    assert [intent.market_id for intent in first] == ["m1", "m2", "m3"]


def test_allocator_limits_deterministic():
    state = GlobalState()
    intents_a = [_intent("m1", "buy", 900_000), _intent("m2", "sell", 1_100_000)]
    intents_b = [_intent("m3", "buy", 800_000), _intent("m4", "sell", 1_200_000)]
    allocator = Allocator(
        config=AllocatorConfig(max_intents_global=3, max_intents_per_strategy=1)
    )
    merged = allocator.merge_intents({"b": intents_b, "a": intents_a}, state)
    assert [intent.market_id for intent in merged] == ["m1", "m3"]
