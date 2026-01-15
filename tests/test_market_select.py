from pm_data.capture_online import _load_pinned_markets
from pm_data.config import Config
from pm_data.gamma import select_active_binary_markets


def test_select_active_binary_markets_filters_and_orders():
    markets = [
        {
            "id": "m1",
            "active": True,
            "enableOrderBook": True,
            "clobTokenIds": ["t1", "t2"],
        },
        {
            "id": "m2",
            "active": False,
            "clobTokenIds": ["t3", "t4"],
        },
        {
            "id": "m3",
            "active": True,
            "enableOrderBook": False,
            "clobTokenIds": ["t5", "t6"],
        },
        {
            "id": "m4",
            "active": True,
            "clobTokenIds": ["t7"],
        },
        {
            "id": "m5",
            "active": True,
            "clobTokenIds": ["t8", "t9"],
        },
    ]
    selected = select_active_binary_markets(markets, max_markets=10)
    assert [market["id"] for market in selected] == ["m1", "m5"]
    truncated = select_active_binary_markets(markets, max_markets=1)
    assert [market["id"] for market in truncated] == ["m1"]


def test_capture_pins_active_binary_by_default(monkeypatch):
    markets = [
        {
            "id": "m1",
            "slug": "only",
            "question": "q1",
            "active": True,
            "enableOrderBook": True,
            "clobTokenIds": ["t1", "t2"],
        },
        {
            "id": "m2",
            "slug": "other",
            "question": "q2",
            "active": True,
            "enableOrderBook": True,
            "clobTokenIds": ["t3", "t4"],
        },
    ]
    monkeypatch.setattr(
        "pm_data.capture_online.fetch_markets", lambda *args, **kwargs: markets
    )
    config = Config(capture_max_markets=10)
    selected, tokens, universe_mode, market_regex_effective, _ = _load_pinned_markets(
        config
    )
    assert universe_mode == "active-binary"
    assert market_regex_effective is None
    assert {market["id"] for market in selected} == {"m1", "m2"}
    assert tokens == ["t1", "t2", "t3", "t4"]
