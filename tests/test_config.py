from pm_arb.config import Config


def test_default_ws_url():
    assert Config().clob_ws_url == "wss://ws-subscriptions-clob.polymarket.com/ws/market"
