from pm_arb import gamma
from pm_arb.config import Config


def test_parse_clob_token_ids_variants():
    assert gamma.parse_clob_token_ids(["id1", "id2"]) == ["id1", "id2"]
    assert gamma.parse_clob_token_ids('["id1","id2"]') == ["id1", "id2"]
    assert gamma.parse_clob_token_ids("id1,id2") == ["id1", "id2"]


def test_fetch_markets_pagination(monkeypatch):
    pages = {
        0: [
            {"id": "m1", "clobTokenIds": '["a","b"]'},
            {"id": "m2", "clobTokenIds": ["c", "d"]},
        ],
        100: [{"id": "m3", "clobTokenIds": "e,f"}],
        200: [],
    }
    class DummyResp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return {"markets": self._data}

    class DummySession:
        def __init__(self):
            self.calls = []
            self.closed = False

        def get(self, url, params=None, timeout=None, headers=None):
            self.calls.append((params, headers))
            offset = params.get("offset", 0)
            data = pages.get(offset, [])
            return DummyResp(data)

        def close(self):
            self.closed = True

    sessions = []

    def fake_session():
        session = DummySession()
        sessions.append(session)
        return session

    monkeypatch.setattr(gamma.requests, "Session", fake_session)

    markets = gamma.fetch_markets("https://gamma", timeout=1.0, limit=100, max_markets=10)
    assert len(markets) == 3
    tokens = [gamma.parse_clob_token_ids(m["clobTokenIds"]) for m in markets]
    assert all(len(t) == 2 for t in tokens)
    assert len(sessions) == 1
    assert sessions[0].closed is True
    params_seen = [call[0] for call in sessions[0].calls]
    headers_seen = [call[1] for call in sessions[0].calls]
    assert [call["offset"] for call in params_seen] == [0, 100, 200]
    assert all(call["limit"] == 100 for call in params_seen)
    assert all(call["order"] == "id" for call in params_seen)
    assert all(call["ascending"] == "false" for call in params_seen)
    assert all(call["User-Agent"] == gamma.DEFAULT_USER_AGENT for call in headers_seen)


def test_compute_desired_universe_orders_and_filters(monkeypatch):
    markets = [
        {"id": "10", "active": True, "enableOrderBook": True, "clobTokenIds": ["a", "b"]},
        {"id": "11", "active": False, "enableOrderBook": True, "clobTokenIds": ["c", "d"]},
        {"id": "12", "active": True, "enableOrderBook": False, "clobTokenIds": ["e", "f"]},
        {"id": "13", "active": True, "enableOrderBook": True, "clobTokenIds": ["g", "h", "i"]},
        {"id": "9", "active": True, "enableOrderBook": True, "clobTokenIds": ["i", "j"]},
    ]

    def fake_fetch(*args, **kwargs):
        return list(markets)

    monkeypatch.setattr(gamma, "fetch_markets", fake_fetch)
    config = Config(capture_max_markets=2)
    snapshot = gamma.compute_desired_universe(config, universe_version=3)
    assert snapshot.universe_version == 3
    assert snapshot.market_ids == ["10", "9"]
    assert snapshot.token_ids == ["a", "b", "i", "j"]
    assert snapshot.selection["max_markets"] == 2
    assert snapshot.selection["filters_enabled"] is False
    assert snapshot.created_wall_ns_utc > 0
    assert snapshot.created_mono_ns > 0


def test_compute_desired_universe_preserves_order_non_numeric_ids(monkeypatch):
    markets = [
        {"id": "b", "active": True, "enableOrderBook": True, "clobTokenIds": ["a", "b"]},
        {"id": "a", "active": True, "enableOrderBook": True, "clobTokenIds": ["c", "d"]},
    ]

    def fake_fetch(*args, **kwargs):
        return list(markets)

    monkeypatch.setattr(gamma, "fetch_markets", fake_fetch)
    config = Config(capture_max_markets=10)
    snapshot = gamma.compute_desired_universe(config)
    assert snapshot.market_ids == ["b", "a"]
