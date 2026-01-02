from pm_arb import gamma


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
    calls = []

    class DummyResp:
        def __init__(self, data):
            self._data = data

        def raise_for_status(self):
            return None

        def json(self):
            return {"markets": self._data}

    def fake_get(url, params=None, timeout=None):
        calls.append(params)
        offset = params.get("offset", 0)
        data = pages.get(offset, [])
        return DummyResp(data)

    monkeypatch.setattr(gamma.requests, "get", fake_get)

    markets = gamma.fetch_markets("https://gamma", timeout=1.0, limit=100, max_markets=10)
    assert len(markets) == 3
    tokens = [gamma.parse_clob_token_ids(m["clobTokenIds"]) for m in markets]
    assert all(len(t) == 2 for t in tokens)
    assert [call["offset"] for call in calls] == [0, 100, 200]
    assert all(call["limit"] == 100 for call in calls)
    assert all(call["closed"] == "false" for call in calls)
