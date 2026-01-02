from pm_arb.book import parse_ws_message


def test_parse_ws_message_list_snapshot():
    payload = [
        {
            "market": "m1",
            "asset_id": "tokenA",
            "timestamp": 1234,
            "bids": [{"price": "0.20", "size": "5"}],
            "asks": [{"price": "0.30", "size": "2"}],
        }
    ]
    token_id, asks, ts = parse_ws_message(payload)
    assert token_id == "tokenA"
    assert ts == 1234
    assert len(asks) == 1
