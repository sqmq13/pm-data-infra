import json

from pm_data.runtime.normalize import Normalizer
from pm_data.runtime.replay import RawFrame


def test_normalizer_skips_schema_invalid_bids_type() -> None:
    payload = {
        "event_type": "book",
        "asset_id": "tokenA",
        "timestamp": 1000,
        "bids": {"price": "0.40", "size": "10"},
        "asks": [{"price": "0.46", "size": "8"}],
    }
    frame = RawFrame(
        payload=json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        rx_mono_ns=123,
        rx_wall_ns_utc=0,
        shard_id=0,
        idx_i=0,
    )
    normalizer = Normalizer()
    events = normalizer.normalize(frame)
    assert len(events) == 1
    assert events[0].market_id == "tokenA"
    assert events[0].bid_px_e6 is None
    assert events[0].ask_px_e6 == 460_000
    assert normalizer.decode_errors == 0
    assert normalizer.schema_errors > 0


def test_normalizer_skips_schema_invalid_asks_type() -> None:
    payload = {
        "event_type": "book",
        "asset_id": "tokenA",
        "timestamp": 1000,
        "bids": [{"price": "0.40", "size": "10"}],
        "asks": "not-a-list",
    }
    frame = RawFrame(
        payload=json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        rx_mono_ns=123,
        rx_wall_ns_utc=0,
        shard_id=0,
        idx_i=0,
    )
    normalizer = Normalizer()
    events = normalizer.normalize(frame)
    assert len(events) == 1
    assert events[0].market_id == "tokenA"
    assert events[0].bid_px_e6 == 400_000
    assert events[0].ask_px_e6 is None
    assert normalizer.decode_errors == 0
    assert normalizer.schema_errors > 0

