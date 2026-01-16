import json

from pm_data.runtime.normalize import Normalizer
from pm_data.runtime.replay import RawFrame


def _frame(payload: object) -> RawFrame:
    return RawFrame(
        payload=json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8"),
        rx_mono_ns=123,
        rx_wall_ns_utc=0,
        shard_id=0,
        idx_i=0,
    )


def test_normalizer_uses_last_level_for_sorted_bid_ask_lists() -> None:
    payload = {
        "event_type": "book",
        "asset_id": "tokenA",
        "timestamp": 1000,
        "bids": [
            {"price": "0.10", "size": "1"},
            {"price": "0.20", "size": "1"},
            {"price": "0.30", "size": "1"},
        ],
        "asks": [
            {"price": "0.90", "size": "1"},
            {"price": "0.80", "size": "1"},
            {"price": "0.70", "size": "1"},
        ],
    }

    normalizer = Normalizer()
    events = normalizer.normalize(_frame(payload))
    assert len(events) == 1
    assert events[0].bid_px_e6 == 300_000
    assert events[0].ask_px_e6 == 700_000
    assert normalizer.bid_scan_count == 0
    assert normalizer.ask_scan_count == 0


def test_normalizer_large_sorted_lists_avoid_scans() -> None:
    bids = [{"price": f"0.{i:02d}", "size": "1"} for i in range(1, 80)]
    asks = [{"price": f"0.{i:02d}", "size": "1"} for i in range(99, 20, -1)]
    payload = {"asset_id": "tokenA", "bids": bids, "asks": asks}
    normalizer = Normalizer()
    events = normalizer.normalize(_frame(payload))
    assert len(events) == 1
    assert normalizer.bid_scan_count == 0
    assert normalizer.ask_scan_count == 0


def test_normalizer_falls_back_to_scan_when_bids_not_monotonic() -> None:
    payload = {
        "event_type": "book",
        "asset_id": "tokenA",
        "timestamp": 1000,
        "bids": [
            {"price": "0.10", "size": "1"},
            {"price": "0.20", "size": "1"},
            {"price": "1.00", "size": "1"},
            {"price": "0.30", "size": "1"},
            {"price": "0.40", "size": "1"},
        ],
        "asks": [],
    }

    normalizer = Normalizer()
    events = normalizer.normalize(_frame(payload))
    assert len(events) == 1
    assert events[0].bid_px_e6 == 1_000_000
    assert normalizer.bid_scan_count > 0


def test_normalizer_falls_back_to_scan_when_asks_not_monotonic() -> None:
    payload = {
        "event_type": "book",
        "asset_id": "tokenA",
        "timestamp": 1000,
        "bids": [],
        "asks": [
            {"price": "0.90", "size": "1"},
            {"price": "0.80", "size": "1"},
            {"price": "0.10", "size": "1"},
            {"price": "0.70", "size": "1"},
            {"price": "0.60", "size": "1"},
        ],
    }

    normalizer = Normalizer()
    events = normalizer.normalize(_frame(payload))
    assert len(events) == 1
    assert events[0].ask_px_e6 == 100_000
    assert normalizer.ask_scan_count > 0
