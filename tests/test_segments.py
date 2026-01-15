from pm_data.segments import (
    build_segment_maps,
    extract_expiry_times,
    infer_cadence_bucket,
    infer_instrument,
    infer_is_crypto,
)


def test_infer_cadence_bucket_from_question():
    market = {"question": "Will BTC 15 minute price stay above 50000?"}
    assert infer_cadence_bucket(market) == "15m"


def test_infer_cadence_bucket_ambiguous():
    market = {"question": "BTC 15 minute or 1 hour price"}
    assert infer_cadence_bucket(market) == "unknown"


def test_infer_cadence_bucket_from_minutes_field():
    market = {"intervalMinutes": 30}
    assert infer_cadence_bucket(market) == "30m"


def test_infer_is_crypto_from_category():
    market = {"category": "Crypto"}
    assert infer_is_crypto(market) is True
    market = {"category": "Sports"}
    assert infer_is_crypto(market) is False


def test_infer_instrument_from_question():
    market = {"question": "ETH 15 minute price"}
    assert infer_instrument(market) == "ETH"


def test_extract_expiry_times_iso():
    market = {"endDate": "2024-01-01T00:00:00Z"}
    start_ns, end_ns, resolve_ns = extract_expiry_times(market)
    assert start_ns is None
    assert end_ns is not None
    assert resolve_ns is None


def test_build_segment_maps_fee_fields():
    market = {
        "id": "m1",
        "question": "BTC 15 minute price",
        "clobTokenIds": ["t1", "t2"],
        "active": True,
        "enableOrderBook": True,
    }
    fee_results = {
        "t1": {"fee_rate_bps": 10, "fee_rate_known": True, "fee_enabled": True},
        "t2": {"fee_rate_bps": 10, "fee_rate_known": True, "fee_enabled": True},
    }
    token_map, market_map = build_segment_maps([market], fee_results_by_token=fee_results)
    assert token_map["t1"].fee_rate_bps == 10
    assert token_map["t1"].fee_rate_known is True
    assert market_map["m1"].fee_rate_known is True
    assert market_map["m1"].fee_rate_bps == 10


def test_build_segment_maps_fee_mismatch_unknown_market_fee():
    market = {
        "id": "m1",
        "question": "BTC 15 minute price",
        "clobTokenIds": ["t1", "t2"],
        "active": True,
        "enableOrderBook": True,
    }
    fee_results = {
        "t1": {"fee_rate_bps": 10, "fee_rate_known": True, "fee_enabled": True},
        "t2": {"fee_rate_bps": 20, "fee_rate_known": True, "fee_enabled": True},
    }
    _, market_map = build_segment_maps([market], fee_results_by_token=fee_results)
    assert market_map["m1"].fee_rate_known is False
