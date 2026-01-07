import orjson
from collections import deque

from pm_arb.capture_online import (
    _extract_minimal_fields,
    _quantile_from_samples,
    _stable_hash,
    assign_shards_by_token,
    split_subscribe_groups,
)
from pm_arb.clob_ws import build_subscribe_payload


def test_assign_shards_by_token_deterministic():
    tokens = ["tokenA", "tokenB", "tokenC", "tokenD", "tokenE"]
    first = assign_shards_by_token(tokens, 3)
    second = assign_shards_by_token(tokens, 3)
    assert first == second
    for token_id in tokens:
        shard_id = _stable_hash(token_id) % 3
        assert token_id in first[shard_id]


def test_assign_shards_by_token_dedupes():
    tokens = ["tokenA", "tokenB", "tokenA", "tokenC"]
    shards = assign_shards_by_token(tokens, 2)
    combined = [token for shard in shards.values() for token in shard]
    assert combined.count("tokenA") == 1


def test_split_subscribe_groups_limits():
    tokens = [f"token{i}" for i in range(10)]
    groups = split_subscribe_groups(tokens, max_tokens=3, max_bytes=200, variant="A")
    assert groups
    for group in groups:
        assert len(group) <= 3
        payload_bytes = orjson.dumps(build_subscribe_payload("A", group))
        assert len(payload_bytes) <= 200
    assert split_subscribe_groups(tokens, max_tokens=3, max_bytes=200, variant="A") == groups


def test_extract_minimal_fields_book_event():
    payload = {"event_type": "book", "asset_id": "123", "bids": [], "asks": []}
    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    assert ("123", "book") in token_pairs
    assert msg_type_counts["book"] == 1


def test_extract_minimal_fields_price_change_event():
    payload = {
        "event_type": "price_change",
        "price_changes": [{"asset_id": "a"}, {"asset_id": "b"}],
    }
    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    assert ("a", "price_change") in token_pairs
    assert ("b", "price_change") in token_pairs
    assert msg_type_counts["price_change"] == 1


def test_extract_minimal_fields_infers_book():
    payload = {"asset_id": "456", "bids": [], "asks": []}
    token_pairs, msg_type_counts = _extract_minimal_fields(payload)
    assert ("456", "book") in token_pairs
    assert msg_type_counts["book"] == 1


def test_quantile_from_samples_ignores_zero():
    samples = deque([0, 0, 5, 10])
    assert _quantile_from_samples(samples, 50) == 5


def test_quantile_from_samples_all_zero():
    samples = deque([0, 0, 0])
    assert _quantile_from_samples(samples, 95) == 0
