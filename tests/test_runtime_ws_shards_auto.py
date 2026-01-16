from pm_data.runtime.entrypoint import _effective_live_ws_shards


def test_effective_live_ws_shards_auto_threshold() -> None:
    assert _effective_live_ws_shards(ws_shards=8, runtime_auto=True, token_count=4999) == 8
    assert _effective_live_ws_shards(ws_shards=8, runtime_auto=True, token_count=5000) == 4


def test_effective_live_ws_shards_auto_respects_manual() -> None:
    assert _effective_live_ws_shards(ws_shards=4, runtime_auto=True, token_count=8000) == 4
    assert _effective_live_ws_shards(ws_shards=16, runtime_auto=True, token_count=8000) == 16
    assert _effective_live_ws_shards(ws_shards=8, runtime_auto=False, token_count=8000) == 8

