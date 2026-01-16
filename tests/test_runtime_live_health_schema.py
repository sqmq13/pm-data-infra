from __future__ import annotations

from pm_data.runtime.health import LiveUniverseSummary, build_live_health_summary
from pm_data.runtime.latency import LiveLatencyCollector


def _assert_json_safe(value: object) -> None:
    if value is None:
        return
    if isinstance(value, (bool, int, str)):
        return
    if isinstance(value, list):
        for item in value:
            _assert_json_safe(item)
        return
    if isinstance(value, dict):
        for key, item in value.items():
            assert isinstance(key, str)
            _assert_json_safe(item)
        return
    raise AssertionError(f"unexpected json value type: {type(value)}")


def test_live_health_summary_schema_version_and_types() -> None:
    collector = LiveLatencyCollector()
    collector.start(1_000)
    collector.observe_event(
        recv_ns=1_000,
        decode_start_ns=1_100,
        decode_end_ns=1_200,
        normalize_end_ns=1_300,
        state_end_ns=1_350,
        strategy_end_ns=1_400,
        allocator_end_ns=1_450,
        execution_end_ns=1_500,
        emit_end_ns=1_500,
    )
    collector.frames = 1
    collector.intents = 2
    collector.finish(2_000)

    report = build_live_health_summary(
        collector=collector,
        universe=LiveUniverseSummary(subscribed_tokens=10, selected_markets=5, fetched_markets=20),
        ws_shards=8,
        capture_max_markets=2000,
        reconnects=0,
        dropped=0,
        decode_errors=0,
        schema_errors=0,
        schema_error_bids_type=0,
        schema_error_asks_type=0,
        schema_error_item_exception=0,
        payload_items_total=1,
        payload_items_max=1,
        payload_frames_with_items=1,
        payload_frames_multi_item=0,
        prefilter_skipped_frames=0,
        stable=False,
    )

    assert report["record_type"] == "live_health_summary"
    assert report["schema_version"] == 1
    assert report["mode"] == "live"
    assert report["execution"] == "sim"
    assert set(report.keys()) == {
        "record_type",
        "schema_version",
        "mode",
        "execution",
        "t0_mono_ns",
        "t1_mono_ns",
        "elapsed_ns",
        "latency",
        "quality",
        "throughput",
        "workload",
        "shape",
        "resource",
    }
    latency = report["latency"]
    assert isinstance(latency, dict)
    assert set(latency.keys()) == {"e2e_total", "stages"}
    stages = latency["stages"]
    assert isinstance(stages, dict)
    for stage in (
        "recv_to_decode",
        "decode_to_normalize",
        "normalize_to_state",
        "state_to_strategy",
        "strategy_to_allocator",
        "allocator_to_execution",
        "execution_to_emit",
    ):
        assert stage in stages
    assert "recv_to_decode_start" in stages
    assert "decode_start_to_decode_end" in stages
    _assert_json_safe(dict(report))
