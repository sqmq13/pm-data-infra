from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping

from .latency import LiveLatencyCollector


@dataclass(frozen=True, slots=True)
class LiveUniverseSummary:
    subscribed_tokens: int
    selected_markets: int | None = None
    fetched_markets: int | None = None


def _stage_p99_max(collector: LiveLatencyCollector, stage: str) -> dict[str, int]:
    hist = collector.stages.get(stage)
    if hist is None:
        return {"p99_us": 0, "max_us": 0}
    summary = hist.summary()
    return {"p99_us": int(summary.p99_us), "max_us": int(summary.max_us)}


def build_live_health_summary(
    *,
    collector: LiveLatencyCollector,
    universe: LiveUniverseSummary,
    ws_shards: int,
    capture_max_markets: int | None,
    reconnects: int,
    dropped: int,
    decode_errors: int,
    schema_errors: int,
    schema_error_bids_type: int,
    schema_error_asks_type: int,
    schema_error_item_exception: int,
    payload_items_total: int,
    payload_items_max: int,
    payload_frames_with_items: int,
    payload_frames_multi_item: int,
    prefilter_skipped_frames: int,
    stable: bool = False,
) -> Mapping[str, Any]:
    elapsed_ns = max(0, int(collector.end_mono_ns - collector.start_mono_ns))
    cpu_ns = max(0, int(collector.end_cpu_ns - collector.start_cpu_ns))

    cpu_util_bp = 0
    frames_per_sec_e3 = 0
    events_per_sec_e3 = 0
    intents_per_sec_e3 = 0
    if elapsed_ns > 0:
        cpu_count = 1
        try:
            import os

            cpu_count = os.cpu_count() or 1
        except Exception:
            cpu_count = 1
        cpu_util_bp = int((cpu_ns * 10_000) // (elapsed_ns * cpu_count))
        frames_per_sec_e3 = int((collector.frames * 1_000_000_000_000) // elapsed_ns)
        events_per_sec_e3 = int(
            (collector.canonical_events * 1_000_000_000_000) // elapsed_ns
        )
        intents_per_sec_e3 = int((collector.intents * 1_000_000_000_000) // elapsed_ns)

    quality_degraded = bool(
        reconnects > 0
        or dropped > 0
        or decode_errors > 0
        or schema_errors > 0
        or schema_error_item_exception > 0
    )

    e2e = collector.stages["e2e_total"].summary().to_dict()
    if stable:
        e2e["max_us"] = int(e2e["max_us"])

    return {
        "record_type": "live_health_summary",
        "schema_version": 1,
        "mode": "live",
        "execution": "sim",
        "t0_mono_ns": int(collector.start_mono_ns),
        "t1_mono_ns": int(collector.end_mono_ns),
        "elapsed_ns": int(elapsed_ns),
        "latency": {
            "e2e_total": e2e,
            "stages": {
                "recv_to_decode": _stage_p99_max(collector, "recv_to_decode"),
                "decode_to_normalize": _stage_p99_max(collector, "decode_to_normalize"),
                "normalize_to_state": _stage_p99_max(collector, "normalize_to_state"),
                "state_to_strategy": _stage_p99_max(collector, "state_to_strategy"),
                "strategy_to_allocator": _stage_p99_max(collector, "strategy_to_allocator"),
                "allocator_to_execution": _stage_p99_max(collector, "allocator_to_execution"),
                "execution_to_emit": _stage_p99_max(collector, "execution_to_emit"),
                "recv_to_decode_start": _stage_p99_max(collector, "recv_to_decode_start"),
                "decode_start_to_decode_end": _stage_p99_max(
                    collector, "decode_start_to_decode_end"
                ),
            },
        },
        "quality": {
            "reconnects": int(reconnects),
            "dropped": int(dropped),
            "decode_errors": int(decode_errors),
            "schema_errors": int(schema_errors),
            "schema_error_bids_type": int(schema_error_bids_type),
            "schema_error_asks_type": int(schema_error_asks_type),
            "schema_error_item_exception": int(schema_error_item_exception),
            "quality_degraded": bool(quality_degraded),
        },
        "throughput": {
            "frames": int(collector.frames),
            "canonical_events": int(collector.canonical_events),
            "intents": int(collector.intents),
            "frames_per_sec_e3": int(frames_per_sec_e3),
            "events_per_sec_e3": int(events_per_sec_e3),
            "intents_per_sec_e3": int(intents_per_sec_e3),
        },
        "workload": {
            "subscribed_tokens": int(universe.subscribed_tokens),
            "selected_markets": int(universe.selected_markets)
            if universe.selected_markets is not None
            else None,
            "fetched_markets": int(universe.fetched_markets)
            if universe.fetched_markets is not None
            else None,
            "ws_shards": int(ws_shards),
            "capture_max_markets": int(capture_max_markets)
            if capture_max_markets is not None
            else None,
        },
        "shape": {
            "payload_items_total": int(payload_items_total),
            "payload_items_max": int(payload_items_max),
            "payload_frames_with_items": int(payload_frames_with_items),
            "payload_frames_multi_item": int(payload_frames_multi_item),
            "prefilter_skipped_frames": int(prefilter_skipped_frames),
        },
        "resource": {
            "cpu_time_ns": int(cpu_ns),
            "cpu_util_bp": int(cpu_util_bp),
        },
    }
