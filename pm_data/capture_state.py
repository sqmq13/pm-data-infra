from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from math import ceil
from statistics import mean
from pathlib import Path
from typing import Any, Iterable

from .capture import RunBootstrap
from .capture_offline import quantile
from .config import Config
from .fees import FeeRateClient, FeeRegimeMonitor, FeeRegimeState
from .gamma import UniverseSnapshot
from .policy import PolicySelector
from .segments import SegmentTag
from .ws_primitives import DropCounter


def _append_sample(samples: deque[int], value: int, max_samples: int) -> None:
    samples.append(value)
    while len(samples) > max_samples:
        samples.popleft()


def _append_sample_float(samples: deque[float], value: float, max_samples: int) -> None:
    samples.append(value)
    while len(samples) > max_samples:
        samples.popleft()


def _quantile_from_samples(samples: Iterable[int], percentile: float) -> int:
    values = [value for value in samples if value > 0]
    if not values:
        return 0
    return quantile(values, percentile)


def _quantiles_from_samples(
    samples: Iterable[int],
    percentiles: Iterable[float],
) -> dict[float, int]:
    values = [value for value in samples if value > 0]
    result: dict[float, int] = {}
    if not values:
        for percentile in percentiles:
            result[percentile] = 0
        return result
    ordered = sorted(values)
    count = len(ordered)
    for percentile in percentiles:
        rank = max(0, ceil(percentile / 100.0 * count) - 1)
        result[percentile] = ordered[rank]
    return result


def _quantiles_from_samples_any(
    samples: Iterable[int],
    percentiles: Iterable[float],
) -> dict[float, int]:
    values = list(samples)
    result: dict[float, int] = {}
    if not values:
        for percentile in percentiles:
            result[percentile] = 0
        return result
    ordered = sorted(values)
    count = len(ordered)
    for percentile in percentiles:
        rank = max(0, ceil(percentile / 100.0 * count) - 1)
        result[percentile] = ordered[rank]
    return result


def _quantile_from_float_samples(samples: Iterable[float], percentile: float) -> float:
    values = [value for value in samples if value >= 0]
    if not values:
        return 0.0
    ordered = sorted(values)
    count = len(ordered)
    rank = max(0, ceil(percentile / 100.0 * count) - 1)
    return ordered[rank]


def _quantiles_with_mean(samples: Iterable[float]) -> dict[str, float]:
    values = list(samples)
    if not values:
        return {"mean": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "max": 0.0}
    ordered = sorted(values)
    count = len(ordered)
    return {
        "mean": float(mean(ordered)),
        "p50": float(ordered[max(0, ceil(0.50 * count) - 1)]),
        "p95": float(ordered[max(0, ceil(0.95 * count) - 1)]),
        "p99": float(ordered[max(0, ceil(0.99 * count) - 1)]),
        "max": float(ordered[-1]),
    }


@dataclass
class ShardStats:
    frames: int = 0
    bytes_written: int = 0
    write_durations_ns: deque[int] = field(default_factory=deque)
    ingest_latencies_ns: deque[int] = field(default_factory=deque)
    backpressure_ns: deque[int] = field(default_factory=deque)
    token_ids: set[str] = field(default_factory=set)
    msg_type_counts: dict[str, int] = field(default_factory=dict)
    decode_errors: int = 0

    def record(
        self,
        payload_len: int,
        header_len: int,
        idx_len: int,
        write_duration_ns: int,
        ingest_latency_ns: int,
        backpressure_ns: int,
        token_ids: Iterable[str],
        msg_type_counts: dict[str, int],
        max_samples: int,
    ) -> None:
        self.frames += 1
        self.bytes_written += payload_len + header_len + idx_len
        _append_sample(self.write_durations_ns, write_duration_ns, max_samples)
        _append_sample(self.ingest_latencies_ns, ingest_latency_ns, max_samples)
        _append_sample(self.backpressure_ns, backpressure_ns, max_samples)
        for token_id in token_ids:
            self.token_ids.add(token_id)
        for msg_type, count in msg_type_counts.items():
            self.msg_type_counts[msg_type] = self.msg_type_counts.get(msg_type, 0) + count

    def record_counts(
        self,
        token_ids: Iterable[str],
        msg_type_counts: dict[str, int],
    ) -> None:
        for token_id in token_ids:
            self.token_ids.add(token_id)
        for msg_type, count in msg_type_counts.items():
            self.msg_type_counts[msg_type] = self.msg_type_counts.get(msg_type, 0) + count


@dataclass(frozen=True)
class ShardTargetPlan:
    token_ids: list[str]
    groups: list[list[str]]
    confirm_token_ids: list[str]
    target_version: int


@dataclass
class ShardTarget:
    token_ids: list[str]
    groups: list[list[str]]
    confirm_token_ids: list[str] = field(default_factory=list)
    refresh_requested: asyncio.Event = field(default_factory=asyncio.Event)
    target_version: int = 0


@dataclass
class ShardState:
    shard_id: int
    token_ids: list[str]
    groups: list[list[str]]
    frames_path: Path
    idx_path: Path
    frames_fh: Any
    idx_fh: Any
    ring: deque[tuple[bytes, bytes] | bytes]
    stats: ShardStats = field(default_factory=ShardStats)
    last_seen: dict[str, int] = field(default_factory=dict)
    reconnects: int = 0
    confirm_failures: int = 0
    confirmed: bool = False
    confirm_token_ids: list[str] = field(default_factory=list)
    confirm_events_seen: int = 0
    confirm_deadline_mono_ns: int | None = None
    target: ShardTarget | None = None
    last_subscribe_variant: str | None = None
    last_subscribe_group_index: int | None = None
    subscribe_variant_locked: str | None = None
    subscribe_variant_index: int = 0
    last_yield_mono_ns: int = 0
    connected: bool = False
    last_rx_mono_ns: int = 0
    disconnected_since_mono_ns: int | None = None
    ready_once: bool = False
    ready_mono_ns: int | None = None
    startup_connect_time_s: float = 0.0
    midrun_disconnected_time_s_total: float = 0.0
    midrun_disconnected_incidents: int = 0


@dataclass
class UniverseState:
    universe_version: int
    current_token_ids: set[str]
    current_market_ids: set[str]
    token_added_mono_ns: dict[str, int]
    shard_targets: dict[int, ShardTarget]
    refresh_task: asyncio.Task | None = None
    refresh_cancelled: bool = False
    effective_refresh_interval_seconds: float = 0.0
    refresh_count: int = 0
    refresh_failures: int = 0
    refresh_churn_pct_last: float = 0.0
    refresh_churn_guard_count: int = 0
    refresh_skipped_delta_below_min_count: int = 0
    refresh_last_decision_reason: str = "SKIPPED_DELTA_BELOW_MIN"
    refresh_inflight_future: asyncio.Future | None = None
    refresh_inflight_started_mono_ns: int | None = None
    refresh_inflight_timed_out: bool = False
    refresh_duration_ms_last: float = 0.0
    refresh_worker_duration_ms_last: float = 0.0
    refresh_gamma_pages_fetched_last: int = 0
    refresh_markets_seen_last: int = 0
    refresh_tokens_selected_last: int = 0
    refresh_inflight_skipped_count: int = 0
    tokens_added_last: int = 0
    tokens_removed_last: int = 0
    shards_refreshed_last: int = 0
    expected_churn_last: bool = False
    expected_churn_reason_last: str = "UNKNOWN"
    expected_churn_rollover_hit_last: bool = False
    expected_churn_expiry_hit_last: bool = False
    segment_by_token_id: dict[str, SegmentTag] = field(default_factory=dict)
    segment_by_market_id: dict[str, SegmentTag] = field(default_factory=dict)
    policy_counts_last: dict[str, int] = field(default_factory=dict)
    fee_regime_state: str = "OK"
    fee_regime_reason: str | None = None
    fee_regime_sampled_tokens_count: int = 0
    fee_rate_unknown_count_last: int = 0


@dataclass(frozen=True)
class RefreshPlan:
    snapshot: UniverseSnapshot
    desired_token_ids: set[str]
    desired_market_ids: set[str]
    ordered_tokens: list[str]
    shard_targets: dict[int, ShardTargetPlan]
    segment_by_token_id: dict[str, SegmentTag]
    segment_by_market_id: dict[str, SegmentTag]
    policy_counts: dict[str, int]
    fee_regime_state: FeeRegimeState
    fee_regime_sampled_tokens_count: int
    fee_rate_unknown_count: int
    gamma_pages_fetched: int
    markets_seen: int
    tokens_selected: int
    worker_duration_ms: float


@dataclass
class LossBudget:
    parse_queue_drops: DropCounter = field(default_factory=DropCounter)
    parse_results_drops: DropCounter = field(default_factory=DropCounter)
    metrics_drops: DropCounter = field(default_factory=DropCounter)
    runlog_enqueue_timeouts: DropCounter = field(default_factory=DropCounter)
    frame_write_queue_pressure_events: DropCounter = field(default_factory=DropCounter)


@dataclass
class CaptureState:
    run: RunBootstrap
    config: Config
    shards: list[ShardState]
    pinned_tokens: list[str]
    universe: UniverseState
    fee_rate_client: FeeRateClient | None = None
    policy_selector: PolicySelector | None = None
    fee_regime_monitor: FeeRegimeMonitor | None = None
    refresh_executor: Any | None = None
    refresh_worker: Any | None = None
    runlog_writer: Any | None = None
    metrics_writer: Any | None = None
    runlog_failed: bool = False
    metrics_writer_failed: bool = False
    parse_worker: Any | None = None
    frame_writer: Any | None = None
    loss_budget: LossBudget = field(default_factory=LossBudget)
    loop_lag_samples_ms: deque[float] = field(default_factory=lambda: deque(maxlen=1000))
    ws_rx_to_enqueue_ms_samples: deque[float] = field(default_factory=deque)
    ws_rx_to_parsed_ms_samples: deque[float] = field(default_factory=deque)
    ws_rx_to_applied_ms_samples: deque[float] = field(default_factory=deque)
    write_end_to_apply_ms_samples: deque[float] = field(default_factory=deque)
    apply_update_ms_samples: deque[float] = field(default_factory=deque)
    ws_in_q_depth_samples: deque[int] = field(default_factory=deque)
    parse_q_depth_samples: deque[int] = field(default_factory=deque)
    write_q_depth_samples: deque[int] = field(default_factory=deque)
    dropped_messages_total_last: int = 0
    startup_connect_time_s_samples: deque[float] = field(default_factory=deque)
    midrun_disconnected_time_s_total: float = 0.0
    midrun_disconnected_incidents: int = 0
    midrun_disconnected_durations_s: deque[float] = field(default_factory=deque)
    stop_event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_reason: str | None = None
    gc_was_enabled: bool = False
    backpressure_breach_count: int = 0
    fatal_event: asyncio.Event = field(default_factory=asyncio.Event)
    fatal_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    heartbeat_samples: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=10))
