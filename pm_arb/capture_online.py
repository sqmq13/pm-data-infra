from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import inspect
import gc
import os
import platform
import queue
import shutil
import signal
import socket
import struct
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from math import gcd, ceil
from collections import deque
from statistics import mean
from dataclasses import asdict, dataclass, field, fields, is_dataclass
from pathlib import Path
from typing import Any, Iterable

import orjson
import requests
import websockets

from .capture import RunBootstrap, _append_ndjson, _write_json, bootstrap_run, monotonic_ns
from .capture_format import (
    FLAG_BINARY_PAYLOAD,
    FLAG_TEXT_PAYLOAD,
    append_record,
    frames_header_len,
    frames_header_struct,
    frames_magic,
    idx_entry_len,
)
from .capture_offline import quantile
from .clob_ws import build_subscribe_payload
from .config import Config
from .fees import (
    FeeRateClient,
    FeeRegimeMonitor,
    FeeRegimeState,
    parse_expected_fee_rate_bps_values,
)
from .gamma import (
    UniverseSnapshot,
    compute_desired_universe,
    fetch_markets,
    parse_clob_token_ids,
    select_active_binary_markets,
)
from .policy import PolicySelector, parse_policy_rules, policy_counts
from .segments import SegmentTag, build_segment_maps

SUBSCRIBE_VARIANTS = ("A", "B", "C")


_IO_WRITER_STOP = object()
_IO_WRITER: "_NdjsonWriter | None" = None
_PARSE_WORKER_STOP = object()
_FRAME_WRITE_STOP = object()
_ORJSON_NDJSON_OPTIONS = orjson.OPT_APPEND_NEWLINE
if hasattr(orjson, "OPT_ESCAPE_NON_ASCII"):
    _ORJSON_NDJSON_OPTIONS |= orjson.OPT_ESCAPE_NON_ASCII


def _normalize_orjson(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, bytearray):
        return bytes(value).hex()
    if isinstance(value, memoryview):
        return value.tobytes().hex()
    if is_dataclass(value) and not isinstance(value, type):
        return _normalize_orjson(asdict(value))
    if isinstance(value, dict):
        return {str(key): _normalize_orjson(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set, frozenset, deque)):
        return [_normalize_orjson(item) for item in value]
    return str(value)


@dataclass(frozen=True)
class _ParseJob:
    shard_id: int
    payload_bytes: bytes
    rx_mono_ns: int


@dataclass(frozen=True)
class _ParseResult:
    shard_id: int
    rx_mono_ns: int
    t_parsed_mono_ns: int
    token_ids: list[str]
    msg_type_counts: dict[str, int]
    decode_error: bool


class _ParseWorker:
    def __init__(
        self,
        *,
        max_queue: int = 5000,
        max_results: int = 5000,
    ) -> None:
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._results: queue.Queue[object] = queue.Queue(maxsize=max_results)
        self._dropped = 0
        self._results_dropped = 0
        self._processed = 0
        self._thread = threading.Thread(
            target=self._run,
            name="payload-parse",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, job: _ParseJob) -> bool:
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            self._dropped += 1
            return False
        return True

    def get_nowait(self) -> _ParseResult | None:
        try:
            item = self._results.get_nowait()
        except queue.Empty:
            return None
        if item is _PARSE_WORKER_STOP:
            return None
        return item

    def close(self, timeout_seconds: float = 5.0) -> None:
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_PARSE_WORKER_STOP)
        self._thread.join(timeout=timeout_seconds)

    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "results_size": self._results.qsize(),
            "dropped": self._dropped,
            "results_dropped": self._results_dropped,
            "processed": self._processed,
        }

    def queue_size(self) -> int:
        return self._queue.qsize()

    def results_size(self) -> int:
        return self._results.qsize()

    def _run(self) -> None:
        while True:
            job = self._queue.get()
            if job is _PARSE_WORKER_STOP:
                with contextlib.suppress(queue.Full):
                    self._results.put_nowait(_PARSE_WORKER_STOP)
                break
            assert isinstance(job, _ParseJob)
            try:
                payload = orjson.loads(job.payload_bytes)
                token_pairs, msg_type_counts = _extract_minimal_fields(payload)
                token_ids = [token_id for token_id, _ in token_pairs]
                decode_error = False
            except orjson.JSONDecodeError:
                token_ids = []
                msg_type_counts = {}
                decode_error = True
            t_parsed_mono_ns = monotonic_ns()
            result = _ParseResult(
                shard_id=job.shard_id,
                rx_mono_ns=job.rx_mono_ns,
                t_parsed_mono_ns=t_parsed_mono_ns,
                token_ids=token_ids,
                msg_type_counts=msg_type_counts,
                decode_error=decode_error,
            )
            try:
                self._results.put_nowait(result)
                self._processed += 1
            except queue.Full:
                self._results_dropped += 1


@dataclass(frozen=True)
class _FrameWriteJob:
    shard_id: int
    payload_bytes: bytes
    rx_mono_ns: int
    rx_wall_ns_utc: int
    flags: int


@dataclass(frozen=True)
class _FrameWriteResult:
    shard_id: int
    payload_bytes: bytes
    header_bytes: bytes
    schema_version: int
    payload_len: int
    rx_mono_ns: int
    write_duration_ns: int
    ingest_latency_ns: int
    backpressure_ns: int


class _FrameWriter:
    def __init__(
        self,
        *,
        shards: list["ShardState"],
        schema_version: int,
        max_queue: int = 5000,
        max_results: int = 5000,
    ) -> None:
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._results: queue.Queue[object] = queue.Queue(maxsize=max_results)
        self._handles: dict[int, tuple[Any, Any]] = {
            shard.shard_id: (shard.frames_fh, shard.idx_fh) for shard in shards
        }
        self._schema_version = schema_version
        self._dropped = 0
        self._results_dropped = 0
        self._processed = 0
        self._thread = threading.Thread(
            target=self._run,
            name="frame-writer",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, job: _FrameWriteJob) -> bool:
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            self._dropped += 1
            return False
        return True

    def get_nowait(self) -> _FrameWriteResult | None:
        try:
            item = self._results.get_nowait()
        except queue.Empty:
            return None
        if item is _FRAME_WRITE_STOP:
            return None
        return item

    def close(self, timeout_seconds: float = 5.0) -> None:
        try:
            self._queue.put(_FRAME_WRITE_STOP, timeout=timeout_seconds)
        except queue.Full:
            return
        self._thread.join(timeout=timeout_seconds)

    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "results_size": self._results.qsize(),
            "dropped": self._dropped,
            "results_dropped": self._results_dropped,
            "processed": self._processed,
        }

    def queue_size(self) -> int:
        return self._queue.qsize()

    def results_size(self) -> int:
        return self._results.qsize()

    def _run(self) -> None:
        while True:
            job = self._queue.get()
            if job is _FRAME_WRITE_STOP:
                with contextlib.suppress(queue.Full):
                    self._results.put_nowait(_FRAME_WRITE_STOP)
                break
            assert isinstance(job, _FrameWriteJob)
            handles = self._handles.get(job.shard_id)
            if handles is None:
                continue
            frames_fh, idx_fh = handles
            write_start_ns = monotonic_ns()
            record = append_record(
                frames_fh,
                idx_fh,
                job.payload_bytes,
                job.rx_mono_ns,
                job.rx_wall_ns_utc,
                flags=job.flags,
                schema_version=self._schema_version,
            )
            write_end_ns = monotonic_ns()
            backpressure_ns = max(0, write_start_ns - job.rx_mono_ns)
            ingest_latency_ns = write_end_ns - job.rx_mono_ns
            header_struct = frames_header_struct(record.schema_version)
            if record.schema_version == 1:
                header_bytes = header_struct.pack(
                    frames_magic(record.schema_version),
                    record.schema_version,
                    record.flags,
                    record.rx_mono_ns,
                    record.payload_len,
                    record.payload_crc32,
                )
            else:
                header_bytes = header_struct.pack(
                    frames_magic(record.schema_version),
                    record.schema_version,
                    record.flags,
                    record.rx_mono_ns,
                    record.rx_wall_ns_utc,
                    record.payload_len,
                    record.payload_crc32,
                )
            result = _FrameWriteResult(
                shard_id=job.shard_id,
                payload_bytes=record.payload,
                header_bytes=header_bytes,
                schema_version=record.schema_version,
                payload_len=record.payload_len,
                rx_mono_ns=record.rx_mono_ns,
                write_duration_ns=write_end_ns - write_start_ns,
                ingest_latency_ns=ingest_latency_ns,
                backpressure_ns=backpressure_ns,
            )
            try:
                self._results.put_nowait(result)
                self._processed += 1
            except queue.Full:
                self._results_dropped += 1


class _NdjsonWriter:
    def __init__(
        self,
        *,
        max_queue: int = 10000,
        flush_interval_seconds: float = 0.5,
        batch_size: int = 200,
    ) -> None:
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._flush_interval_seconds = flush_interval_seconds
        self._batch_size = batch_size
        self._dropped = 0
        self._thread = threading.Thread(
            target=self._run,
            name="ndjson-writer",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, path: Path, record: dict[str, Any]) -> bool:
        try:
            self._queue.put_nowait((str(path), record))
        except queue.Full:
            self._dropped += 1
            return False
        return True

    def close(self, timeout_seconds: float = 5.0) -> None:
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_IO_WRITER_STOP)
        self._thread.join(timeout=timeout_seconds)

    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "dropped": self._dropped,
        }

    def _run(self) -> None:
        files: dict[str, Any] = {}
        pending: dict[str, list[bytes]] = {}
        pending_count = 0
        last_flush = time.time()
        try:
            while True:
                item = None
                try:
                    item = self._queue.get(timeout=self._flush_interval_seconds)
                except queue.Empty:
                    item = None
                if item is _IO_WRITER_STOP:
                    self._flush_pending(pending, files)
                    break
                if item is not None:
                    path_text, record = item
                    line = orjson.dumps(record, option=_ORJSON_NDJSON_OPTIONS)
                    pending.setdefault(path_text, []).append(line)
                    pending_count += 1
                now = time.time()
                if (
                    pending_count >= self._batch_size
                    or now - last_flush >= self._flush_interval_seconds
                ):
                    if pending_count:
                        self._flush_pending(pending, files)
                        pending_count = 0
                    last_flush = now
        finally:
            self._flush_pending(pending, files)
            for handle in files.values():
                with contextlib.suppress(Exception):
                    handle.flush()
                    handle.close()

    @staticmethod
    def _flush_pending(pending: dict[str, list[bytes]], files: dict[str, Any]) -> None:
        for path_text, lines in list(pending.items()):
            if not lines:
                continue
            handle = files.get(path_text)
            if handle is None:
                path = Path(path_text)
                path.parent.mkdir(parents=True, exist_ok=True)
                handle = path.open("ab")
                files[path_text] = handle
            handle.write(b"".join(lines))
            handle.flush()
        pending.clear()


def _default_user_agent_header() -> str | None:
    with contextlib.suppress(Exception):
        import websockets.http11 as http11

        user_agent = getattr(http11, "USER_AGENT", None)
        if user_agent:
            return user_agent
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        with contextlib.suppress(Exception):
            from websockets.legacy.client import USER_AGENT

            return USER_AGENT
    return None


DEFAULT_USER_AGENT_HEADER = _default_user_agent_header()
CONNECT_SUPPORTS_CLOSE_TIMEOUT = (
    "close_timeout" in inspect.signature(websockets.connect).parameters
)
DEFAULT_WS_CLOSE_TIMEOUT_SECONDS = 5.0


FATAL_LOW_DISK = "LOW_DISK"
FATAL_DROP = "DROP"
FATAL_LATENCY = "LATENCY_FATAL"
FATAL_BACKPRESSURE = "BACKPRESSURE_STALL"
FATAL_WRITE_QUEUE = "WRITE_QUEUE_FULL"
FATAL_RECONNECT_STORM = "RECONNECT_STORM"
FATAL_VERIFY = "VERIFY_CORRUPTION"
FATAL_SUBSCRIBE_CONFIRM = "SUBSCRIBE_CONFIRM_FAIL"
FATAL_MONO_QUANTIZED = "MONO_TIME_QUANTIZED"
FATAL_INTERNAL = "INTERNAL_ASSERT"
FATAL_CHURN_GUARD = "CHURN_GUARD_SUSTAINED"


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
class CaptureState:
    run: RunBootstrap
    config: Config
    shards: list[ShardState]
    pinned_tokens: list[str]
    universe: UniverseState
    fee_rate_client: FeeRateClient | None = None
    policy_selector: PolicySelector | None = None
    fee_regime_monitor: FeeRegimeMonitor | None = None
    refresh_executor: ThreadPoolExecutor | None = None
    refresh_worker: "UniverseRefreshWorker | None" = None
    io_writer: _NdjsonWriter | None = None
    parse_worker: _ParseWorker | None = None
    parse_dropped_total: int = 0
    frame_writer: _FrameWriter | None = None
    frame_write_dropped_total: int = 0
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


def _mark_reconnecting(shard: ShardState) -> None:
    shard.confirmed = False
    shard.confirm_events_seen = 0
    shard.confirm_deadline_mono_ns = None
    shard.last_subscribe_variant = None
    shard.last_subscribe_group_index = None


class _DataIdleTimeout(TimeoutError):
    pass


def _normalize_ws_keepalive(config: Config) -> tuple[float | None, float | None, float | None]:
    ping_interval = config.ws_ping_interval_seconds
    if ping_interval <= 0:
        ping_interval = None
    ping_timeout = config.ws_ping_timeout_seconds
    if ping_timeout <= 0:
        ping_timeout = None
    data_idle_reconnect = config.ws_data_idle_reconnect_seconds
    if data_idle_reconnect <= 0:
        data_idle_reconnect = None
    return ping_interval, ping_timeout, data_idle_reconnect


def _looks_like_ping_timeout(exc: Exception) -> bool:
    reason = getattr(exc, "reason", None)
    if reason:
        reason_text = str(reason).lower()
        if "ping" in reason_text and "timeout" in reason_text:
            return True
    message = str(exc).lower()
    return "ping" in message and "timeout" in message


def _close_was_clean(exc: Exception) -> bool | None:
    if isinstance(exc, websockets.exceptions.ConnectionClosedOK):
        return True
    if isinstance(exc, websockets.exceptions.ConnectionClosedError):
        return False
    return None


def _extract_close_details(exc: Exception) -> tuple[int | None, str | None, bool | None]:
    if isinstance(exc, websockets.exceptions.ConnectionClosed):
        return (
            getattr(exc, "code", None),
            getattr(exc, "reason", None),
            _close_was_clean(exc),
        )
    return None, None, None


def _classify_reconnect_trigger(exc: Exception) -> str:
    if isinstance(exc, _DataIdleTimeout):
        return "data_idle_timeout"
    if isinstance(exc, TimeoutError) and str(exc) == "subscribe confirmation timeout":
        return "confirm_timeout"
    if isinstance(exc, websockets.exceptions.ConnectionClosed):
        if _looks_like_ping_timeout(exc):
            return "ping_timeout"
        return "exception"
    return "exception"


def _apply_churn_guard_policy(
    current_interval: float,
    baseline_interval: float,
    max_interval: float,
    *,
    guard_triggered: bool,
    guard_count: int,
    fatal_threshold: int,
) -> tuple[float, int, bool]:
    if guard_triggered:
        base = current_interval if current_interval > 0 else baseline_interval
        next_interval = min(max_interval, base * 2)
        next_guard_count = guard_count + 1
        return next_interval, next_guard_count, next_guard_count >= fatal_threshold
    return baseline_interval, 0, False


@dataclass(frozen=True)
class ExpectedChurnSummary:
    expected_churn: bool
    reason: str
    rollover_window_hit: bool
    expiry_window_hit: bool
    expected_count: int
    total_count: int


def _cadence_window_seconds(config: Config, cadence_bucket: str) -> float:
    if cadence_bucket == "5m":
        return config.capture_expected_churn_window_seconds_5m
    if cadence_bucket == "15m":
        return config.capture_expected_churn_window_seconds_15m
    if cadence_bucket == "30m":
        return config.capture_expected_churn_window_seconds_30m
    if cadence_bucket == "60m":
        return config.capture_expected_churn_window_seconds_60m
    return 0.0


def _is_within_cadence_window(
    now_utc: datetime,
    cadence_bucket: str,
    window_seconds: float,
) -> bool:
    if window_seconds <= 0:
        return False
    interval_seconds = {
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "60m": 3600,
    }.get(cadence_bucket)
    if interval_seconds is None:
        return False
    seconds_since_hour = (
        now_utc.minute * 60
        + now_utc.second
        + (now_utc.microsecond / 1_000_000.0)
    )
    offset = seconds_since_hour % interval_seconds
    distance = min(offset, interval_seconds - offset)
    return distance <= window_seconds


def _classify_expected_churn(
    now_utc: datetime,
    segment: SegmentTag,
    config: Config,
) -> tuple[bool, str, bool, bool]:
    expiry_window_ns = int(config.capture_expected_churn_expiry_window_seconds * 1_000_000_000)
    end_ns = segment.end_wall_ns_utc
    if end_ns is not None and expiry_window_ns > 0:
        now_ns = int(now_utc.timestamp() * 1_000_000_000)
        if abs(now_ns - end_ns) <= expiry_window_ns:
            return True, "EXPECTED_EXPIRY", False, True
    cadence_bucket = segment.cadence_bucket
    window_seconds = _cadence_window_seconds(config, cadence_bucket)
    if cadence_bucket != "unknown" and _is_within_cadence_window(
        now_utc, cadence_bucket, window_seconds
    ):
        reason = "EXPECTED_ROLLOVER_1H" if cadence_bucket == "60m" else "EXPECTED_ROLLOVER_15M"
        return True, reason, True, False
    return False, "UNKNOWN", False, False


def _expected_churn_for_refresh(
    now_wall_ns_utc: int,
    removed_market_ids: set[str],
    added_market_ids: set[str],
    current_segments: dict[str, SegmentTag],
    next_segments: dict[str, SegmentTag],
    config: Config,
) -> ExpectedChurnSummary:
    if not config.capture_expected_churn_enable:
        return ExpectedChurnSummary(
            expected_churn=False,
            reason="UNKNOWN",
            rollover_window_hit=False,
            expiry_window_hit=False,
            expected_count=0,
            total_count=0,
        )
    now_utc = datetime.fromtimestamp(now_wall_ns_utc / 1_000_000_000, tz=timezone.utc)
    expected_count = 0
    total_count = 0
    reasons: dict[str, int] = {}
    rollover_hit = False
    expiry_hit = False
    for market_id in removed_market_ids:
        segment = current_segments.get(str(market_id))
        if segment is None:
            continue
        expected, reason, roll_hit, exp_hit = _classify_expected_churn(
            now_utc, segment, config
        )
        total_count += 1
        if expected:
            expected_count += 1
            reasons[reason] = reasons.get(reason, 0) + 1
        rollover_hit = rollover_hit or roll_hit
        expiry_hit = expiry_hit or exp_hit
    for market_id in added_market_ids:
        segment = next_segments.get(str(market_id))
        if segment is None:
            continue
        expected, reason, roll_hit, exp_hit = _classify_expected_churn(
            now_utc, segment, config
        )
        total_count += 1
        if expected:
            expected_count += 1
            reasons[reason] = reasons.get(reason, 0) + 1
        rollover_hit = rollover_hit or roll_hit
        expiry_hit = expiry_hit or exp_hit
    expected_churn = False
    if total_count > 0:
        expected_churn = (expected_count / total_count) >= config.capture_expected_churn_min_ratio
    reason = "UNKNOWN"
    if expected_churn and reasons:
        reason = max(reasons.items(), key=lambda item: (item[1], item[0]))[0]
    return ExpectedChurnSummary(
        expected_churn=expected_churn,
        reason=reason,
        rollover_window_hit=rollover_hit,
        expiry_window_hit=expiry_hit,
        expected_count=expected_count,
        total_count=total_count,
    )


def _monotonic_precision_stats(sample_count: int) -> dict[str, Any]:
    if sample_count < 2:
        raise ValueError("sample_count must be >= 2")
    stamps = [monotonic_ns() for _ in range(sample_count)]
    deltas = [
        later - earlier
        for earlier, later in zip(stamps, stamps[1:])
        if later > earlier
    ]
    any_non_ms = any(stamp % 1_000_000 != 0 for stamp in stamps)
    stats: dict[str, Any] = {
        "sample_count": sample_count,
        "any_non_ms": any_non_ms,
    }
    if not deltas:
        stats["ok"] = False
        stats["reason"] = "no_positive_deltas"
        return stats
    delta_gcd = deltas[0]
    for delta in deltas[1:]:
        delta_gcd = gcd(delta_gcd, delta)
    stats.update(
        {
            "delta_gcd_ns": delta_gcd,
            "min_delta_ns": min(deltas),
            "max_delta_ns": max(deltas),
        }
    )
    stats["ok"] = any_non_ms and delta_gcd < 1_000_000
    if not stats["ok"]:
        stats["reason"] = "quantized"
    return stats


def _config_snapshot(config: Config) -> dict[str, Any]:
    snapshot = {}
    for field in fields(Config):
        snapshot[field.name] = getattr(config, field.name)
    return snapshot


def _snapshot_for_manifest(snapshot: UniverseSnapshot) -> dict[str, Any]:
    payload = asdict(snapshot)
    payload.pop("selected_markets", None)
    return payload


def _read_git_commit() -> str:
    git_dir = Path.cwd() / ".git"
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return "unknown"
    head = head_path.read_text(encoding="utf-8").strip()
    if head.startswith("ref:"):
        ref = head.split(" ", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
    return head or "unknown"


def _environment_metadata() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
    }


def _install_signal_handlers(state: CaptureState) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop(sig: signal.Signals) -> None:
        if state.stop_event.is_set():
            return
        state.stop_reason = sig.name
        loop.call_soon_threadsafe(state.stop_event.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, sig)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, lambda *_args, _sig=sig: _request_stop(_sig))
            except (ValueError, AttributeError):
                continue


def _build_fee_rate_client(config: Config) -> FeeRateClient:
    return FeeRateClient(
        base_url=config.fee_rate_base_url,
        timeout_seconds=config.fee_rate_timeout_seconds,
        cache_ttl_seconds=config.fee_rate_cache_ttl_seconds,
        max_in_flight=config.fee_rate_max_in_flight,
    )


def _build_policy_selector(config: Config) -> PolicySelector:
    policy_map = parse_policy_rules(config.policy_rules_json, config.policy_default_id)
    return PolicySelector(policy_map)


def _build_fee_regime_monitor(config: Config) -> FeeRegimeMonitor:
    expected_values = parse_expected_fee_rate_bps_values(
        config.fee_regime_expected_fee_rate_bps_values
    )
    return FeeRegimeMonitor(
        expect_15m_crypto_fee_enabled=config.fee_regime_expect_15m_crypto_fee_enabled,
        expect_other_fee_free=config.fee_regime_expect_other_fee_free,
        expect_unknown_fee_free=config.fee_regime_expect_unknown_fee_free,
        expected_fee_rate_bps_values=expected_values,
    )


def _parse_canary_tokens(raw: str) -> list[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _segment_policy_summary(
    selector: PolicySelector,
    segments_by_token_id: dict[str, SegmentTag],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for segment in segments_by_token_id.values():
        segment_key = (
            segment.cadence_bucket,
            segment.is_crypto,
            segment.fee_enabled,
            segment.fee_rate_known,
        )
        policy_id = selector.select(segment)
        group_key = (segment_key, policy_id)
        entry = grouped.get(group_key)
        if entry is None:
            entry = {
                "segment_key": {
                    "cadence_bucket": segment.cadence_bucket,
                    "is_crypto": segment.is_crypto,
                    "fee_enabled": segment.fee_enabled,
                    "fee_rate_known": segment.fee_rate_known,
                },
                "policy_id": policy_id,
                "token_count": 0,
            }
            grouped[group_key] = entry
        entry["token_count"] += 1
    ordered = sorted(
        grouped.values(),
        key=lambda item: (
            item["policy_id"],
            str(item["segment_key"]["cadence_bucket"]),
            str(item["segment_key"]["is_crypto"]),
        ),
    )
    return ordered


def _stable_hash(token_id: str) -> int:
    digest = hashlib.sha256(token_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


def _coerce_numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _token_score_map(markets: list[dict[str, Any]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    numeric_keys = ("volume", "volumeNum", "liquidity", "liquidityNum")
    for market in markets:
        score: float | None = None
        for key in numeric_keys:
            score = _coerce_numeric(market.get(key))
            if score is not None:
                break
        if score is None:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_ids:
            current = scores.get(token_id)
            if current is None or score > current:
                scores[token_id] = score
    return scores


def _select_confirm_tokens(
    shard_tokens: list[str],
    scores: dict[str, float],
    max_tokens: int,
) -> list[str]:
    if not shard_tokens:
        return []
    scored: list[tuple[float, str]] = []
    for token_id in shard_tokens:
        score = scores.get(token_id)
        if score is not None:
            scored.append((score, token_id))
    if scored:
        scored.sort(key=lambda item: (-item[0], item[1]))
        ordered = [token_id for _, token_id in scored]
    else:
        ordered = sorted(shard_tokens, key=lambda token_id: (_stable_hash(token_id), token_id))
    return ordered[: min(max_tokens, len(ordered))]


def assign_shards_by_token(
    token_ids: list[str],
    shard_count: int,
) -> dict[int, list[str]]:
    if shard_count <= 0:
        raise ValueError("shard_count must be >= 1")
    shards: dict[int, list[str]] = {idx: [] for idx in range(shard_count)}
    seen: set[str] = set()
    for token_id in token_ids:
        token_key = str(token_id)
        if token_key in seen:
            continue
        seen.add(token_key)
        shard_id = _stable_hash(token_key) % shard_count
        shards[shard_id].append(token_key)
    return shards


def split_subscribe_groups(
    token_ids: list[str],
    max_tokens: int,
    max_bytes: int,
    variant: str,
) -> list[list[str]]:
    groups: list[list[str]] = []
    current: list[str] = []
    for token_id in token_ids:
        candidate = current + [token_id]
        payload = build_subscribe_payload(variant, candidate)
        payload_bytes = orjson.dumps(payload)
        if len(candidate) > max_tokens or len(payload_bytes) > max_bytes:
            if not current:
                raise ValueError("single subscribe payload exceeds limits")
            groups.append(current)
            current = [token_id]
        else:
            current = candidate
    if current:
        groups.append(current)
    return groups


def _sorted_unique_tokens(token_ids: Iterable[str]) -> list[str]:
    return sorted({str(token_id) for token_id in token_ids if str(token_id)})


def _compute_refresh_delta(
    current_tokens: set[str],
    desired_tokens: set[str],
) -> tuple[set[str], set[str]]:
    added = desired_tokens - current_tokens
    removed = current_tokens - desired_tokens
    return added, removed


def _should_apply_refresh(
    added: set[str],
    removed: set[str],
    min_delta_tokens: int,
) -> bool:
    return (len(added) + len(removed)) >= min_delta_tokens


def _build_shard_targets(
    token_ids: list[str],
    config: Config,
    *,
    target_version: int,
    scores: dict[str, float] | None = None,
) -> dict[int, ShardTarget]:
    plans = _build_shard_target_plans(
        token_ids,
        config,
        target_version=target_version,
        scores=scores,
    )
    return _materialize_shard_targets(plans)


def _build_shard_target_plans(
    token_ids: list[str],
    config: Config,
    *,
    target_version: int,
    scores: dict[str, float] | None = None,
) -> dict[int, ShardTargetPlan]:
    ordered_tokens = _sorted_unique_tokens(token_ids)
    shard_map = assign_shards_by_token(ordered_tokens, config.ws_shards)
    if scores is None:
        scores = {}
    targets: dict[int, ShardTargetPlan] = {}
    for shard_id, tokens in shard_map.items():
        groups = split_subscribe_groups(
            tokens,
            config.ws_subscribe_max_tokens,
            config.ws_subscribe_max_bytes,
            "A",
        )
        targets[shard_id] = ShardTargetPlan(
            token_ids=tokens,
            groups=groups,
            confirm_token_ids=_select_confirm_tokens(
                tokens,
                scores,
                config.capture_confirm_tokens_per_shard,
            ),
            target_version=target_version,
        )
    return targets


def _materialize_shard_targets(
    targets: dict[int, ShardTargetPlan],
) -> dict[int, ShardTarget]:
    materialized: dict[int, ShardTarget] = {}
    for shard_id, target in targets.items():
        materialized[shard_id] = ShardTarget(
            token_ids=target.token_ids,
            groups=target.groups,
            confirm_token_ids=target.confirm_token_ids,
            target_version=target.target_version,
        )
    return materialized


def _select_changed_shards(
    current_targets: dict[int, ShardTarget],
    next_targets: dict[int, ShardTarget],
) -> dict[int, ShardTarget]:
    changed: dict[int, ShardTarget] = {}
    for shard_id, next_target in next_targets.items():
        current = current_targets.get(shard_id)
        if current is None or set(current.token_ids) != set(next_target.token_ids):
            changed[shard_id] = next_target
    return changed


def _payload_bytes(raw: Any) -> tuple[bytes, int]:
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw), FLAG_BINARY_PAYLOAD
    if isinstance(raw, str):
        return raw.encode("utf-8"), FLAG_TEXT_PAYLOAD
    return str(raw).encode("utf-8"), FLAG_TEXT_PAYLOAD


def _iter_items(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
    elif isinstance(payload, dict):
        yield payload


def _get_token_id(item: dict[str, Any]) -> str | None:
    token_id = item.get("asset_id")
    if token_id is None:
        token_id = item.get("token_id") or item.get("tokenId") or item.get("assetId")
    if token_id is None:
        return None
    return str(token_id)


def _get_msg_type(item: dict[str, Any]) -> str:
    msg_type = item.get("type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    msg_type = item.get("event_type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    if "bids" in item or "asks" in item:
        return "book"
    if "price_changes" in item:
        return "price_change"
    return "unknown"


def _extract_minimal_fields(payload: Any) -> tuple[list[tuple[str, str]], dict[str, int]]:
    token_pairs: list[tuple[str, str]] = []
    msg_type_counts: dict[str, int] = {}
    for item in _iter_items(payload):
        msg_type = _get_msg_type(item)
        msg_type_counts[msg_type] = msg_type_counts.get(msg_type, 0) + 1
        token_id = _get_token_id(item)
        if token_id is not None:
            token_pairs.append((token_id, msg_type))
        price_changes = item.get("price_changes")
        if isinstance(price_changes, list):
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                change_token_id = _get_token_id(change)
                if change_token_id is None:
                    continue
                token_pairs.append((change_token_id, msg_type))
    return token_pairs, msg_type_counts


def _load_pinned_markets(
    config: Config,
) -> tuple[list[dict[str, Any]], list[str], str, str | None, list[dict[str, Any]]]:
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=config.capture_max_markets,
    )
    selected_markets = select_active_binary_markets(
        markets, max_markets=config.capture_max_markets
    )
    universe_mode = "active-binary"
    market_regex_effective: str | None = None
    selected: list[dict[str, Any]] = []
    tokens: set[str] = set()
    for market in selected_markets:
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        selected.append({"id": market.get("id"), "token_ids": [token_a, token_b]})
        tokens.add(token_a)
        tokens.add(token_b)
    return selected, sorted(tokens), universe_mode, market_regex_effective, selected_markets


def _snapshot_from_selected_markets(
    config: Config,
    selected_markets: list[dict[str, Any]],
    *,
    universe_version: int,
) -> UniverseSnapshot:
    market_ids: list[str] = []
    token_ids: list[str] = []
    token_seen: set[str] = set()
    for market in selected_markets:
        market_id = market.get("id")
        if market_id is None:
            continue
        market_ids.append(str(market_id))
        token_list = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_list:
            token_key = str(token_id)
            if token_key in token_seen:
                continue
            token_seen.add(token_key)
            token_ids.append(token_key)
    return UniverseSnapshot(
        universe_version=universe_version,
        market_ids=market_ids,
        token_ids=token_ids,
        created_wall_ns_utc=time.time_ns(),
        created_mono_ns=monotonic_ns(),
        selection={
            "max_markets": config.capture_max_markets,
            "filters_enabled": False,
        },
        selected_markets=selected_markets,
    )


def _load_startup_universe(
    config: Config,
) -> tuple[UniverseSnapshot, list[str], list[dict[str, Any]], list[dict[str, Any]], str]:
    snapshot = compute_desired_universe(config, universe_version=1)
    pinned_tokens = list(snapshot.token_ids)
    universe_mode = "active-binary"
    if snapshot.selected_markets is not None:
        selected_markets = list(snapshot.selected_markets)
    else:
        markets = fetch_markets(
            config.gamma_base_url,
            config.rest_timeout,
            limit=config.gamma_limit,
            max_markets=config.capture_max_markets,
        )
        selected_markets = select_active_binary_markets(
            markets,
            max_markets=config.capture_max_markets,
        )
    market_by_id = {
        str(market["id"]): market
        for market in selected_markets
        if market.get("id") is not None
    }
    pinned_markets: list[dict[str, Any]] = []
    ordered_selected_markets: list[dict[str, Any]] = []
    for market_id in snapshot.market_ids:
        market = market_by_id.get(str(market_id))
        if market is None:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        pinned_markets.append({"id": market_id, "token_ids": [token_a, token_b]})
        ordered_selected_markets.append(market)
    return snapshot, pinned_tokens, pinned_markets, ordered_selected_markets, universe_mode


def _coverage_pct(
    token_ids: list[str],
    last_seen: dict[str, int],
    now_ns: int,
    window_ns: int | None,
    *,
    token_added_mono_ns: dict[str, int] | None = None,
    grace_ns: int | None = None,
) -> float:
    eligible = _eligible_token_ids(
        token_ids, last_seen, token_added_mono_ns, now_ns, grace_ns
    )
    if not eligible:
        return 100.0
    seen = 0
    for token_id in eligible:
        ts = last_seen.get(token_id)
        if ts is None:
            continue
        if window_ns is None or now_ns - ts <= window_ns:
            seen += 1
    return 100.0 * seen / len(eligible)


def _missing_tokens(
    token_ids: list[str],
    last_seen: dict[str, int],
    now_ns: int,
    window_ns: int | None,
    *,
    token_added_mono_ns: dict[str, int] | None = None,
    grace_ns: int | None = None,
) -> list[dict[str, Any]]:
    missing: list[dict[str, Any]] = []
    eligible = _eligible_token_ids(
        token_ids, last_seen, token_added_mono_ns, now_ns, grace_ns
    )
    for token_id in eligible:
        ts = last_seen.get(token_id)
        if ts is None:
            missing.append({"token_id": token_id, "last_seen_mono_ns": None})
        elif window_ns is not None and now_ns - ts > window_ns:
            missing.append({"token_id": token_id, "last_seen_mono_ns": ts})
    return missing


def _eligible_token_ids(
    token_ids: list[str],
    last_seen: dict[str, int],
    token_added_mono_ns: dict[str, int] | None,
    now_ns: int,
    grace_ns: int | None,
) -> list[str]:
    if not token_ids:
        return []
    if token_added_mono_ns is None or not token_added_mono_ns or not grace_ns or grace_ns <= 0:
        return list(token_ids)
    eligible: list[str] = []
    to_remove: list[str] = []
    for token_id in token_ids:
        added_ns = token_added_mono_ns.get(token_id)
        if added_ns is None:
            eligible.append(token_id)
            continue
        if token_id in last_seen or now_ns - added_ns >= grace_ns:
            eligible.append(token_id)
            to_remove.append(token_id)
    for token_id in to_remove:
        token_added_mono_ns.pop(token_id, None)
    return eligible


KEEPALIVE_PAYLOAD = object()


def _fast_confirm_event(payload_bytes: bytes) -> bool:
    if payload_bytes in (b"PONG", b"PING"):
        return False
    return True


def _confirm_event_from_payload(payload_bytes: bytes) -> tuple[bool, Any | None]:
    if payload_bytes in (b"PONG", b"PING"):
        return False, KEEPALIVE_PAYLOAD
    try:
        payload = orjson.loads(payload_bytes)
    except orjson.JSONDecodeError:
        return False, None
    if isinstance(payload, list) and not payload:
        return False, payload
    return True, payload


def _ring_header_sample(shard: ShardState, max_entries: int) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for entry in list(shard.ring)[-max_entries:]:
        header_bytes = entry[0] if isinstance(entry, tuple) else entry
        if len(header_bytes) < 8:
            continue
        magic = header_bytes[:8]
        try:
            schema_version = 1 if magic == frames_magic(1) else 2 if magic == frames_magic(2) else None
            if schema_version is None:
                continue
            header_struct = frames_header_struct(schema_version)
            header_len = header_struct.size
            if len(header_bytes) < header_len:
                continue
            if schema_version == 1:
                magic, schema_field, flags, rx_mono_ns, payload_len, payload_crc32 = (
                    header_struct.unpack(header_bytes)
                )
                rx_wall_ns_utc = 0
            else:
                (
                    magic,
                    schema_field,
                    flags,
                    rx_mono_ns,
                    rx_wall_ns_utc,
                    payload_len,
                    payload_crc32,
                ) = header_struct.unpack(header_bytes)
            if schema_field != schema_version:
                continue
        except struct.error:
            continue
        samples.append(
            {
                "raw_header_hex": header_bytes.hex(),
                "magic": magic.decode("ascii", "ignore"),
                "schema_version": schema_version,
                "flags": flags,
                "rx_mono_ns": rx_mono_ns,
                "rx_wall_ns_utc": rx_wall_ns_utc,
                "payload_len": payload_len,
                "payload_crc32": payload_crc32,
            }
        )
    return samples


def _cap_missing_tokens(
    missing: list[dict[str, Any]],
    *,
    max_tokens: int,
) -> dict[str, Any]:
    return {
        "count": len(missing),
        "sample": missing[:max_tokens],
    }


def _build_missing_tokens_dump(
    state: CaptureState,
    reason: str,
    now_ns: int,
    *,
    include_missing_list: bool,
    window_ns: int | None,
) -> dict[str, Any]:
    per_shard: dict[str, Any] = {}
    last_seen_global: dict[str, int] = {}
    for shard in state.shards:
        last_seen_global.update(shard.last_seen)
        deadline = shard.confirm_deadline_mono_ns
        per_shard[str(shard.shard_id)] = {
            "confirmed": shard.confirmed,
            "confirm_events_seen": shard.confirm_events_seen,
            "confirm_deadline_mono_ns": deadline,
            "confirm_deadline_exceeded": deadline is not None and now_ns > deadline,
            "reconnects": shard.reconnects,
            "confirm_token_ids": shard.confirm_token_ids,
            "recent_headers": _ring_header_sample(shard, max_entries=10),
        }
    grace_ns = int(state.config.capture_universe_refresh_grace_seconds * 1_000_000_000)
    missing_global = _missing_tokens(
        state.pinned_tokens,
        last_seen_global,
        now_ns,
        window_ns,
        token_added_mono_ns=state.universe.token_added_mono_ns,
        grace_ns=grace_ns,
    )
    if include_missing_list:
        global_missing = missing_global
    else:
        global_missing = _cap_missing_tokens(missing_global, max_tokens=200)
    return {
        "reason": reason,
        "global_missing": global_missing,
        "per_shard": per_shard,
    }


def _write_runlog(run_dir: Path, record: dict[str, Any]) -> None:
    normalized = _normalize_orjson(record)
    if _IO_WRITER is not None:
        _IO_WRITER.enqueue(run_dir / "runlog.ndjson", normalized)
        return
    _append_ndjson(run_dir / "runlog.ndjson", normalized)


def _write_metrics(path: Path, record: dict[str, Any]) -> None:
    normalized = _normalize_orjson(record)
    if _IO_WRITER is not None:
        _IO_WRITER.enqueue(path, normalized)
        return
    _append_ndjson(path, normalized)


def _write_startup_fatal(
    run: RunBootstrap,
    reason: str,
    message: str,
    *,
    extra: dict[str, Any] | None = None,
) -> None:
    _write_runlog(
        run.run_dir,
        {
            "record_type": "fatal",
            "run_id": run.run_id,
            "fatal_reason": reason,
            "fatal_message": message,
        },
    )
    payload = {
        "fatal_reason": reason,
        "fatal_message": message,
        "run_id": run.run_id,
    }
    if extra:
        payload.update(extra)
    _write_json(run.run_dir / "fatal.json", payload)


def _all_shards_confirmed(state: CaptureState) -> bool:
    return all(shard.confirmed for shard in state.shards)


def _confirm_failure_message(shard: ShardState, config: Config) -> str:
    variant = shard.last_subscribe_variant or "unknown"
    group_index = (
        "unknown"
        if shard.last_subscribe_group_index is None
        else str(shard.last_subscribe_group_index)
    )
    return (
        "confirmation deadline exceeded "
        f"shard_id={shard.shard_id} "
        f"failures={shard.confirm_failures} "
        f"timeout_seconds={config.capture_confirm_timeout_seconds} "
        f"variant={variant} "
        f"group_index={group_index}"
    )


async def _handle_confirm_deadline_miss(
    state: CaptureState,
    shard: ShardState,
    *,
    variant: str,
    now_ns: int,
) -> None:
    shard.confirm_failures += 1
    shard.subscribe_variant_index += 1
    if shard.subscribe_variant_locked == variant:
        shard.subscribe_variant_locked = None
    _write_runlog(
        state.run.run_dir,
        {
            "record_type": "subscribe_confirm_fail",
            "run_id": state.run.run_id,
            "shard_id": shard.shard_id,
            "variant": variant,
            "confirm_events_seen": shard.confirm_events_seen,
        },
    )
    if shard.confirm_failures > state.config.capture_confirm_max_failures:
        message = _confirm_failure_message(shard, state.config)
        missing = _build_missing_tokens_dump(
            state,
            FATAL_SUBSCRIBE_CONFIRM,
            now_ns,
            include_missing_list=False,
            window_ns=None,
        )
        await _trigger_fatal(
            state,
            FATAL_SUBSCRIBE_CONFIRM,
            message,
            missing_tokens=missing,
        )
    raise TimeoutError("subscribe confirmation timeout")


async def _check_backpressure_fatal(
    state: CaptureState,
    now_ns: int,
    global_backpressure_p99: int,
) -> None:
    threshold_ns = int(state.config.capture_backpressure_fatal_ms * 1_000_000)
    if threshold_ns <= 0:
        return
    if global_backpressure_p99 > threshold_ns:
        state.backpressure_breach_count += 1
    else:
        state.backpressure_breach_count = 0
    if state.backpressure_breach_count < 3:
        return
    missing = _build_missing_tokens_dump(
        state,
        FATAL_BACKPRESSURE,
        now_ns,
        include_missing_list=False,
        window_ns=None,
    )
    missing["backpressure_ns_p99"] = global_backpressure_p99
    await _trigger_fatal(
        state,
        FATAL_BACKPRESSURE,
        "backpressure p99 exceeded threshold",
        missing_tokens=missing,
    )


async def _trigger_fatal(
    state: CaptureState,
    reason: str,
    message: str,
    *,
    first_error: dict[str, Any] | None = None,
    missing_tokens: dict[str, Any] | None = None,
) -> None:
    async with state.fatal_lock:
        if state.fatal_event.is_set():
            return
        state.fatal_event.set()
        _write_runlog(
            state.run.run_dir,
            {
                "record_type": "fatal",
                "run_id": state.run.run_id,
                "fatal_reason": reason,
                "fatal_message": message,
            },
        )
        fatal_record: dict[str, Any] = {
            "fatal_reason": reason,
            "fatal_message": message,
            "run_id": state.run.run_id,
            "recent_heartbeats": list(state.heartbeat_samples),
        }
        if first_error:
            fatal_record.update(first_error)
        _write_json(state.run.run_dir / "fatal.json", fatal_record)
        if missing_tokens:
            _write_json(state.run.run_dir / "missing_tokens.json", missing_tokens)
        for shard in state.shards:
            dump_path = state.run.run_dir / f"last_frames_shard_{shard.shard_id:02d}.bin"
            chunks: list[bytes] = []
            for entry in shard.ring:
                if isinstance(entry, tuple):
                    chunks.append(entry[0])
                    chunks.append(entry[1])
                else:
                    chunks.append(entry)
            dump_path.write_bytes(b"".join(chunks))


async def _loop_lag_monitor(state: CaptureState) -> None:
    interval_ns = 100_000_000
    next_tick = monotonic_ns() + interval_ns
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        now_ns = monotonic_ns()
        sleep_ns = next_tick - now_ns
        if sleep_ns > 0:
            await asyncio.sleep(sleep_ns / 1_000_000_000)
        now_ns = monotonic_ns()
        lag_ns = max(0, now_ns - next_tick)
        state.loop_lag_samples_ms.append(lag_ns / 1_000_000.0)
        next_tick += interval_ns
        if next_tick < now_ns:
            next_tick = now_ns + interval_ns


async def _parse_results_loop(state: CaptureState) -> None:
    worker = state.parse_worker
    if worker is None:
        return
    max_batch = 500
    sleep_seconds = 0.01
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        processed = 0
        while processed < max_batch:
            result = worker.get_nowait()
            if result is None:
                break
            processed += 1
            parsed_latency_ms = max(
                0.0, (result.t_parsed_mono_ns - result.rx_mono_ns) / 1_000_000.0
            )
            _append_sample_float(
                state.ws_rx_to_parsed_ms_samples,
                parsed_latency_ms,
                state.config.capture_metrics_max_samples,
            )
            shard = state.shards[result.shard_id]
            if result.decode_error:
                shard.stats.decode_errors += 1
                continue
            shard.stats.record_counts(result.token_ids, result.msg_type_counts)
            for token_id in result.token_ids:
                shard.last_seen[token_id] = result.rx_mono_ns
                if token_id in state.universe.token_added_mono_ns:
                    state.universe.token_added_mono_ns.pop(token_id, None)
        if processed == 0:
            await asyncio.sleep(sleep_seconds)


async def _write_results_loop(state: CaptureState) -> None:
    writer = state.frame_writer
    if writer is None:
        return
    max_batch = 500
    sleep_seconds = 0.01
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        processed = 0
        while processed < max_batch:
            result = writer.get_nowait()
            if result is None:
                break
            processed += 1
            t_applied_mono_ns = monotonic_ns()
            applied_latency_ms = max(
                0.0, (t_applied_mono_ns - result.rx_mono_ns) / 1_000_000.0
            )
            _append_sample_float(
                state.ws_rx_to_applied_ms_samples,
                applied_latency_ms,
                state.config.capture_metrics_max_samples,
            )
            write_end_to_apply_ms = max(
                0.0,
                (
                    t_applied_mono_ns
                    - (result.rx_mono_ns + result.ingest_latency_ns)
                )
                / 1_000_000.0,
            )
            _append_sample_float(
                state.write_end_to_apply_ms_samples,
                write_end_to_apply_ms,
                state.config.capture_metrics_max_samples,
            )
            shard = state.shards[result.shard_id]
            shard.stats.record(
                result.payload_len,
                frames_header_len(result.schema_version),
                idx_entry_len(result.schema_version),
                result.write_duration_ns,
                result.ingest_latency_ns,
                result.backpressure_ns,
                [],
                {},
                state.config.capture_metrics_max_samples,
            )
            shard.ring.append((result.header_bytes, result.payload_bytes))
            _append_sample(
                state.write_q_depth_samples,
                writer.results_size(),
                state.config.capture_metrics_max_samples,
            )
            apply_done_mono_ns = monotonic_ns()
            apply_update_ms = max(
                0.0, (apply_done_mono_ns - t_applied_mono_ns) / 1_000_000.0
            )
            _append_sample_float(
                state.apply_update_ms_samples,
                apply_update_ms,
                state.config.capture_metrics_max_samples,
            )
        if processed == 0:
            await asyncio.sleep(sleep_seconds)


async def _heartbeat_loop(state: CaptureState) -> None:
    interval_ns = int(state.config.capture_heartbeat_interval_seconds * 1_000_000_000)
    run_dir = state.run.run_dir
    grace_ns = int(state.config.capture_universe_refresh_grace_seconds * 1_000_000_000)
    metrics_global = run_dir / "metrics" / "global.ndjson"
    metrics_shard_paths = {
        shard.shard_id: run_dir / "metrics" / f"shard_{shard.shard_id:02d}.ndjson"
        for shard in state.shards
    }
    next_tick = state.run.t0_mono_ns + interval_ns
    last_yield_ns = monotonic_ns()
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        now_ns = monotonic_ns()
        if now_ns < next_tick:
            await asyncio.sleep((next_tick - now_ns) / 1_000_000_000)
            continue
        now_ns = monotonic_ns()
        hb_wall_ns_utc = time.time_ns()
        elapsed_ns = now_ns - state.run.t0_mono_ns
        _write_runlog(
            run_dir,
            {
                "record_type": "heartbeat",
                "run_id": state.run.run_id,
                "hb_wall_ns_utc": hb_wall_ns_utc,
                "hb_mono_ns": now_ns,
            },
        )

        rx_idle_values: list[float] = []
        midrun_disconnected_time_s_total = state.midrun_disconnected_time_s_total
        midrun_disconnected_incidents = state.midrun_disconnected_incidents
        midrun_active = 0
        midrun_durations: list[float] = list(state.midrun_disconnected_durations_s)
        startup_connect_samples: list[float] = list(
            state.startup_connect_time_s_samples
        )
        startup_pending_shards = 0
        for shard in state.shards:
            if shard.last_rx_mono_ns:
                idle_s = max(
                    0.0, (now_ns - shard.last_rx_mono_ns) / 1_000_000_000.0
                )
                rx_idle_values.append(idle_s)
            if not shard.ready_once:
                startup_pending_shards += 1
                startup_connect_samples.append(
                    max(0.0, (now_ns - state.run.t0_mono_ns) / 1_000_000_000.0)
                )
            if shard.disconnected_since_mono_ns is not None and shard.ready_once:
                midrun_active += 1
                duration_s = (
                    now_ns - shard.disconnected_since_mono_ns
                ) / 1_000_000_000.0
                midrun_disconnected_time_s_total += duration_s
                midrun_durations.append(duration_s)
        midrun_incidents_total = midrun_disconnected_incidents + midrun_active
        midrun_incident_duration_s_p95 = _quantile_from_float_samples(
            midrun_durations, 95
        )
        midrun_incident_duration_s_max = (
            max(midrun_durations) if midrun_durations else 0.0
        )
        startup_connect_time_s_p50 = _quantile_from_float_samples(
            startup_connect_samples, 50
        )
        startup_connect_time_s_p95 = _quantile_from_float_samples(
            startup_connect_samples, 95
        )
        startup_connect_time_s_max = (
            max(startup_connect_samples) if startup_connect_samples else 0.0
        )
        rx_idle_any_shard_s = min(rx_idle_values) if rx_idle_values else 0.0
        rx_idle_all_shards_s = max(rx_idle_values) if rx_idle_values else 0.0

        global_frames = 0
        global_bytes = 0
        global_write_samples: list[int] = []
        global_ingest_samples: list[int] = []
        global_backpressure_samples: list[int] = []
        global_decode_errors = 0
        global_msg_type_counts: dict[str, int] = {}
        global_reconnects = 0
        global_confirm_failures = 0
        global_unconfirmed_count = 0
        global_tokens_seen: set[str] = set()
        global_coverage_pct = 0.0

        for shard in state.shards:
            shard_stats = shard.stats
            global_frames += shard_stats.frames
            global_bytes += shard_stats.bytes_written
            global_decode_errors += shard_stats.decode_errors
            global_reconnects += shard.reconnects
            global_confirm_failures += shard.confirm_failures
            if shard.confirm_deadline_mono_ns is not None and not shard.confirmed:
                global_unconfirmed_count += 1
            global_tokens_seen.update(shard_stats.token_ids)
            global_write_samples.extend(shard_stats.write_durations_ns)
            global_ingest_samples.extend(shard_stats.ingest_latencies_ns)
            global_backpressure_samples.extend(shard_stats.backpressure_ns)
            for key, value in shard_stats.msg_type_counts.items():
                global_msg_type_counts[key] = global_msg_type_counts.get(key, 0) + value

            shard_rx_idle_s = None
            if shard.last_rx_mono_ns:
                shard_rx_idle_s = max(
                    0.0, (now_ns - shard.last_rx_mono_ns) / 1_000_000_000.0
                )
            shard_startup_connect_s = (
                shard.startup_connect_time_s
                if shard.ready_once
                else max(0.0, (now_ns - state.run.t0_mono_ns) / 1_000_000_000.0)
            )
            shard_midrun_disconnected_s = shard.midrun_disconnected_time_s_total
            shard_midrun_incidents = shard.midrun_disconnected_incidents
            if shard.disconnected_since_mono_ns is not None and shard.ready_once:
                shard_midrun_disconnected_s += (
                    now_ns - shard.disconnected_since_mono_ns
                ) / 1_000_000_000.0
                shard_midrun_incidents += 1

            shard_coverage_pct = _coverage_pct(
                shard.token_ids,
                shard.last_seen,
                now_ns,
                None,
                token_added_mono_ns=state.universe.token_added_mono_ns,
                grace_ns=grace_ns,
            )
            write_quantiles = _quantiles_from_samples(
                shard_stats.write_durations_ns, (50, 95, 99)
            )
            ingest_quantiles = _quantiles_from_samples(
                shard_stats.ingest_latencies_ns, (50, 95, 99)
            )
            backpressure_quantiles = _quantiles_from_samples(
                shard_stats.backpressure_ns, (50, 95, 99)
            )
            if shard.confirm_deadline_mono_ns is None:
                deadline_remaining_ms = -1
            else:
                deadline_remaining_ms = int(
                    (shard.confirm_deadline_mono_ns - now_ns) / 1_000_000
                )
            shard_record = {
                "record_type": "heartbeat",
                "run_id": state.run.run_id,
                "shard_id": shard.shard_id,
                "hb_wall_ns_utc": hb_wall_ns_utc,
                "hb_mono_ns": now_ns,
                "elapsed_ns": elapsed_ns,
                "frames": shard_stats.frames,
                "bytes_written": shard_stats.bytes_written,
                "msgs_per_sec": shard_stats.frames
                / max(elapsed_ns / 1_000_000_000.0, 1e-9),
                "bytes_per_sec": shard_stats.bytes_written
                / max(elapsed_ns / 1_000_000_000.0, 1e-9),
                "write_ns_p50": write_quantiles[50],
                "write_ns_p95": write_quantiles[95],
                "write_ns_p99": write_quantiles[99],
                "ingest_ns_p50": ingest_quantiles[50],
                "ingest_ns_p95": ingest_quantiles[95],
                "ingest_ns_p99": ingest_quantiles[99],
                "backpressure_ns_p50": backpressure_quantiles[50],
                "backpressure_ns_p95": backpressure_quantiles[95],
                "backpressure_ns_p99": backpressure_quantiles[99],
                "coverage_pct": shard_coverage_pct,
                "token_ids_seen": len(shard_stats.token_ids),
                "token_ids_assigned": len(shard.token_ids),
                "reconnects": shard.reconnects,
                "confirm_failures": shard.confirm_failures,
                "confirmed": shard.confirmed,
                "confirm_deadline_remaining_ms": deadline_remaining_ms,
                "confirm_events_seen": shard.confirm_events_seen,
                "decode_errors": shard_stats.decode_errors,
                "msg_type_counts": shard_stats.msg_type_counts,
                "rx_idle_s": shard_rx_idle_s,
                "startup_connect_time_s": shard_startup_connect_s,
                "midrun_disconnected_time_s_total": shard_midrun_disconnected_s,
                "midrun_disconnected_incidents": shard_midrun_incidents,
            }
            _write_metrics(metrics_shard_paths[shard.shard_id], shard_record)
            now_ns = monotonic_ns()
            if now_ns - last_yield_ns >= 5_000_000:
                last_yield_ns = now_ns
                await asyncio.sleep(0)

        if state.pinned_tokens:
            last_seen_global: dict[str, int] = {}
            for shard in state.shards:
                last_seen_global.update(shard.last_seen)
            global_coverage_pct = _coverage_pct(
                state.pinned_tokens,
                last_seen_global,
                now_ns,
                None,
                token_added_mono_ns=state.universe.token_added_mono_ns,
                grace_ns=grace_ns,
            )

        fee_rate_stats = (
            state.fee_rate_client.stats_snapshot() if state.fee_rate_client else {}
        )
        parse_stats = (
            state.parse_worker.stats() if state.parse_worker is not None else {}
        )
        frame_write_stats = (
            state.frame_writer.stats() if state.frame_writer is not None else {}
        )
        enqueue_samples = list(state.ws_rx_to_enqueue_ms_samples)
        parsed_samples = list(state.ws_rx_to_parsed_ms_samples)
        applied_samples = list(state.ws_rx_to_applied_ms_samples)
        write_end_to_apply_samples = list(state.write_end_to_apply_ms_samples)
        apply_update_samples = list(state.apply_update_ms_samples)
        ws_in_q_samples = list(state.ws_in_q_depth_samples)
        parse_q_samples = list(state.parse_q_depth_samples)
        write_q_samples = list(state.write_q_depth_samples)
        enqueue_stats = _quantiles_with_mean(enqueue_samples)
        parsed_stats = _quantiles_with_mean(parsed_samples)
        applied_stats = _quantiles_with_mean(applied_samples)
        write_end_to_apply_stats = _quantiles_with_mean(write_end_to_apply_samples)
        apply_update_stats = _quantiles_with_mean(apply_update_samples)
        ws_in_q_quantiles = _quantiles_from_samples_any(ws_in_q_samples, (95,))
        parse_q_quantiles = _quantiles_from_samples_any(parse_q_samples, (95,))
        write_q_quantiles = _quantiles_from_samples_any(write_q_samples, (95,))
        enqueue_max = enqueue_stats["max"]
        parsed_max = parsed_stats["max"]
        applied_max = applied_stats["max"]
        write_end_to_apply_max = write_end_to_apply_stats["max"]
        apply_update_max = apply_update_stats["max"]
        ws_in_q_max = max(ws_in_q_samples) if ws_in_q_samples else 0
        parse_q_max = max(parse_q_samples) if parse_q_samples else 0
        write_q_max = max(write_q_samples) if write_q_samples else 0
        loop_lag_samples = list(state.loop_lag_samples_ms)
        loop_lag_ms_max_last_interval = (
            max(loop_lag_samples) if loop_lag_samples else 0.0
        )
        state.loop_lag_samples_ms.clear()
        state.ws_rx_to_enqueue_ms_samples.clear()
        state.ws_rx_to_parsed_ms_samples.clear()
        state.ws_rx_to_applied_ms_samples.clear()
        state.write_end_to_apply_ms_samples.clear()
        state.apply_update_ms_samples.clear()
        state.ws_in_q_depth_samples.clear()
        state.parse_q_depth_samples.clear()
        state.write_q_depth_samples.clear()
        parse_dropped = parse_stats.get("dropped", state.parse_dropped_total)
        parse_results_dropped = parse_stats.get("results_dropped", 0)
        frame_write_dropped = frame_write_stats.get(
            "dropped", state.frame_write_dropped_total
        )
        frame_write_results_dropped = frame_write_stats.get("results_dropped", 0)
        dropped_messages_total = (
            parse_dropped
            + parse_results_dropped
            + frame_write_dropped
            + frame_write_results_dropped
        )
        dropped_messages_count = max(
            0, dropped_messages_total - state.dropped_messages_total_last
        )
        state.dropped_messages_total_last = dropped_messages_total
        global_write_quantiles = _quantiles_from_samples(global_write_samples, (50, 95, 99))
        global_ingest_quantiles = _quantiles_from_samples(
            global_ingest_samples, (50, 95, 99)
        )
        global_backpressure_quantiles = _quantiles_from_samples(
            global_backpressure_samples, (50, 95, 99)
        )
        global_record = {
            "record_type": "heartbeat",
            "run_id": state.run.run_id,
            "hb_wall_ns_utc": hb_wall_ns_utc,
            "hb_mono_ns": now_ns,
            "elapsed_ns": elapsed_ns,
            "frames": global_frames,
            "bytes_written": global_bytes,
            "msgs_per_sec": global_frames / max(elapsed_ns / 1_000_000_000.0, 1e-9),
            "bytes_per_sec": global_bytes / max(elapsed_ns / 1_000_000_000.0, 1e-9),
            "write_ns_p50": global_write_quantiles[50],
            "write_ns_p95": global_write_quantiles[95],
            "write_ns_p99": global_write_quantiles[99],
            "ingest_ns_p50": global_ingest_quantiles[50],
            "ingest_ns_p95": global_ingest_quantiles[95],
            "ingest_ns_p99": global_ingest_quantiles[99],
            "backpressure_ns_p50": global_backpressure_quantiles[50],
            "backpressure_ns_p95": global_backpressure_quantiles[95],
            "backpressure_ns_p99": global_backpressure_quantiles[99],
            "coverage_pct": global_coverage_pct,
            "token_ids_seen": len(global_tokens_seen),
            "token_ids_assigned": len(state.pinned_tokens),
            "reconnects": global_reconnects,
            "confirm_failures": global_confirm_failures,
            "confirm_failures_total": global_confirm_failures,
            "shards_unconfirmed_count": global_unconfirmed_count,
            "decode_errors": global_decode_errors,
            "msg_type_counts": global_msg_type_counts,
            "universe_version": state.universe.universe_version,
            "refresh_count": state.universe.refresh_count,
            "refresh_churn_pct_last": state.universe.refresh_churn_pct_last,
            "tokens_added_last": state.universe.tokens_added_last,
            "tokens_removed_last": state.universe.tokens_removed_last,
            "shards_refreshed_last": state.universe.shards_refreshed_last,
            "refresh_failures": state.universe.refresh_failures,
            "refresh_interval_seconds_current": state.universe.effective_refresh_interval_seconds,
            "refresh_skipped_delta_below_min_count": state.universe.refresh_skipped_delta_below_min_count,
            "refresh_last_decision_reason": state.universe.refresh_last_decision_reason,
            "refresh_inflight_skipped_count": state.universe.refresh_inflight_skipped_count,
            "universe_refresh_duration_ms": state.universe.refresh_duration_ms_last,
            "universe_refresh_worker_duration_ms": state.universe.refresh_worker_duration_ms_last,
            "gamma_pages_fetched": state.universe.refresh_gamma_pages_fetched_last,
            "markets_seen": state.universe.refresh_markets_seen_last,
            "tokens_selected": state.universe.refresh_tokens_selected_last,
            "expected_churn_bool": state.universe.expected_churn_last,
            "expected_churn_reason": state.universe.expected_churn_reason_last,
            "rollover_window_hit": state.universe.expected_churn_rollover_hit_last,
            "expiry_window_hit": state.universe.expected_churn_expiry_hit_last,
            "loop_lag_ms_max_last_interval": loop_lag_ms_max_last_interval,
            "ws_rx_to_enqueue_ms_mean": enqueue_stats["mean"],
            "ws_rx_to_enqueue_ms_p50": enqueue_stats["p50"],
            "ws_rx_to_enqueue_ms_p95": enqueue_stats["p95"],
            "ws_rx_to_enqueue_ms_p99": enqueue_stats["p99"],
            "ws_rx_to_enqueue_ms_max": enqueue_max,
            "ws_rx_to_enqueue_ms_count": len(enqueue_samples),
            "ws_rx_to_parsed_ms_mean": parsed_stats["mean"],
            "ws_rx_to_parsed_ms_p50": parsed_stats["p50"],
            "ws_rx_to_parsed_ms_p95": parsed_stats["p95"],
            "ws_rx_to_parsed_ms_p99": parsed_stats["p99"],
            "ws_rx_to_parsed_ms_max": parsed_max,
            "ws_rx_to_parsed_ms_count": len(parsed_samples),
            "ws_rx_to_applied_ms_mean": applied_stats["mean"],
            "ws_rx_to_applied_ms_p50": applied_stats["p50"],
            "ws_rx_to_applied_ms_p95": applied_stats["p95"],
            "ws_rx_to_applied_ms_p99": applied_stats["p99"],
            "ws_rx_to_applied_ms_max": applied_max,
            "ws_rx_to_applied_ms_count": len(applied_samples),
            "write_end_to_apply_ms_mean": write_end_to_apply_stats["mean"],
            "write_end_to_apply_ms_p50": write_end_to_apply_stats["p50"],
            "write_end_to_apply_ms_p95": write_end_to_apply_stats["p95"],
            "write_end_to_apply_ms_p99": write_end_to_apply_stats["p99"],
            "write_end_to_apply_ms_max": write_end_to_apply_max,
            "write_end_to_apply_ms_count": len(write_end_to_apply_samples),
            "apply_update_ms_mean": apply_update_stats["mean"],
            "apply_update_ms_p50": apply_update_stats["p50"],
            "apply_update_ms_p95": apply_update_stats["p95"],
            "apply_update_ms_p99": apply_update_stats["p99"],
            "apply_update_ms_max": apply_update_max,
            "apply_update_ms_count": len(apply_update_samples),
            "ws_in_q_depth_p95": ws_in_q_quantiles[95],
            "ws_in_q_depth_max": ws_in_q_max,
            "parse_q_depth_p95": parse_q_quantiles[95],
            "parse_q_depth_max": parse_q_max,
            "write_q_depth_p95": write_q_quantiles[95],
            "write_q_depth_max": write_q_max,
            "dropped_messages_count": dropped_messages_count,
            "dropped_messages_total": dropped_messages_total,
            "rx_idle_any_shard_s": rx_idle_any_shard_s,
            "rx_idle_all_shards_s": rx_idle_all_shards_s,
            "startup_connect_time_s_p50": startup_connect_time_s_p50,
            "startup_connect_time_s_p95": startup_connect_time_s_p95,
            "startup_connect_time_s_max": startup_connect_time_s_max,
            "startup_connect_pending_shards": startup_pending_shards,
            "midrun_disconnected_time_s_total": midrun_disconnected_time_s_total,
            "midrun_disconnected_incidents": midrun_incidents_total,
            "midrun_disconnected_incident_duration_s_p95": midrun_incident_duration_s_p95,
            "midrun_disconnected_incident_duration_s_max": midrun_incident_duration_s_max,
            "fee_rate_cache_hits": fee_rate_stats.get("cache_hits", 0),
            "fee_rate_cache_misses": fee_rate_stats.get("cache_misses", 0),
            "fee_rate_unknown_count": state.universe.fee_rate_unknown_count_last,
            "fee_regime_state": state.universe.fee_regime_state,
            "circuit_breaker_reason": state.universe.fee_regime_reason,
            "sampled_tokens_count": state.universe.fee_regime_sampled_tokens_count,
            "policy_counts": state.universe.policy_counts_last,
            "parse_queue_size": parse_stats.get("queue_size", 0),
            "parse_results_queue_size": parse_stats.get("results_size", 0),
            "parse_dropped": state.parse_dropped_total,
            "parse_worker_dropped": parse_stats.get("dropped", 0),
            "parse_results_dropped": parse_stats.get("results_dropped", 0),
            "parse_processed": parse_stats.get("processed", 0),
            "frame_write_queue_size": frame_write_stats.get("queue_size", 0),
            "frame_write_results_queue_size": frame_write_stats.get("results_size", 0),
            "frame_write_dropped": state.frame_write_dropped_total,
            "frame_write_worker_dropped": frame_write_stats.get("dropped", 0),
            "frame_write_results_dropped": frame_write_stats.get("results_dropped", 0),
            "frame_write_processed": frame_write_stats.get("processed", 0),
        }
        _write_metrics(metrics_global, global_record)
        state.heartbeat_samples.append(global_record)

        await _check_backpressure_fatal(
            state,
            now_ns,
            global_record["backpressure_ns_p99"],
        )
        if state.fatal_event.is_set() or state.stop_event.is_set():
            break

        if state.config.min_free_disk_gb is not None:
            usage = shutil.disk_usage(run_dir)
            free_gb = usage.free / (1024**3)
            if free_gb < state.config.min_free_disk_gb:
                await _trigger_fatal(
                    state,
                    FATAL_LOW_DISK,
                    f"free disk below {state.config.min_free_disk_gb} GB",
                )
                break

        next_tick = now_ns + interval_ns


def _handle_payload(
    state: CaptureState,
    shard: ShardState,
    raw: Any,
    *,
    rx_mono_ns: int,
    rx_wall_ns_utc: int,
) -> None:
    payload_bytes, flags = _payload_bytes(raw)
    confirm_event = _fast_confirm_event(payload_bytes)
    if confirm_event:
        shard.confirm_events_seen += 1
        if not shard.confirmed and (
            shard.confirm_events_seen >= state.config.capture_confirm_min_events
        ):
            shard.confirmed = True
            if not shard.ready_once:
                ready_ns = monotonic_ns()
                shard.ready_once = True
                shard.ready_mono_ns = ready_ns
                shard.startup_connect_time_s = max(
                    0.0, (ready_ns - state.run.t0_mono_ns) / 1_000_000_000.0
                )
                state.startup_connect_time_s_samples.append(shard.startup_connect_time_s)
            if shard.last_subscribe_variant is not None:
                shard.subscribe_variant_locked = shard.last_subscribe_variant
            variant = shard.last_subscribe_variant
            if variant is None:
                variant = shard.subscribe_variant_locked
                if variant is None:
                    variant = SUBSCRIBE_VARIANTS[
                        shard.subscribe_variant_index % len(SUBSCRIBE_VARIANTS)
                    ]
                shard.last_subscribe_variant = variant
            runlog_record = {
                "record_type": "subscribe_confirm_success",
                "run_id": state.run.run_id,
                "shard_id": shard.shard_id,
                "confirm_events_seen": shard.confirm_events_seen,
                "confirm_deadline_mono_ns": shard.confirm_deadline_mono_ns,
                "variant": variant,
            }
            if shard.last_subscribe_group_index is not None:
                runlog_record["group_index"] = shard.last_subscribe_group_index
            _write_runlog(
                state.run.run_dir,
                runlog_record,
            )

    t_enq_mono_ns = monotonic_ns()
    enqueue_latency_ms = max(0.0, (t_enq_mono_ns - rx_mono_ns) / 1_000_000.0)
    _append_sample_float(
        state.ws_rx_to_enqueue_ms_samples,
        enqueue_latency_ms,
        state.config.capture_metrics_max_samples,
    )

    if state.frame_writer is not None:
        if not state.frame_writer.enqueue(
            _FrameWriteJob(
                shard_id=shard.shard_id,
                payload_bytes=payload_bytes,
                rx_mono_ns=rx_mono_ns,
                rx_wall_ns_utc=rx_wall_ns_utc,
                flags=flags,
            )
        ):
            state.frame_write_dropped_total += 1
            if not state.fatal_event.is_set():
                asyncio.get_running_loop().create_task(
                    _trigger_fatal(
                        state,
                        FATAL_WRITE_QUEUE,
                        "frame write queue full",
                    )
                )
            return
        _append_sample(
            state.ws_in_q_depth_samples,
            state.frame_writer.queue_size(),
            state.config.capture_metrics_max_samples,
        )
    else:
        write_start_ns = monotonic_ns()
        record = append_record(
            shard.frames_fh,
            shard.idx_fh,
            payload_bytes,
            rx_mono_ns,
            rx_wall_ns_utc,
            flags=flags,
            schema_version=state.config.capture_frames_schema_version,
        )
        write_end_ns = monotonic_ns()
        backpressure_ns = max(0, write_start_ns - rx_mono_ns)
        shard.stats.record(
            record.payload_len,
            frames_header_len(record.schema_version),
            idx_entry_len(record.schema_version),
            write_end_ns - write_start_ns,
            write_end_ns - rx_mono_ns,
            backpressure_ns,
            [],
            {},
            state.config.capture_metrics_max_samples,
        )
        header_struct = frames_header_struct(record.schema_version)
        if record.schema_version == 1:
            header_bytes = header_struct.pack(
                frames_magic(record.schema_version),
                record.schema_version,
                record.flags,
                record.rx_mono_ns,
                record.payload_len,
                record.payload_crc32,
            )
        else:
            header_bytes = header_struct.pack(
                frames_magic(record.schema_version),
                record.schema_version,
                record.flags,
                record.rx_mono_ns,
                record.rx_wall_ns_utc,
                record.payload_len,
                record.payload_crc32,
            )
        shard.ring.append((header_bytes, record.payload))

    if payload_bytes in (b"PONG", b"PING"):
        return

    if state.parse_worker is None:
        try:
            payload = orjson.loads(payload_bytes)
        except orjson.JSONDecodeError:
            shard.stats.decode_errors += 1
            return
        token_pairs, msg_type_counts = _extract_minimal_fields(payload)
        token_ids = [token_id for token_id, _ in token_pairs]
        shard.stats.record_counts(token_ids, msg_type_counts)
        for token_id in token_ids:
            shard.last_seen[token_id] = rx_mono_ns
            if token_id in state.universe.token_added_mono_ns:
                state.universe.token_added_mono_ns.pop(token_id, None)
        return

    if not state.parse_worker.enqueue(
        _ParseJob(
            shard_id=shard.shard_id,
            payload_bytes=payload_bytes,
            rx_mono_ns=rx_mono_ns,
        )
    ):
        state.parse_dropped_total += 1
    else:
        _append_sample(
            state.parse_q_depth_samples,
            state.parse_worker.queue_size(),
            state.config.capture_metrics_max_samples,
        )


def _refresh_requested(shard: ShardState) -> bool:
    return shard.target is not None and shard.target.refresh_requested.is_set()


def _apply_shard_refresh(state: CaptureState, shard: ShardState) -> bool:
    target = shard.target
    if target is None or not target.refresh_requested.is_set():
        return False
    shard.token_ids = list(target.token_ids)
    shard.groups = list(target.groups)
    shard.confirm_token_ids = list(target.confirm_token_ids)
    _mark_reconnecting(shard)
    target.refresh_requested.clear()
    _write_runlog(
        state.run.run_dir,
        {
            "record_type": "shard_refresh_applied",
            "run_id": state.run.run_id,
            "shard_id": shard.shard_id,
            "to_version": target.target_version,
            "confirm_reset": True,
            "confirm_events_seen": shard.confirm_events_seen,
        },
    )
    return True


async def _wait_for_refresh_or_fatal(state: CaptureState, shard: ShardState) -> None:
    if shard.target is None:
        await asyncio.wait(
            [
                asyncio.create_task(state.fatal_event.wait()),
                asyncio.create_task(state.stop_event.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        return
    refresh_task = asyncio.create_task(shard.target.refresh_requested.wait())
    fatal_task = asyncio.create_task(state.fatal_event.wait())
    stop_task = asyncio.create_task(state.stop_event.wait())
    done, pending = await asyncio.wait(
        [refresh_task, fatal_task, stop_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task


def _compute_refresh_plan_sync(
    config: Config,
    *,
    universe_version: int,
    run_id: str,
    canary_tokens: list[str],
    session: requests.Session,
    loop: asyncio.AbstractEventLoop | None,
    fee_rate_client: FeeRateClient | None,
    policy_selector: PolicySelector | None,
    fee_regime_monitor: FeeRegimeMonitor | None,
    refresh_timeout_seconds: float,
) -> RefreshPlan:
    start_ns = time.perf_counter_ns()
    stats: dict[str, int] = {}
    snapshot = compute_desired_universe(
        config,
        universe_version=universe_version,
        session=session,
        stats=stats,
    )
    selected_markets = snapshot.selected_markets or []
    desired_tokens = {str(token_id) for token_id in snapshot.token_ids}
    desired_market_ids = {str(market_id) for market_id in snapshot.market_ids}
    fee_results: dict[str, Any] = {}
    sample_tokens: list[str] = []
    fee_regime_state = FeeRegimeState(
        state="OK",
        reason="DISABLED",
        sampled_tokens_count=0,
        unknown_fee_count=0,
        observed_fee_rates=[],
    )
    if config.fee_rate_enable and fee_rate_client and fee_regime_monitor:
        sample_tokens = fee_regime_monitor.sample_tokens(
            desired_tokens,
            run_id=run_id,
            universe_version=snapshot.universe_version,
            sample_size=config.fee_regime_sample_size_per_cycle,
            canary_token_ids=canary_tokens,
        )
        if loop is not None and not loop.is_closed():
            prefetch_timeout = config.fee_rate_refresh_timeout_seconds
            if refresh_timeout_seconds > 0:
                prefetch_timeout = min(prefetch_timeout, refresh_timeout_seconds)
            if sample_tokens and prefetch_timeout > 0:
                future = asyncio.run_coroutine_threadsafe(
                    fee_rate_client.prefetch_fee_rates(
                        sample_tokens,
                        timeout_seconds=prefetch_timeout,
                        max_tokens=config.fee_rate_prefetch_max_tokens,
                    ),
                    loop,
                )
                try:
                    future.result(timeout=prefetch_timeout)
                except FuturesTimeoutError:
                    with contextlib.suppress(Exception):
                        future.cancel()
                except Exception:
                    with contextlib.suppress(Exception):
                        future.cancel()
        fee_results = fee_rate_client.cached_fee_rates(desired_tokens)

    segment_by_token, segment_by_market = build_segment_maps(
        selected_markets,
        fee_results_by_token=fee_results,
    )
    if config.fee_rate_enable and fee_regime_monitor:
        fee_regime_state = fee_regime_monitor.evaluate(
            segment_by_token,
            token_ids_sampled=sample_tokens,
        )
    if policy_selector is not None:
        next_policy_counts = policy_counts(policy_selector, segment_by_token)
    else:
        next_policy_counts = {}
    scores = _token_score_map(selected_markets)
    ordered_tokens = _sorted_unique_tokens(snapshot.token_ids)
    shard_targets = _build_shard_target_plans(
        ordered_tokens,
        config,
        target_version=snapshot.universe_version,
        scores=scores,
    )
    worker_duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
    return RefreshPlan(
        snapshot=snapshot,
        desired_token_ids=desired_tokens,
        desired_market_ids=desired_market_ids,
        ordered_tokens=ordered_tokens,
        shard_targets=shard_targets,
        segment_by_token_id=segment_by_token,
        segment_by_market_id=segment_by_market,
        policy_counts=next_policy_counts,
        fee_regime_state=fee_regime_state,
        fee_regime_sampled_tokens_count=fee_regime_state.sampled_tokens_count,
        fee_rate_unknown_count=fee_regime_state.unknown_fee_count,
        gamma_pages_fetched=stats.get("pages_fetched", 0),
        markets_seen=stats.get("markets_seen", len(selected_markets)),
        tokens_selected=len(desired_tokens),
        worker_duration_ms=worker_duration_ms,
    )


class UniverseRefreshWorker:
    def __init__(
        self,
        *,
        config: Config,
        loop: asyncio.AbstractEventLoop,
        fee_rate_client: FeeRateClient | None,
        policy_selector: PolicySelector | None,
        fee_regime_monitor: FeeRegimeMonitor | None,
        canary_tokens: list[str],
    ) -> None:
        self._config = config
        self._loop = loop
        self._fee_rate_client = fee_rate_client
        self._policy_selector = policy_selector
        self._fee_regime_monitor = fee_regime_monitor
        self._canary_tokens = list(canary_tokens)
        self._session: requests.Session | None = None

    def close(self) -> None:
        if self._session is not None:
            with contextlib.suppress(Exception):
                self._session.close()

    def compute_plan(
        self,
        *,
        universe_version: int,
        run_id: str,
        refresh_timeout_seconds: float,
    ) -> RefreshPlan:
        if self._session is None:
            self._session = requests.Session()
        return _compute_refresh_plan_sync(
            self._config,
            universe_version=universe_version,
            run_id=run_id,
            canary_tokens=self._canary_tokens,
            session=self._session,
            loop=self._loop,
            fee_rate_client=self._fee_rate_client,
            policy_selector=self._policy_selector,
            fee_regime_monitor=self._fee_regime_monitor,
            refresh_timeout_seconds=refresh_timeout_seconds,
        )


async def _refresh_loop(state: CaptureState) -> None:
    config = state.config
    baseline_interval = config.capture_universe_refresh_interval_seconds
    if baseline_interval <= 0:
        return
    if state.universe.effective_refresh_interval_seconds <= 0:
        state.universe.effective_refresh_interval_seconds = baseline_interval
    run_dir = state.run.run_dir
    if state.fee_rate_client is None and config.fee_rate_enable:
        state.fee_rate_client = _build_fee_rate_client(config)
    if state.policy_selector is None:
        state.policy_selector = _build_policy_selector(config)
    if state.fee_regime_monitor is None:
        state.fee_regime_monitor = _build_fee_regime_monitor(config)
    canary_tokens = _parse_canary_tokens(config.fee_regime_canary_token_ids)
    if state.refresh_executor is None:
        state.refresh_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="universe-refresh",
        )
    if state.refresh_worker is None:
        state.refresh_worker = UniverseRefreshWorker(
            config=config,
            loop=asyncio.get_running_loop(),
            fee_rate_client=state.fee_rate_client,
            policy_selector=state.policy_selector,
            fee_regime_monitor=state.fee_regime_monitor,
            canary_tokens=canary_tokens,
        )
    refresh_timeout_seconds = config.capture_universe_refresh_timeout_seconds
    if refresh_timeout_seconds <= 0:
        refresh_timeout_seconds = max(baseline_interval, 1.0)
    try:
        while (
            not state.fatal_event.is_set()
            and not state.stop_event.is_set()
            and not state.universe.refresh_cancelled
        ):
            await asyncio.sleep(state.universe.effective_refresh_interval_seconds)
            if (
                state.fatal_event.is_set()
                or state.stop_event.is_set()
                or state.universe.refresh_cancelled
            ):
                break
            inflight = state.universe.refresh_inflight_future
            if inflight is not None:
                if not inflight.done():
                    state.universe.refresh_inflight_skipped_count += 1
                    state.universe.shards_refreshed_last = 0
                    state.universe.refresh_last_decision_reason = "SKIPPED_INFLIGHT"
                    inflight_age_ms = None
                    if state.universe.refresh_inflight_started_mono_ns is not None:
                        inflight_age_ms = int(
                            (monotonic_ns() - state.universe.refresh_inflight_started_mono_ns)
                            / 1_000_000
                        )
                    _write_runlog(
                        run_dir,
                        {
                            "record_type": "universe_refresh",
                            "run_id": state.run.run_id,
                            "universe_version": state.universe.universe_version + 1,
                            "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                            "reason": "SKIPPED_INFLIGHT",
                            "inflight_age_ms": inflight_age_ms,
                        },
                    )
                    continue
                try:
                    inflight.result()
                except Exception:
                    pass
                state.universe.refresh_inflight_future = None
                state.universe.refresh_inflight_timed_out = False
                state.universe.refresh_inflight_started_mono_ns = None
            try:
                started_ns = monotonic_ns()
                refresh_call = functools.partial(
                    state.refresh_worker.compute_plan,
                    universe_version=state.universe.universe_version + 1,
                    run_id=state.run.run_id,
                    refresh_timeout_seconds=refresh_timeout_seconds,
                )
                future = asyncio.get_running_loop().run_in_executor(
                    state.refresh_executor,
                    refresh_call,
                )
                state.universe.refresh_inflight_future = future
                state.universe.refresh_inflight_started_mono_ns = started_ns
                state.universe.refresh_inflight_timed_out = False
                plan = await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=refresh_timeout_seconds,
                )
                duration_ms = (monotonic_ns() - started_ns) / 1_000_000.0
                state.universe.refresh_duration_ms_last = duration_ms
                state.universe.refresh_worker_duration_ms_last = plan.worker_duration_ms
                state.universe.refresh_gamma_pages_fetched_last = plan.gamma_pages_fetched
                state.universe.refresh_markets_seen_last = plan.markets_seen
                state.universe.refresh_tokens_selected_last = plan.tokens_selected
                state.universe.fee_regime_state = plan.fee_regime_state.state
                state.universe.fee_regime_reason = plan.fee_regime_state.reason
                state.universe.fee_regime_sampled_tokens_count = (
                    plan.fee_regime_sampled_tokens_count
                )
                state.universe.fee_rate_unknown_count_last = plan.fee_rate_unknown_count
                snapshot = plan.snapshot
                desired_tokens = plan.desired_token_ids
                desired_market_ids = plan.desired_market_ids
                added, removed = _compute_refresh_delta(
                    state.universe.current_token_ids, desired_tokens
                )
                added_markets = desired_market_ids - state.universe.current_market_ids
                removed_markets = state.universe.current_market_ids - desired_market_ids
                delta_count = len(added) + len(removed)
                churn_pct = 100.0 * delta_count / max(
                    1, len(state.universe.current_token_ids)
                )
                state.universe.refresh_churn_pct_last = churn_pct
                state.universe.tokens_added_last = len(added)
                state.universe.tokens_removed_last = len(removed)

                expected_summary = _expected_churn_for_refresh(
                    time.time_ns(),
                    removed_markets,
                    added_markets,
                    state.universe.segment_by_market_id,
                    plan.segment_by_market_id,
                    config,
                )
                state.universe.expected_churn_last = expected_summary.expected_churn
                state.universe.expected_churn_reason_last = expected_summary.reason
                state.universe.expected_churn_rollover_hit_last = (
                    expected_summary.rollover_window_hit
                )
                state.universe.expected_churn_expiry_hit_last = (
                    expected_summary.expiry_window_hit
                )

                if not _should_apply_refresh(
                    added,
                    removed,
                    config.capture_universe_refresh_min_delta_tokens,
                ):
                    state.universe.refresh_skipped_delta_below_min_count += 1
                    state.universe.shards_refreshed_last = 0
                    state.universe.refresh_last_decision_reason = "SKIPPED_DELTA_BELOW_MIN"
                    _write_runlog(
                        run_dir,
                        {
                            "record_type": "universe_refresh",
                            "run_id": state.run.run_id,
                            "universe_version": snapshot.universe_version,
                            "added_count": len(added),
                            "removed_count": len(removed),
                            "churn_pct": churn_pct,
                            "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                            "added_sample": sorted(added)[:10],
                            "removed_sample": sorted(removed)[:10],
                            "reason": "SKIPPED_DELTA_BELOW_MIN",
                            "duration_ms": duration_ms,
                            "gamma_pages_fetched": plan.gamma_pages_fetched,
                            "markets_seen": plan.markets_seen,
                            "tokens_selected": plan.tokens_selected,
                        },
                    )
                    state.universe.refresh_inflight_future = None
                    state.universe.refresh_inflight_started_mono_ns = None
                    continue
                guard_triggered = (
                    churn_pct > config.capture_universe_refresh_max_churn_pct
                )
                bypass_guard = guard_triggered and expected_summary.expected_churn
                if bypass_guard:
                    state.universe.refresh_last_decision_reason = "BYPASSED_EXPECTED_CHURN"
                    guard_triggered = False
                (
                    next_interval,
                    next_guard_count,
                    guard_fatal,
                ) = _apply_churn_guard_policy(
                    state.universe.effective_refresh_interval_seconds,
                    baseline_interval,
                    config.capture_universe_refresh_max_interval_seconds,
                    guard_triggered=guard_triggered,
                    guard_count=state.universe.refresh_churn_guard_count,
                    fatal_threshold=config.capture_universe_refresh_churn_guard_consecutive_fatal,
                )
                state.universe.effective_refresh_interval_seconds = next_interval
                state.universe.refresh_churn_guard_count = next_guard_count
                if guard_triggered:
                    state.universe.shards_refreshed_last = 0
                    state.universe.refresh_last_decision_reason = "SKIPPED_CHURN_GUARD"
                    _write_runlog(
                        run_dir,
                        {
                            "record_type": "universe_refresh",
                            "run_id": state.run.run_id,
                            "universe_version": snapshot.universe_version,
                            "added_count": len(added),
                            "removed_count": len(removed),
                            "churn_pct": churn_pct,
                            "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                            "added_sample": sorted(added)[:10],
                            "removed_sample": sorted(removed)[:10],
                            "reason": "CHURN_GUARD",
                            "duration_ms": duration_ms,
                            "gamma_pages_fetched": plan.gamma_pages_fetched,
                            "markets_seen": plan.markets_seen,
                            "tokens_selected": plan.tokens_selected,
                        },
                    )
                    if guard_fatal:
                        await _trigger_fatal(
                            state,
                            FATAL_CHURN_GUARD,
                            "churn guard sustained",
                        )
                        break
                    state.universe.refresh_inflight_future = None
                    state.universe.refresh_inflight_started_mono_ns = None
                    continue
                if not bypass_guard:
                    state.universe.refresh_last_decision_reason = "APPLIED"

                next_targets = _materialize_shard_targets(plan.shard_targets)
                changed_targets = _select_changed_shards(
                    state.universe.shard_targets, next_targets
                )

                _write_runlog(
                    run_dir,
                    {
                        "record_type": "universe_refresh",
                        "run_id": state.run.run_id,
                        "universe_version": snapshot.universe_version,
                        "added_count": len(added),
                        "removed_count": len(removed),
                        "churn_pct": churn_pct,
                        "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                        "added_sample": sorted(added)[:10],
                        "removed_sample": sorted(removed)[:10],
                        "reason": "APPLIED",
                        "duration_ms": duration_ms,
                        "gamma_pages_fetched": plan.gamma_pages_fetched,
                        "markets_seen": plan.markets_seen,
                        "tokens_selected": plan.tokens_selected,
                    },
                )

                state.universe.universe_version = snapshot.universe_version
                state.universe.refresh_count += 1
                state.universe.shards_refreshed_last = len(changed_targets)
                state.universe.current_token_ids = desired_tokens
                state.universe.current_market_ids = desired_market_ids
                state.universe.segment_by_token_id = plan.segment_by_token_id
                state.universe.segment_by_market_id = plan.segment_by_market_id
                state.universe.policy_counts_last = plan.policy_counts
                now_ns = monotonic_ns()
                for token_id in added:
                    state.universe.token_added_mono_ns[token_id] = now_ns
                for token_id in removed:
                    state.universe.token_added_mono_ns.pop(token_id, None)
                state.pinned_tokens = plan.ordered_tokens
                if removed:
                    for shard in state.shards:
                        for token_id in removed:
                            shard.last_seen.pop(token_id, None)

                for shard_id, next_target in next_targets.items():
                    if (
                        state.universe.refresh_cancelled
                        or state.fatal_event.is_set()
                        or state.stop_event.is_set()
                    ):
                        break
                    current = state.universe.shard_targets.get(shard_id)
                    if current is None:
                        state.universe.shard_targets[shard_id] = next_target
                        current = next_target
                    if shard_id in changed_targets:
                        _write_runlog(
                            run_dir,
                            {
                                "record_type": "shard_refresh_begin",
                                "run_id": state.run.run_id,
                                "shard_id": shard_id,
                                "from_version": current.target_version,
                                "to_version": snapshot.universe_version,
                                "from_token_count": len(current.token_ids),
                                "to_token_count": len(next_target.token_ids),
                            },
                        )
                        current.token_ids = next_target.token_ids
                        current.groups = next_target.groups
                        current.confirm_token_ids = next_target.confirm_token_ids
                        current.target_version = snapshot.universe_version
                        current.refresh_requested.set()
                        await asyncio.sleep(config.capture_universe_refresh_stagger_seconds)
                    else:
                        current.target_version = snapshot.universe_version
                state.universe.refresh_inflight_future = None
                state.universe.refresh_inflight_started_mono_ns = None
            except asyncio.TimeoutError:
                state.universe.refresh_failures += 1
                state.universe.refresh_last_decision_reason = "ERROR"
                if state.universe.refresh_inflight_started_mono_ns is not None:
                    state.universe.refresh_duration_ms_last = (
                        monotonic_ns() - state.universe.refresh_inflight_started_mono_ns
                    ) / 1_000_000.0
                state.universe.refresh_inflight_timed_out = True
                _write_runlog(
                    run_dir,
                    {
                        "record_type": "universe_refresh_error",
                        "run_id": state.run.run_id,
                        "error": "TimeoutError",
                        "message": "refresh timeout",
                        "duration_ms": state.universe.refresh_duration_ms_last,
                    },
                )
            except asyncio.CancelledError:
                state.universe.refresh_cancelled = True
                break
            except Exception as exc:
                state.universe.refresh_failures += 1
                state.universe.refresh_last_decision_reason = "ERROR"
                if state.universe.refresh_inflight_started_mono_ns is not None:
                    state.universe.refresh_duration_ms_last = (
                        monotonic_ns() - state.universe.refresh_inflight_started_mono_ns
                    ) / 1_000_000.0
                _write_runlog(
                    run_dir,
                    {
                        "record_type": "universe_refresh_error",
                        "run_id": state.run.run_id,
                        "error": type(exc).__name__,
                        "message": str(exc),
                        "duration_ms": state.universe.refresh_duration_ms_last,
                    },
                )
                state.universe.refresh_inflight_future = None
                state.universe.refresh_inflight_started_mono_ns = None
    finally:
        inflight = state.universe.refresh_inflight_future
        if state.refresh_worker is not None and (inflight is None or inflight.done()):
            state.refresh_worker.close()
            state.refresh_worker = None
        if state.refresh_executor is not None:
            state.refresh_executor.shutdown(wait=False, cancel_futures=True)
            state.refresh_executor = None


async def _run_shard(state: CaptureState, shard: ShardState) -> None:
    run_dir = state.run.run_dir
    yield_interval_ns = 5_000_000
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        if not shard.token_ids:
            await _wait_for_refresh_or_fatal(state, shard)
            if state.fatal_event.is_set() or state.stop_event.is_set():
                break
            _apply_shard_refresh(state, shard)
            continue
        if _refresh_requested(shard):
            _apply_shard_refresh(state, shard)
            continue
        if shard.subscribe_variant_locked:
            variant = shard.subscribe_variant_locked
        else:
            variant = SUBSCRIBE_VARIANTS[
                shard.subscribe_variant_index % len(SUBSCRIBE_VARIANTS)
            ]
        refresh_applied = False
        ping_interval, ping_timeout, data_idle_reconnect_seconds = _normalize_ws_keepalive(
            state.config
        )
        connect_kwargs: dict[str, Any] = {
            "ping_interval": ping_interval,
            "ping_timeout": ping_timeout,
        }
        if CONNECT_SUPPORTS_CLOSE_TIMEOUT:
            connect_kwargs["close_timeout"] = DEFAULT_WS_CLOSE_TIMEOUT_SECONDS
        try:
            async with websockets.connect(
                state.config.clob_ws_url,
                **connect_kwargs,
            ) as ws:
                connected_ns = monotonic_ns()
                if shard.disconnected_since_mono_ns is not None:
                    duration_s = (
                        connected_ns - shard.disconnected_since_mono_ns
                    ) / 1_000_000_000.0
                    if shard.ready_once:
                        shard.midrun_disconnected_time_s_total += duration_s
                        shard.midrun_disconnected_incidents += 1
                        state.midrun_disconnected_time_s_total += duration_s
                        state.midrun_disconnected_incidents += 1
                        state.midrun_disconnected_durations_s.append(duration_s)
                    shard.disconnected_since_mono_ns = None
                shard.connected = True
                _mark_reconnecting(shard)
                _write_runlog(
                    run_dir,
                    {
                        "record_type": "ws_connect",
                        "run_id": state.run.run_id,
                        "shard_id": shard.shard_id,
                        "ws_url": state.config.clob_ws_url,
                        "user_agent_header": DEFAULT_USER_AGENT_HEADER,
                        "ping_interval_seconds": ping_interval,
                        "ping_timeout_seconds": ping_timeout,
                        "data_idle_reconnect_seconds": data_idle_reconnect_seconds,
                        "ts_mono_ns": connected_ns,
                        "ts_wall_ns_utc": time.time_ns(),
                    },
                )
                shard.last_subscribe_variant = variant
                shard.last_subscribe_group_index = None
                shard.confirm_deadline_mono_ns = (
                    monotonic_ns()
                    + int(state.config.capture_confirm_timeout_seconds * 1_000_000_000)
                )

                async def _check_confirm_deadline(now_ns: int) -> None:
                    if shard.confirmed or shard.confirm_deadline_mono_ns is None:
                        return
                    if now_ns < shard.confirm_deadline_mono_ns:
                        return
                    await _handle_confirm_deadline_miss(
                        state,
                        shard,
                        variant=variant,
                        now_ns=now_ns,
                    )

                for group_index, token_group in enumerate(shard.groups):
                    payload = build_subscribe_payload(variant, token_group)
                    payload_bytes = orjson.dumps(payload)
                    shard.last_subscribe_variant = variant
                    shard.last_subscribe_group_index = group_index
                    await ws.send(payload_bytes.decode("utf-8"))
                    _write_runlog(
                        run_dir,
                        {
                            "record_type": "subscribe_attempt",
                            "run_id": state.run.run_id,
                            "shard_id": shard.shard_id,
                            "variant": variant,
                            "group_index": group_index,
                            "token_count": len(token_group),
                            "payload_bytes": len(payload_bytes),
                        },
                    )

                data_idle_reconnect_ns = None
                if data_idle_reconnect_seconds is not None:
                    data_idle_reconnect_ns = int(
                        data_idle_reconnect_seconds * 1_000_000_000
                    )
                last_rx_mono_ns = monotonic_ns()

                while not state.fatal_event.is_set() and not state.stop_event.is_set():
                    if _refresh_requested(shard):
                        refresh_applied = _apply_shard_refresh(state, shard)
                        await ws.close()
                        break
                    if state.stop_event.is_set():
                        await ws.close()
                        break
                    now_ns = monotonic_ns()
                    await _check_confirm_deadline(now_ns)
                    recv_timeout = 1.0
                    if data_idle_reconnect_ns is not None:
                        idle_elapsed_ns = now_ns - last_rx_mono_ns
                        remaining_idle_ns = data_idle_reconnect_ns - idle_elapsed_ns
                        if remaining_idle_ns <= 0:
                            raise _DataIdleTimeout("data idle timeout")
                        recv_timeout = min(
                            recv_timeout,
                            remaining_idle_ns / 1_000_000_000,
                        )
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    except asyncio.TimeoutError:
                        now_ns = monotonic_ns()
                        await _check_confirm_deadline(now_ns)
                        if data_idle_reconnect_ns is not None:
                            if now_ns - last_rx_mono_ns >= data_idle_reconnect_ns:
                                raise _DataIdleTimeout("data idle timeout")
                        continue

                    rx_mono_ns = monotonic_ns()
                    last_rx_mono_ns = rx_mono_ns
                    shard.last_rx_mono_ns = rx_mono_ns
                    rx_wall_ns_utc = time.time_ns()
                    await _check_confirm_deadline(monotonic_ns())
                    _handle_payload(
                        state,
                        shard,
                        raw,
                        rx_mono_ns=rx_mono_ns,
                        rx_wall_ns_utc=rx_wall_ns_utc,
                    )
                    if rx_mono_ns - shard.last_yield_mono_ns >= yield_interval_ns:
                        shard.last_yield_mono_ns = rx_mono_ns
                        await asyncio.sleep(0)
            if refresh_applied:
                continue
        except Exception as exc:
            if state.stop_event.is_set():
                break
            now_ns = monotonic_ns()
            if shard.connected:
                shard.connected = False
                shard.disconnected_since_mono_ns = now_ns
            _mark_reconnecting(shard)
            shard.reconnects += 1
            trigger = _classify_reconnect_trigger(exc)
            close_code, close_reason, close_was_clean = _extract_close_details(exc)
            _write_runlog(
                run_dir,
                {
                    "record_type": "reconnect",
                    "run_id": state.run.run_id,
                    "shard_id": shard.shard_id,
                    "reason": type(exc).__name__,
                    "trigger": trigger,
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "close_was_clean": close_was_clean,
                    "ping_interval_seconds": ping_interval,
                    "ping_timeout_seconds": ping_timeout,
                    "data_idle_reconnect_seconds": data_idle_reconnect_seconds,
                    "reconnects": shard.reconnects,
                    "ts_mono_ns": now_ns,
                    "ts_wall_ns_utc": time.time_ns(),
                },
            )
            if shard.reconnects >= state.config.ws_reconnect_max:
                await _trigger_fatal(
                    state,
                    FATAL_RECONNECT_STORM,
                    "reconnect limit exceeded",
                )
                break
            await asyncio.sleep(state.config.ws_reconnect_backoff_seconds)


async def _capture_online_async(config: Config, run_id: str | None = None) -> int:
    if config.ws_shards <= 0:
        raise ValueError("ws_shards must be >= 1")
    (
        initial_snapshot,
        pinned_tokens,
        pinned_markets,
        selected_markets,
        universe_mode,
    ) = _load_startup_universe(config)
    if not pinned_tokens:
        raise RuntimeError("no pinned tokens available for capture")
    scores = _token_score_map(selected_markets)
    shard_targets = _build_shard_targets(
        pinned_tokens,
        config,
        target_version=initial_snapshot.universe_version,
        scores=scores,
    )
    fee_rate_client = _build_fee_rate_client(config) if config.fee_rate_enable else None
    policy_selector = _build_policy_selector(config)
    fee_regime_monitor = _build_fee_regime_monitor(config)
    fee_results = (
        fee_rate_client.cached_fee_rates(pinned_tokens) if fee_rate_client else {}
    )
    segment_by_token, segment_by_market = build_segment_maps(
        selected_markets,
        fee_results_by_token=fee_results,
    )
    policy_counts_initial = policy_counts(policy_selector, segment_by_token)
    policy_summary_initial = _segment_policy_summary(policy_selector, segment_by_token)
    canary_tokens = _parse_canary_tokens(config.fee_regime_canary_token_ids)
    expected_fee_rate_values = sorted(
        parse_expected_fee_rate_bps_values(config.fee_regime_expected_fee_rate_bps_values)
    )

    manifest_extra = {
        "capture_schema_version": config.capture_frames_schema_version,
        "payload_source": "text",
        "payload_encoder": "utf-8",
        "universe_mode": universe_mode,
        "monotonic_ns_source": "perf_counter_ns",
        "git_commit": _read_git_commit(),
        "environment": _environment_metadata(),
        "config": _config_snapshot(config),
        "pinned_tokens": pinned_tokens,
        "pinned_markets": pinned_markets,
        "segment_metadata": {
            "schema_version": 1,
            "by_token_id": {
                token_id: tag.to_dict() for token_id, tag in segment_by_token.items()
            },
            "by_market_id": {
                market_id: tag.to_dict() for market_id, tag in segment_by_market.items()
            },
        },
        "policy_map": policy_selector.policy_map.to_dict(),
        "policy_assignments": {
            "by_policy_id": policy_counts_initial,
            "by_segment": policy_summary_initial,
        },
        "fee_regime": {
            "expect_15m_crypto_fee_enabled": config.fee_regime_expect_15m_crypto_fee_enabled,
            "expect_other_fee_free": config.fee_regime_expect_other_fee_free,
            "expect_unknown_fee_free": config.fee_regime_expect_unknown_fee_free,
            "sample_size_per_cycle": config.fee_regime_sample_size_per_cycle,
            "canary_token_ids": canary_tokens,
            "expected_fee_rate_bps_values": expected_fee_rate_values,
        },
        "initial_universe": _snapshot_for_manifest(initial_snapshot),
        "universe_refresh": {
            "enabled": config.capture_universe_refresh_enable,
            "interval_seconds": config.capture_universe_refresh_interval_seconds,
            "timeout_seconds": config.capture_universe_refresh_timeout_seconds,
            "stagger_seconds": config.capture_universe_refresh_stagger_seconds,
            "grace_seconds": config.capture_universe_refresh_grace_seconds,
            "min_delta_tokens": config.capture_universe_refresh_min_delta_tokens,
            "max_churn_pct": config.capture_universe_refresh_max_churn_pct,
            "max_interval_seconds": config.capture_universe_refresh_max_interval_seconds,
            "churn_guard_consecutive_fatal": config.capture_universe_refresh_churn_guard_consecutive_fatal,
        },
        "shards": {
            "count": config.ws_shards,
            "assignments": {
                str(shard_id): {"token_ids": target.token_ids, "groups": target.groups}
                for shard_id, target in shard_targets.items()
            },
        },
        "subscribe_caps": {
            "max_tokens": config.ws_subscribe_max_tokens,
            "max_bytes": config.ws_subscribe_max_bytes,
        },
    }

    run = bootstrap_run(config, run_id=run_id, manifest_extra=manifest_extra)
    run_dir = run.run_dir
    print(f"capture run dir: {run_dir}")
    io_writer = _NdjsonWriter()
    global _IO_WRITER
    _IO_WRITER = io_writer

    precision = _monotonic_precision_stats(sample_count=200)
    _write_runlog(
        run_dir,
        {
            "record_type": "monotonic_precision",
            "run_id": run.run_id,
            **precision,
        },
    )
    if not precision.get("ok", False):
        _write_startup_fatal(
            run,
            FATAL_MONO_QUANTIZED,
            "monotonic clock precision insufficient",
            extra={"monotonic_precision": precision},
        )
        io_writer.close()
        _IO_WRITER = None
        return 1
    shards: list[ShardState] = []
    for shard_id, target in shard_targets.items():
        frames_path = run_dir / "capture" / f"shard_{shard_id:02d}.frames"
        idx_path = run_dir / "capture" / f"shard_{shard_id:02d}.idx"
        frames_fh = frames_path.open("ab")
        idx_fh = idx_path.open("ab")
        ring = deque(maxlen=config.capture_ring_buffer_frames)
        shards.append(
            ShardState(
                shard_id=shard_id,
                token_ids=target.token_ids,
                groups=target.groups,
                frames_path=frames_path,
                idx_path=idx_path,
                frames_fh=frames_fh,
                idx_fh=idx_fh,
                ring=ring,
                confirm_token_ids=target.confirm_token_ids,
                target=target,
                disconnected_since_mono_ns=run.t0_mono_ns,
            )
        )
    token_added_mono_ns: dict[str, int] = {}
    universe_state = UniverseState(
        universe_version=initial_snapshot.universe_version,
        current_token_ids=set(initial_snapshot.token_ids),
        current_market_ids=set(initial_snapshot.market_ids),
        token_added_mono_ns=token_added_mono_ns,
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
        segment_by_token_id=segment_by_token,
        segment_by_market_id=segment_by_market,
        policy_counts_last=policy_counts_initial,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=shards,
        pinned_tokens=pinned_tokens,
        universe=universe_state,
        fee_rate_client=fee_rate_client,
        policy_selector=policy_selector,
        fee_regime_monitor=fee_regime_monitor,
    )
    state.io_writer = io_writer
    state.parse_worker = _ParseWorker()
    state.frame_writer = _FrameWriter(
        shards=shards,
        schema_version=config.capture_frames_schema_version,
    )
    if config.capture_gc_disable and gc.isenabled():
        gc.disable()
        state.gc_was_enabled = True
    _install_signal_handlers(state)
    _write_runlog(
        run_dir,
        {
            "record_type": "capture_start",
            "run_id": run.run_id,
            "shards": config.ws_shards,
            "token_count": len(pinned_tokens),
            "universe_version": initial_snapshot.universe_version,
        },
    )

    tasks = [asyncio.create_task(_run_shard(state, shard)) for shard in shards]
    heartbeat_task = asyncio.create_task(_heartbeat_loop(state))
    tasks.append(heartbeat_task)
    lag_task = asyncio.create_task(_loop_lag_monitor(state))
    tasks.append(lag_task)
    parse_task = asyncio.create_task(_parse_results_loop(state))
    tasks.append(parse_task)
    write_task = asyncio.create_task(_write_results_loop(state))
    tasks.append(write_task)
    if config.capture_universe_refresh_enable:
        refresh_task = asyncio.create_task(_refresh_loop(state))
        state.universe.refresh_task = refresh_task
        tasks.append(refresh_task)
    stop_task = asyncio.create_task(state.stop_event.wait())
    tasks.append(stop_task)

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    if state.stop_event.is_set() and not state.fatal_event.is_set():
        _write_runlog(
            run_dir,
            {
                "record_type": "capture_stop",
                "run_id": state.run.run_id,
                "reason": state.stop_reason or "signal",
            },
        )
    if not state.fatal_event.is_set():
        for task in done:
            exc = task.exception()
            if exc is not None:
                await _trigger_fatal(
                    state,
                    FATAL_INTERNAL,
                    f"task failed: {type(exc).__name__}",
                )
                break
    state.universe.refresh_cancelled = True
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    if state.frame_writer is not None:
        state.frame_writer.close()
        state.frame_writer = None

    for shard in shards:
        shard.frames_fh.flush()
        shard.idx_fh.flush()
        os.fsync(shard.frames_fh.fileno())
        os.fsync(shard.idx_fh.fileno())
        shard.frames_fh.close()
        shard.idx_fh.close()

    if state.parse_worker is not None:
        state.parse_worker.close()
        state.parse_worker = None

    if state.io_writer is not None:
        state.io_writer.close()
        state.io_writer = None
        _IO_WRITER = None

    if state.gc_was_enabled:
        gc.enable()

    return 1 if state.fatal_event.is_set() else 0


def run_capture_online(config: Config, run_id: str | None = None) -> int:
    try:
        return asyncio.run(_capture_online_async(config, run_id=run_id))
    except KeyboardInterrupt:
        return 0
