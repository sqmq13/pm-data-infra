import asyncio
import struct
import sys
from collections import deque
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any

from .capture import _append_ndjson, _write_json
from .capture_format import frames_header_struct, frames_magic
from .capture_state import CaptureState, ShardState

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
FATAL_RUNLOG = "RUNLOG_FAILURE"
FATAL_FRAME_WRITER = "FRAME_WRITER_ERROR"


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


def _writer_error_payload(label: str, error: Exception | None) -> dict[str, Any]:
    if error is None:
        return {"writer": label}
    return {
        "writer": label,
        "error_type": type(error).__name__,
        "error_message": str(error),
    }


def _mark_runlog_failed(
    state: CaptureState,
    *,
    reason: str,
    error: Exception | None,
) -> None:
    if state.runlog_failed:
        return
    state.runlog_failed = True
    detail = reason
    if error is not None:
        detail = f"{type(error).__name__}: {error}"
    print(f"runlog failure: {detail}", file=sys.stderr)
    if not state.fatal_event.is_set():
        first_error = {"runlog_failure": _writer_error_payload("runlog", error)}
        asyncio.get_running_loop().create_task(
            _trigger_fatal(
                state,
                FATAL_RUNLOG,
                "runlog writer failure",
                first_error=first_error,
            )
        )


def _write_runlog(state: CaptureState, record: dict[str, Any]) -> None:
    if state.runlog_failed:
        return
    normalized = _normalize_orjson(record)
    path = state.run.run_dir / "runlog.ndjson"
    writer = state.runlog_writer
    if writer is None:
        _append_ndjson(
            path,
            normalized,
            fsync_on_close=state.config.capture_ndjson_fsync_on_close,
            fsync_interval_seconds=state.config.capture_ndjson_fsync_interval_seconds,
        )
        return
    error = writer.error()
    if error is not None:
        _mark_runlog_failed(state, reason="writer_error", error=error)
        return
    timeout_seconds = state.config.capture_runlog_enqueue_timeout_seconds
    if not writer.enqueue_blocking(
        path,
        normalized,
        timeout_seconds=timeout_seconds,
    ):
        state.loss_budget.runlog_enqueue_timeouts.bump()
        _mark_runlog_failed(state, reason="enqueue_timeout", error=None)


def _write_metrics(state: CaptureState, path: Path, record: dict[str, Any]) -> None:
    normalized = _normalize_orjson(record)
    writer = state.metrics_writer
    if writer is None:
        _append_ndjson(
            path,
            normalized,
            fsync_on_close=state.config.capture_ndjson_fsync_on_close,
            fsync_interval_seconds=state.config.capture_ndjson_fsync_interval_seconds,
        )
        return
    error = writer.error()
    if error is not None:
        if not state.metrics_writer_failed:
            state.metrics_writer_failed = True
            _write_runlog(
                state,
                {
                    "record_type": "metrics_writer_error",
                    "run_id": state.run.run_id,
                    "error": _writer_error_payload("metrics", error),
                },
            )
        return
    if not writer.enqueue_nowait(path, normalized):
        state.loss_budget.metrics_drops.bump()


def _write_startup_fatal(
    state: CaptureState,
    reason: str,
    message: str,
    *,
    extra: dict[str, Any] | None = None,
) -> None:
    _write_runlog(
        state,
        {
            "record_type": "fatal",
            "run_id": state.run.run_id,
            "fatal_reason": reason,
            "fatal_message": message,
        },
    )
    payload = {
        "fatal_reason": reason,
        "fatal_message": message,
        "run_id": state.run.run_id,
    }
    if extra:
        payload.update(extra)
    _write_json(state.run.run_dir / "fatal.json", payload)


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
        if not state.runlog_failed:
            _write_runlog(
                state,
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
            dump_path = (
                state.run.run_dir / f"last_frames_shard_{shard.shard_id:02d}.bin"
            )
            chunks: list[bytes] = []
            for entry in shard.ring:
                if isinstance(entry, tuple):
                    chunks.append(entry[0])
                    chunks.append(entry[1])
                else:
                    chunks.append(entry)
            dump_path.write_bytes(b"".join(chunks))
