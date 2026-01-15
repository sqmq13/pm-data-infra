from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .capture import RunBootstrap, _append_ndjson, bootstrap_run, monotonic_ns
from .capture_format import (
    append_record,
    frames_header_len,
    idx_entry_len,
)
from .config import Config

FRAME_FILES = (
    "ws_book_clean.json",
    "ws_book_extra_fields.json",
    "ws_book_malformed.json",
    "ws_book_out_of_order.json",
)

PAYLOAD_SOURCE = "decoded"
PAYLOAD_ENCODER = "json.dumps(separators=(',', ':'), ensure_ascii=True)"


@dataclass
class CaptureStats:
    frames: int = 0
    bytes_written: int = 0
    write_durations_ns: list[int] = field(default_factory=list)
    ingest_latencies_ns: list[int] = field(default_factory=list)
    token_ids: set[str] = field(default_factory=set)
    msg_type_counts: dict[str, int] = field(default_factory=dict)

    def record(
        self,
        payload_len: int,
        header_len: int,
        idx_len: int,
        write_duration_ns: int,
        ingest_latency_ns: int,
        token_ids: Iterable[str],
        msg_types: Iterable[str],
    ) -> None:
        self.frames += 1
        self.bytes_written += payload_len + header_len + idx_len
        self.write_durations_ns.append(write_duration_ns)
        self.ingest_latencies_ns.append(ingest_latency_ns)
        for token_id in token_ids:
            self.token_ids.add(token_id)
        for msg_type in msg_types:
            self.msg_type_counts[msg_type] = self.msg_type_counts.get(msg_type, 0) + 1


@dataclass
class CaptureRunResult:
    run: RunBootstrap
    stats: CaptureStats
    elapsed_ns: int
    shard_id: int
    pinned_tokens: set[str]


def quantile(values: list[int], percentile: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    rank = max(0, math.ceil(percentile / 100.0 * len(ordered)) - 1)
    return ordered[rank]


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _iter_frames_from_fixture(fixtures_dir: Path) -> Iterable[Any]:
    for name in FRAME_FILES:
        path = fixtures_dir / name
        if not path.exists():
            raise FileNotFoundError(str(path))
        payload = _load_json(path)
        if isinstance(payload, list):
            for item in payload:
                yield item
        else:
            yield payload


def _iter_items(frame: Any) -> Iterable[dict[str, Any]]:
    if isinstance(frame, list):
        for item in frame:
            if isinstance(item, dict):
                yield item
    elif isinstance(frame, dict):
        yield frame


def _get_token_id(item: dict[str, Any]) -> str | None:
    token_id = item.get("asset_id")
    if token_id is None:
        token_id = item.get("token_id") or item.get("tokenId") or item.get("assetId")
    if token_id is None:
        return None
    return str(token_id)


def _get_msg_type(item: dict[str, Any]) -> str:
    msg_type = item.get("event_type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    msg_type = item.get("type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    if "bids" in item or "asks" in item:
        return "book"
    if "price_changes" in item:
        return "price_change"
    return "unknown"


def _extract_minimal_fields(frame: Any) -> tuple[list[str], list[str]]:
    token_ids: list[str] = []
    msg_types: list[str] = []
    for item in _iter_items(frame):
        msg_type = _get_msg_type(item)
        msg_types.append(msg_type)
        token_id = _get_token_id(item)
        if token_id is not None:
            token_ids.append(token_id)
        price_changes = item.get("price_changes")
        if isinstance(price_changes, list):
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                change_token_id = _get_token_id(change)
                if change_token_id is None:
                    continue
                token_ids.append(change_token_id)
    return token_ids, msg_types


def _load_pinned_tokens(fixtures_dir: Path) -> set[str]:
    path = fixtures_dir / "gamma_markets.json"
    if not path.exists():
        return set()
    payload = _load_json(path)
    tokens: set[str] = set()
    if not isinstance(payload, list):
        return tokens
    for market in payload:
        if not isinstance(market, dict):
            continue
        token_ids = market.get("clobTokenIds") or market.get("clob_token_ids")
        if not isinstance(token_ids, list) or len(token_ids) != 2:
            continue
        for token_id in token_ids:
            tokens.add(str(token_id))
    return tokens


def _write_heartbeat(
    run_dir: Path,
    run_id: str,
    hb_wall_ns_utc: int,
    hb_mono_ns: int,
    *,
    fsync_on_close: bool,
    fsync_interval_seconds: float | None,
) -> None:
    record = {
        "record_type": "heartbeat",
        "run_id": run_id,
        "hb_wall_ns_utc": hb_wall_ns_utc,
        "hb_mono_ns": hb_mono_ns,
    }
    _append_ndjson(
        run_dir / "runlog.ndjson",
        record,
        fsync_on_close=fsync_on_close,
        fsync_interval_seconds=fsync_interval_seconds,
    )


def _write_metrics(
    metrics_path: Path,
    run_id: str,
    shard_id: int | None,
    stats: CaptureStats,
    elapsed_ns: int,
    pinned_tokens: set[str],
    *,
    fsync_on_close: bool,
    fsync_interval_seconds: float | None,
) -> None:
    elapsed_sec = max(elapsed_ns / 1_000_000_000.0, 1e-9)
    coverage_pct = 0.0
    if pinned_tokens:
        coverage_pct = 100.0 * len(stats.token_ids) / len(pinned_tokens)
    record: dict[str, Any] = {
        "record_type": "heartbeat",
        "run_id": run_id,
        "elapsed_ns": elapsed_ns,
        "frames": stats.frames,
        "bytes_written": stats.bytes_written,
        "msgs_per_sec": stats.frames / elapsed_sec,
        "bytes_per_sec": stats.bytes_written / elapsed_sec,
        "write_ns_p50": quantile(stats.write_durations_ns, 50),
        "write_ns_p95": quantile(stats.write_durations_ns, 95),
        "write_ns_p99": quantile(stats.write_durations_ns, 99),
        "ingest_ns_p50": quantile(stats.ingest_latencies_ns, 50),
        "ingest_ns_p95": quantile(stats.ingest_latencies_ns, 95),
        "ingest_ns_p99": quantile(stats.ingest_latencies_ns, 99),
        "coverage_pct": coverage_pct,
        "msg_type_counts": stats.msg_type_counts,
        "token_ids_seen": len(stats.token_ids),
    }
    if shard_id is not None:
        record["shard_id"] = shard_id
    _append_ndjson(
        metrics_path,
        record,
        fsync_on_close=fsync_on_close,
        fsync_interval_seconds=fsync_interval_seconds,
    )


def run_capture_offline(
    config: Config,
    fixtures_dir: Path,
    run_id: str | None = None,
    *,
    emit_metrics: bool = True,
    multiplier: int = 1,
) -> CaptureRunResult:
    if multiplier < 1:
        raise ValueError("multiplier must be >= 1")
    pinned_tokens = _load_pinned_tokens(fixtures_dir)
    manifest_extra = {
        "capture_schema_version": config.capture_frames_schema_version,
        "payload_source": PAYLOAD_SOURCE,
        "payload_encoder": PAYLOAD_ENCODER,
        "fixtures_dir": str(fixtures_dir),
        "fixtures_multiplier": multiplier,
        "pinned_token_ids": sorted(pinned_tokens),
        "ndjson_fsync_on_close": config.capture_ndjson_fsync_on_close,
        "ndjson_fsync_interval_seconds": config.capture_ndjson_fsync_interval_seconds,
    }
    run = bootstrap_run(config, run_id=run_id, manifest_extra=manifest_extra)
    run_dir = run.run_dir
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    metrics_global = run_dir / "metrics" / "global.ndjson"
    metrics_shard = run_dir / "metrics" / "shard_00.ndjson"
    stats = CaptureStats()
    start_mono_ns = run.t0_mono_ns
    next_heartbeat_ns = start_mono_ns + 1_000_000_000

    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        for _ in range(multiplier):
            for frame in _iter_frames_from_fixture(fixtures_dir):
                payload_text = json.dumps(frame, ensure_ascii=True, separators=(",", ":"))
                payload_bytes = payload_text.encode("utf-8")
                rx_mono_ns = monotonic_ns()
                rx_wall_ns_utc = run.t0_wall_ns_utc + (rx_mono_ns - run.t0_mono_ns)
                write_start_ns = monotonic_ns()
                record = append_record(
                    frames_fh,
                    idx_fh,
                    payload_bytes,
                    rx_mono_ns,
                    rx_wall_ns_utc,
                    schema_version=config.capture_frames_schema_version,
                )
                write_end_ns = monotonic_ns()
                write_duration_ns = write_end_ns - write_start_ns
                ingest_latency_ns = write_end_ns - rx_mono_ns
                token_ids, msg_types = _extract_minimal_fields(frame)
                stats.record(
                    len(payload_bytes),
                    frames_header_len(record.schema_version),
                    idx_entry_len(record.schema_version),
                    write_duration_ns,
                    ingest_latency_ns,
                    token_ids,
                    msg_types,
                )
                now_ns = write_end_ns
                if emit_metrics and now_ns >= next_heartbeat_ns:
                    hb_wall_ns_utc = time.time_ns()
                    _write_heartbeat(
                        run_dir,
                        run.run_id,
                        hb_wall_ns_utc,
                        now_ns,
                        fsync_on_close=config.capture_ndjson_fsync_on_close,
                        fsync_interval_seconds=(
                            config.capture_ndjson_fsync_interval_seconds
                        ),
                    )
                    elapsed_ns = now_ns - start_mono_ns
                    _write_metrics(
                        metrics_global,
                        run.run_id,
                        None,
                        stats,
                        elapsed_ns,
                        pinned_tokens,
                        fsync_on_close=config.capture_ndjson_fsync_on_close,
                        fsync_interval_seconds=(
                            config.capture_ndjson_fsync_interval_seconds
                        ),
                    )
                    _write_metrics(
                        metrics_shard,
                        run.run_id,
                        0,
                        stats,
                        elapsed_ns,
                        pinned_tokens,
                        fsync_on_close=config.capture_ndjson_fsync_on_close,
                        fsync_interval_seconds=(
                            config.capture_ndjson_fsync_interval_seconds
                        ),
                    )
                    next_heartbeat_ns = now_ns + 1_000_000_000
        frames_fh.flush()
        idx_fh.flush()
        os.fsync(frames_fh.fileno())
        os.fsync(idx_fh.fileno())

    end_mono_ns = monotonic_ns()
    elapsed_ns = end_mono_ns - start_mono_ns
    hb_wall_ns_utc = time.time_ns()
    _write_heartbeat(
        run_dir,
        run.run_id,
        hb_wall_ns_utc,
        end_mono_ns,
        fsync_on_close=config.capture_ndjson_fsync_on_close,
        fsync_interval_seconds=config.capture_ndjson_fsync_interval_seconds,
    )
    if emit_metrics:
        _write_metrics(
            metrics_global,
            run.run_id,
            None,
            stats,
            elapsed_ns,
            pinned_tokens,
            fsync_on_close=config.capture_ndjson_fsync_on_close,
            fsync_interval_seconds=config.capture_ndjson_fsync_interval_seconds,
        )
        _write_metrics(
            metrics_shard,
            run.run_id,
            0,
            stats,
            elapsed_ns,
            pinned_tokens,
            fsync_on_close=config.capture_ndjson_fsync_on_close,
            fsync_interval_seconds=config.capture_ndjson_fsync_interval_seconds,
        )
    return CaptureRunResult(run, stats, elapsed_ns, shard_id=0, pinned_tokens=pinned_tokens)
