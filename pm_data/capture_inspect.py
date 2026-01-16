from __future__ import annotations

import json
from dataclasses import dataclass
from math import ceil
from pathlib import Path
from typing import Any

from .capture_format import read_idx


@dataclass(frozen=True)
class InspectSummary:
    run_dir: Path
    payload: dict[str, Any]


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _read_ndjson(path: Path) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if not path.exists():
        return records
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        records.append(json.loads(line))
    return records


def audit_heartbeat_gaps(
    run_dir: Path,
    *,
    threshold_seconds: float = 2.0,
    max_gaps: int = 5,
) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    runlog_path = run_dir / "runlog.ndjson"
    if not runlog_path.exists():
        raise FileNotFoundError(str(runlog_path))
    hb_count = 0
    prev_hb_mono_ns: int | None = None
    between_types: dict[str, int] = {}
    gaps: list[dict[str, Any]] = []
    with runlog_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            record_type = record.get("record_type")
            if record_type == "heartbeat":
                hb_count += 1
                hb_mono_ns = record.get("hb_mono_ns")
                if isinstance(hb_mono_ns, int) and prev_hb_mono_ns is not None:
                    gap_seconds = (hb_mono_ns - prev_hb_mono_ns) / 1_000_000_000.0
                    if gap_seconds > threshold_seconds:
                        gaps.append(
                            {
                                "gap_seconds": gap_seconds,
                                "from_hb_mono_ns": prev_hb_mono_ns,
                                "to_hb_mono_ns": hb_mono_ns,
                                "record_types_between": dict(
                                    sorted(between_types.items())
                                ),
                            }
                        )
                if isinstance(hb_mono_ns, int):
                    prev_hb_mono_ns = hb_mono_ns
                between_types = {}
                continue
            if isinstance(record_type, str) and record_type:
                between_types[record_type] = between_types.get(record_type, 0) + 1
    gaps_sorted = sorted(gaps, key=lambda gap: gap["gap_seconds"], reverse=True)
    max_gap = gaps_sorted[0] if gaps_sorted else None
    return {
        "run_dir": str(run_dir),
        "threshold_seconds": threshold_seconds,
        "hb_count": hb_count,
        "gaps_over_threshold": len(gaps_sorted),
        "max_gap_seconds": max_gap["gap_seconds"] if max_gap else 0.0,
        "max_gap": max_gap,
        "top_gaps": gaps_sorted[: max_gaps if max_gaps > 0 else len(gaps_sorted)],
    }


def audit_capture_quality(run_dir: Path) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    capture_stop: dict[str, Any] | None = None
    runlog_path = run_dir / "runlog.ndjson"
    if runlog_path.exists():
        with runlog_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record.get("record_type") == "capture_stop":
                    capture_stop = record

    reasons: list[str] = []
    status = "unknown"
    parse_worker_dropped = None
    parse_results_dropped = None
    loss_parse_queue_drops = None
    loss_parse_results_drops = None
    loss_metrics_drops = None

    if capture_stop is not None and "quality_ok" in capture_stop:
        status = "ok" if capture_stop.get("quality_ok") else "degraded"
        reasons = list(capture_stop.get("quality_reasons") or [])
        parse_worker_dropped = capture_stop.get("parse_worker_dropped")
        parse_results_dropped = capture_stop.get("parse_results_dropped")
        loss_parse_queue_drops = capture_stop.get("loss_parse_queue_drops")
        loss_parse_results_drops = capture_stop.get("loss_parse_results_drops")
        loss_metrics_drops = capture_stop.get("loss_metrics_drops")
    else:
        metrics_path = run_dir / "metrics" / "global.ndjson"
        last_metrics: dict[str, Any] | None = None
        if metrics_path.exists():
            with metrics_path.open("r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if line:
                        last_metrics = json.loads(line)
        if last_metrics is not None:
            parse_worker_dropped = last_metrics.get("parse_worker_dropped")
            parse_results_dropped = last_metrics.get("parse_results_dropped")
            loss_parse_queue_drops = last_metrics.get("loss_parse_queue_drops")
            loss_parse_results_drops = last_metrics.get("loss_parse_results_drops")
            loss_metrics_drops = last_metrics.get("loss_metrics_drops")
            if int(loss_parse_queue_drops or 0) > 0 or int(parse_worker_dropped or 0) > 0:
                reasons.append("parse_queue_drops")
            if int(loss_parse_results_drops or 0) > 0 or int(parse_results_dropped or 0) > 0:
                reasons.append("parse_results_drops")
            if int(loss_metrics_drops or 0) > 0:
                reasons.append("metrics_drops")
            status = "degraded" if reasons else "ok"

    quality_ok = status == "ok"
    return {
        "run_dir": str(run_dir),
        "quality_status": status,
        "quality_ok": quality_ok,
        "quality_reasons": reasons,
        "parse_worker_dropped": parse_worker_dropped,
        "parse_results_dropped": parse_results_dropped,
        "loss_parse_queue_drops": loss_parse_queue_drops,
        "loss_parse_results_drops": loss_parse_results_drops,
        "loss_metrics_drops": loss_metrics_drops,
    }


def inspect_run(run_dir: Path) -> InspectSummary:
    run_dir = run_dir.resolve()
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(str(manifest_path))
    manifest = _load_json(manifest_path)

    capture_dir = run_dir / "capture"
    idx_paths = sorted(capture_dir.glob("*.idx"))
    shard_summaries: dict[str, Any] = {}
    total_records = 0
    total_frames_bytes = 0
    total_idx_bytes = 0
    for idx_path in idx_paths:
        shard_name = idx_path.stem
        frames_path = idx_path.with_suffix(".frames")
        idx_records = read_idx(idx_path, frames_path=frames_path)
        frames_bytes = frames_path.stat().st_size if frames_path.exists() else 0
        idx_bytes = idx_path.stat().st_size
        total_records += len(idx_records)
        total_frames_bytes += frames_bytes
        total_idx_bytes += idx_bytes
        first_rx = idx_records[0].rx_mono_ns if idx_records else None
        last_rx = idx_records[-1].rx_mono_ns if idx_records else None
        first_wall = idx_records[0].rx_wall_ns_utc if idx_records else None
        last_wall = idx_records[-1].rx_wall_ns_utc if idx_records else None
        shard_summaries[shard_name] = {
            "frames_path": str(frames_path),
            "idx_path": str(idx_path),
            "records": len(idx_records),
            "frames_bytes": frames_bytes,
            "idx_bytes": idx_bytes,
            "rx_mono_ns_first": first_rx,
            "rx_mono_ns_last": last_rx,
            "rx_wall_ns_utc_first": first_wall,
            "rx_wall_ns_utc_last": last_wall,
        }

    metrics_dir = run_dir / "metrics"
    global_metrics_path = metrics_dir / "global.ndjson"
    global_records = _read_ndjson(global_metrics_path)
    global_last = global_records[-1] if global_records else None

    shard_metrics: dict[str, Any] = {}
    for metrics_path in sorted(metrics_dir.glob("shard_*.ndjson")):
        records = _read_ndjson(metrics_path)
        shard_metrics[metrics_path.stem] = {
            "count": len(records),
            "last": records[-1] if records else None,
        }

    summary = {
        "run_dir": str(run_dir),
        "run_id": manifest.get("run_id"),
        "manifest_version": manifest.get("manifest_version"),
        "capture_schema_version": manifest.get("capture_schema_version"),
        "payload_source": manifest.get("payload_source"),
        "shards": {
            "count": len(shard_summaries),
            "records": total_records,
            "frames_bytes": total_frames_bytes,
            "idx_bytes": total_idx_bytes,
            "by_shard": shard_summaries,
        },
        "metrics": {
            "global": {
                "path": str(global_metrics_path),
                "count": len(global_records),
                "last": global_last,
            },
            "shards": shard_metrics,
        },
    }
    return InspectSummary(run_dir, summary)


def _quantile_from_sorted(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    rank = max(0, ceil(percentile / 100.0 * len(values)) - 1)
    return values[rank]


def _series_stats(values: list[float]) -> dict[str, float | int]:
    if not values:
        return {
            "count": 0,
            "min": 0.0,
            "max": 0.0,
            "p50": 0.0,
            "p95": 0.0,
            "p99": 0.0,
        }
    ordered = sorted(values)
    return {
        "count": len(ordered),
        "min": ordered[0],
        "max": ordered[-1],
        "p50": _quantile_from_sorted(ordered, 50),
        "p95": _quantile_from_sorted(ordered, 95),
        "p99": _quantile_from_sorted(ordered, 99),
    }


def _numeric_series(records: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for record in records:
        value = record.get(key)
        if isinstance(value, (int, float)):
            values.append(float(value))
    return values


def _optional_verify_summary(run_dir: Path) -> dict[str, Any]:
    candidates = (
        "verify_summary.json",
        "capture_verify.json",
        "capture_verify_summary.json",
    )
    for name in candidates:
        path = run_dir / name
        if path.exists():
            return {"present": True, "path": str(path), "summary": _load_json(path)}
    return {"present": False}


def _latency_series_summary(records: list[dict[str, Any]], prefix: str) -> dict[str, Any]:
    return {
        "p50": _series_stats(_numeric_series(records, f"{prefix}_p50")),
        "p95": _series_stats(_numeric_series(records, f"{prefix}_p95")),
        "p99": _series_stats(_numeric_series(records, f"{prefix}_p99")),
        "max": _series_stats(_numeric_series(records, f"{prefix}_max")),
        "count": len(_numeric_series(records, f"{prefix}_max")),
    }


def _ns_series_as_ms(records: list[dict[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for record in records:
        value = record.get(key)
        if isinstance(value, int) and value >= 0:
            values.append(value / 1_000_000.0)
    return values


def _ns_series_as_ms_when_true(
    records: list[dict[str, Any]], key: str, flag_key: str
) -> list[float]:
    values: list[float] = []
    for record in records:
        if record.get(flag_key) is not True:
            continue
        value = record.get(key)
        if isinstance(value, int) and value >= 0:
            values.append(value / 1_000_000.0)
    return values


def _loop_lag_attribution(metrics_records: list[dict[str, Any]], *, top_n: int = 10) -> dict[str, Any]:
    candidates: list[tuple[float, int]] = []
    for idx, record in enumerate(metrics_records):
        value = record.get("loop_lag_ms_max_last_interval")
        if isinstance(value, (int, float)):
            candidates.append((float(value), idx))
    candidates.sort(key=lambda item: item[0], reverse=True)
    top = candidates[: max(0, int(top_n))]

    fields = (
        "hb_mono_ns",
        "loop_lag_ms_max_last_interval",
        "hb_tick_work_ns",
        "disk_check_ns",
        "disk_free_bytes",
        "universe_refresh_duration_ms",
        "universe_refresh_worker_duration_ms",
        "ws_in_q_depth_max",
        "parse_q_depth_max",
        "write_q_depth_max",
        "dropped_messages_count",
        "dropped_messages_total",
        "ws_rx_to_enqueue_ms_p99",
        "ws_rx_to_parsed_ms_p99",
        "ws_rx_to_applied_ms_p99",
        "write_end_to_apply_ms_p99",
        "apply_update_ms_p99",
    )

    intervals: list[dict[str, Any]] = []
    for rank, (lag_ms, idx) in enumerate(top, start=1):
        record = metrics_records[idx]
        payload: dict[str, Any] = {"rank": rank, "index": idx, "lag_ms": lag_ms}
        for field in fields:
            if field in record:
                payload[field] = record.get(field)
        intervals.append(payload)

    return {"top_n": int(top_n), "top_intervals": intervals}


def _histogram(values: list[float], *, bucket_edges: list[float]) -> dict[str, Any]:
    if not bucket_edges:
        raise ValueError("bucket_edges must be non-empty")
    edges = sorted(float(edge) for edge in bucket_edges)
    counts = [0 for _ in range(len(edges) + 1)]
    for value in values:
        placed = False
        for idx, edge in enumerate(edges):
            if value <= edge:
                counts[idx] += 1
                placed = True
                break
        if not placed:
            counts[-1] += 1
    total = sum(counts)
    buckets: list[dict[str, Any]] = []
    for idx, count in enumerate(counts):
        upper = edges[idx] if idx < len(edges) else None
        buckets.append(
            {
                "le": upper,
                "count": count,
                "pct": (100.0 * count / total) if total else 0.0,
            }
        )
    return {"total": total, "bucket_edges": edges, "buckets": buckets}


def _max_consecutive_true(flags: list[bool]) -> int:
    max_streak = 0
    current = 0
    for flag in flags:
        if flag:
            current += 1
            if current > max_streak:
                max_streak = current
            continue
        current = 0
    return max_streak


def _pressure_streaks(
    metrics_records: list[dict[str, Any]],
    *,
    streak_intervals: int = 5,
    q_depth_max_threshold: int = 200,
    ws_rx_to_parsed_ms_p99_threshold: float = 50.0,
    ws_rx_to_applied_ms_p99_threshold: float = 100.0,
    backpressure_ms_p99_threshold: float = 25.0,
) -> dict[str, Any]:
    streak_intervals = max(1, int(streak_intervals))
    q_depth_max_threshold = max(0, int(q_depth_max_threshold))
    ws_rx_to_parsed_ms_p99_threshold = float(ws_rx_to_parsed_ms_p99_threshold)
    ws_rx_to_applied_ms_p99_threshold = float(ws_rx_to_applied_ms_p99_threshold)
    backpressure_ms_p99_threshold = float(backpressure_ms_p99_threshold)

    ws_in_flags: list[bool] = []
    parse_flags: list[bool] = []
    write_flags: list[bool] = []
    parsed_flags: list[bool] = []
    applied_flags: list[bool] = []
    backpressure_flags: list[bool] = []

    for record in metrics_records:
        ws_in_q_max = record.get("ws_in_q_depth_max")
        parse_q_max = record.get("parse_q_depth_max")
        write_q_max = record.get("write_q_depth_max")
        ws_rx_to_parsed_p99 = record.get("ws_rx_to_parsed_ms_p99")
        ws_rx_to_applied_p99 = record.get("ws_rx_to_applied_ms_p99")
        backpressure_ns_p99 = record.get("backpressure_ns_p99")

        ws_in_flags.append(
            isinstance(ws_in_q_max, int) and ws_in_q_max >= q_depth_max_threshold
        )
        parse_flags.append(
            isinstance(parse_q_max, int) and parse_q_max >= q_depth_max_threshold
        )
        write_flags.append(
            isinstance(write_q_max, int) and write_q_max >= q_depth_max_threshold
        )
        parsed_flags.append(
            isinstance(ws_rx_to_parsed_p99, (int, float))
            and float(ws_rx_to_parsed_p99) >= ws_rx_to_parsed_ms_p99_threshold
        )
        applied_flags.append(
            isinstance(ws_rx_to_applied_p99, (int, float))
            and float(ws_rx_to_applied_p99) >= ws_rx_to_applied_ms_p99_threshold
        )
        backpressure_flags.append(
            isinstance(backpressure_ns_p99, int)
            and (backpressure_ns_p99 / 1_000_000.0) >= backpressure_ms_p99_threshold
        )

    max_streaks = {
        "ws_in_q_depth_max": _max_consecutive_true(ws_in_flags),
        "parse_q_depth_max": _max_consecutive_true(parse_flags),
        "write_q_depth_max": _max_consecutive_true(write_flags),
        "ws_rx_to_parsed_ms_p99": _max_consecutive_true(parsed_flags),
        "ws_rx_to_applied_ms_p99": _max_consecutive_true(applied_flags),
        "backpressure_ms_p99": _max_consecutive_true(backpressure_flags),
    }
    worst_metric = max(max_streaks, key=lambda key: max_streaks[key], default=None)
    worst_streak = max_streaks[worst_metric] if worst_metric else 0
    failed = worst_streak >= streak_intervals and worst_streak > 0
    return {
        "streak_intervals": streak_intervals,
        "thresholds": {
            "q_depth_max": q_depth_max_threshold,
            "ws_rx_to_parsed_ms_p99": ws_rx_to_parsed_ms_p99_threshold,
            "ws_rx_to_applied_ms_p99": ws_rx_to_applied_ms_p99_threshold,
            "backpressure_ms_p99": backpressure_ms_p99_threshold,
        },
        "max_streaks": max_streaks,
        "worst_metric": worst_metric,
        "worst_streak": worst_streak,
        "failed": failed,
    }


def build_latency_report(run_dir: Path) -> dict[str, Any]:
    run_dir = run_dir.resolve()
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(str(manifest_path))
    manifest = _load_json(manifest_path)
    run_id = manifest.get("run_id")
    metrics_path = run_dir / "metrics" / "global.ndjson"
    metrics_records = _read_ndjson(metrics_path)
    metrics_dir = run_dir / "metrics"
    shard_metrics_records: dict[str, list[dict[str, Any]]] = {}
    for metrics_path in sorted(metrics_dir.glob("shard_*.ndjson")):
        shard_metrics_records[metrics_path.stem] = _read_ndjson(metrics_path)
    runlog_path = run_dir / "runlog.ndjson"
    runlog_records = _read_ndjson(runlog_path) if runlog_path.exists() else []
    verify_summary = _optional_verify_summary(run_dir)

    config = manifest.get("config") or {}
    expected_hb_interval_s = float(
        config.get("capture_heartbeat_interval_seconds", 1.0)
    )
    hb_mono_ns: list[int] = []
    for record in runlog_records:
        if record.get("record_type") != "heartbeat":
            continue
        hb_mono = record.get("hb_mono_ns")
        if isinstance(hb_mono, int):
            hb_mono_ns.append(hb_mono)
    hb_dt_s: list[float] = []
    for prev, curr in zip(hb_mono_ns, hb_mono_ns[1:]):
        hb_dt_s.append((curr - prev) / 1_000_000_000.0)
    hb_jitter_s = [dt - expected_hb_interval_s for dt in hb_dt_s]
    hb_counts_over = {
        "1.25": sum(1 for dt in hb_dt_s if dt > 1.25),
        "1.5": sum(1 for dt in hb_dt_s if dt > 1.5),
        "2.0": sum(1 for dt in hb_dt_s if dt > 2.0),
    }

    reconnects_total = 0
    reconnects_by_trigger: dict[str, int] = {}
    reconnects_by_reason: dict[str, int] = {}
    for record in runlog_records:
        if record.get("record_type") != "reconnect":
            continue
        reconnects_total += 1
        trigger = record.get("trigger") or "unknown"
        reason = record.get("reason") or "unknown"
        reconnects_by_trigger[trigger] = reconnects_by_trigger.get(trigger, 0) + 1
        reconnects_by_reason[reason] = reconnects_by_reason.get(reason, 0) + 1

    startup_connect_values: list[float] = []
    midrun_disconnected_total = 0.0
    midrun_disconnected_incidents = 0
    midrun_disconnected_incident_p95 = 0.0
    midrun_disconnected_incident_max = 0.0
    startup_connect_pending_shards = 0
    if metrics_records:
        last_metrics = metrics_records[-1]
        if isinstance(last_metrics.get("midrun_disconnected_time_s_total"), (int, float)):
            midrun_disconnected_total = float(
                last_metrics["midrun_disconnected_time_s_total"]
            )
        if isinstance(last_metrics.get("midrun_disconnected_incidents"), int):
            midrun_disconnected_incidents = int(
                last_metrics["midrun_disconnected_incidents"]
            )
        if isinstance(
            last_metrics.get("midrun_disconnected_incident_duration_s_p95"),
            (int, float),
        ):
            midrun_disconnected_incident_p95 = float(
                last_metrics["midrun_disconnected_incident_duration_s_p95"]
            )
        if isinstance(
            last_metrics.get("midrun_disconnected_incident_duration_s_max"),
            (int, float),
        ):
            midrun_disconnected_incident_max = float(
                last_metrics["midrun_disconnected_incident_duration_s_max"]
            )
        if isinstance(last_metrics.get("startup_connect_pending_shards"), int):
            startup_connect_pending_shards = int(
                last_metrics["startup_connect_pending_shards"]
            )

    for shard_id, records in shard_metrics_records.items():
        if not records:
            continue
        last_record = records[-1]
        value = last_record.get("startup_connect_time_s")
        if isinstance(value, (int, float)):
            startup_connect_values.append(float(value))
    startup_connect_stats = _series_stats(startup_connect_values)

    rx_idle_any_stats = _series_stats(
        _numeric_series(metrics_records, "rx_idle_any_shard_s")
    )
    rx_idle_all_stats = _series_stats(
        _numeric_series(metrics_records, "rx_idle_all_shards_s")
    )
    rx_idle_per_shard: dict[str, Any] = {}
    for shard_id, records in shard_metrics_records.items():
        rx_values = _numeric_series(records, "rx_idle_s")
        rx_idle_per_shard[shard_id] = _series_stats(rx_values)
    loop_lag_stats = _series_stats(
        _numeric_series(metrics_records, "loop_lag_ms_max_last_interval")
    )
    heartbeat_work_ms = _series_stats(_ns_series_as_ms(metrics_records, "hb_tick_work_ns"))
    disk_check_performed_values = _ns_series_as_ms_when_true(
        metrics_records, "disk_check_ns", "disk_check_performed"
    )
    disk_check_ms = _series_stats(disk_check_performed_values)
    dropped_count_stats = _series_stats(
        _numeric_series(metrics_records, "dropped_messages_count")
    )
    dropped_total = 0
    if metrics_records:
        last_metrics = metrics_records[-1]
        if isinstance(last_metrics.get("dropped_messages_total"), int):
            dropped_total = int(last_metrics["dropped_messages_total"])

    ws_in_q_stats = {
        "p95": _series_stats(_numeric_series(metrics_records, "ws_in_q_depth_p95")),
        "max": _series_stats(_numeric_series(metrics_records, "ws_in_q_depth_max")),
    }
    parse_q_stats = {
        "p95": _series_stats(_numeric_series(metrics_records, "parse_q_depth_p95")),
        "max": _series_stats(_numeric_series(metrics_records, "parse_q_depth_max")),
    }
    write_q_stats = {
        "p95": _series_stats(_numeric_series(metrics_records, "write_q_depth_p95")),
        "max": _series_stats(_numeric_series(metrics_records, "write_q_depth_max")),
    }

    refresh_records = [
        record
        for record in runlog_records
        if record.get("record_type") in {"universe_refresh", "universe_refresh_error"}
    ]
    refresh_durations = [
        float(record.get("duration_ms"))
        for record in refresh_records
        if isinstance(record.get("duration_ms"), (int, float))
    ]
    refresh_duration_stats = _series_stats(refresh_durations)
    refresh_errors = sum(
        1
        for record in refresh_records
        if record.get("record_type") == "universe_refresh_error"
    )

    hist_edges_ms = [0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0]
    loop_lag_series = _numeric_series(metrics_records, "loop_lag_ms_max_last_interval")
    stage_histograms = {
        "ws_rx_to_enqueue_ms_p99": _histogram(
            _numeric_series(metrics_records, "ws_rx_to_enqueue_ms_p99"),
            bucket_edges=hist_edges_ms,
        ),
        "ws_rx_to_parsed_ms_p99": _histogram(
            _numeric_series(metrics_records, "ws_rx_to_parsed_ms_p99"),
            bucket_edges=hist_edges_ms,
        ),
        "ws_rx_to_applied_ms_p99": _histogram(
            _numeric_series(metrics_records, "ws_rx_to_applied_ms_p99"),
            bucket_edges=hist_edges_ms,
        ),
        "write_end_to_apply_ms_p99": _histogram(
            _numeric_series(metrics_records, "write_end_to_apply_ms_p99"),
            bucket_edges=hist_edges_ms,
        ),
        "apply_update_ms_p99": _histogram(
            _numeric_series(metrics_records, "apply_update_ms_p99"),
            bucket_edges=hist_edges_ms,
        ),
        "loop_lag_ms_max_last_interval": _histogram(
            [float(value) for value in loop_lag_series],
            bucket_edges=hist_edges_ms,
        ),
        "heartbeat_work_ms": _histogram(
            _ns_series_as_ms(metrics_records, "hb_tick_work_ns"),
            bucket_edges=hist_edges_ms,
        ),
        "disk_check_ms": _histogram(
            disk_check_performed_values,
            bucket_edges=hist_edges_ms,
        ),
    }

    pressure = _pressure_streaks(metrics_records)

    report = {
        "run_dir": str(run_dir),
        "run_id": run_id,
        "capture_verify": verify_summary,
        "heartbeat": {
            "expected_interval_s": expected_hb_interval_s,
            "dt_s": _series_stats(hb_dt_s),
            "jitter_s": _series_stats(hb_jitter_s),
            "counts_over_s": hb_counts_over,
        },
        "reconnects": {
            "total": reconnects_total,
            "by_trigger": dict(sorted(reconnects_by_trigger.items())),
            "by_reason": dict(sorted(reconnects_by_reason.items())),
        },
        "disconnected": {
            "startup_connect_time_s": {
                "stats": startup_connect_stats,
                "pending_shards": startup_connect_pending_shards,
                "by_shard": {
                    shard_id: shard_metrics_records[shard_id][-1].get(
                        "startup_connect_time_s"
                    )
                    for shard_id in shard_metrics_records
                    if shard_metrics_records[shard_id]
                },
            },
            "midrun": {
                "time_s_total": midrun_disconnected_total,
                "incidents": midrun_disconnected_incidents,
                "incident_duration_s_p95": midrun_disconnected_incident_p95,
                "incident_duration_s_max": midrun_disconnected_incident_max,
                "by_shard": {
                    shard_id: shard_metrics_records[shard_id][-1].get(
                        "midrun_disconnected_time_s_total"
                    )
                    for shard_id in shard_metrics_records
                    if shard_metrics_records[shard_id]
                },
            },
        },
        "rx_idle": {
            "global_any_shard_s": rx_idle_any_stats,
            "global_all_shards_s": rx_idle_all_stats,
            "per_shard": rx_idle_per_shard,
        },
        "ingest_latency_ms": {
            "ws_rx_to_enqueue_ms": _latency_series_summary(
                metrics_records, "ws_rx_to_enqueue_ms"
            ),
            "ws_rx_to_parsed_ms": _latency_series_summary(
                metrics_records, "ws_rx_to_parsed_ms"
            ),
            "ws_rx_to_applied_ms": _latency_series_summary(
                metrics_records, "ws_rx_to_applied_ms"
            ),
            "write_end_to_apply_ms": _latency_series_summary(
                metrics_records, "write_end_to_apply_ms"
            ),
            "apply_update_ms": _latency_series_summary(
                metrics_records, "apply_update_ms"
            ),
        },
        "queue_depth": {
            "ws_in_q_depth": ws_in_q_stats,
            "parse_q_depth": parse_q_stats,
            "write_q_depth": write_q_stats,
        },
        "drops": {
            "dropped_messages_total": dropped_total,
            "dropped_messages_per_interval": dropped_count_stats,
        },
        "loop_lag_ms": loop_lag_stats,
        "heartbeat_work_ms": heartbeat_work_ms,
        "disk_check_ms": disk_check_ms,
        "disk_check_performed_count": len(disk_check_performed_values),
        "loop_lag_attribution": _loop_lag_attribution(metrics_records),
        "histograms_ms": stage_histograms,
        "pressure_streaks": pressure,
        "refresh": {
            "duration_ms": refresh_duration_stats,
            "count": len(refresh_records),
            "errors": refresh_errors,
        },
    }
    return report


def _latency_report_text(report: dict[str, Any]) -> str:
    lines = [
        f"run_id: {report.get('run_id')}",
        f"run_dir: {report.get('run_dir')}",
    ]
    verify = report.get("capture_verify") or {}
    if verify.get("present"):
        lines.append(f"capture_verify: present ({verify.get('path')})")
    else:
        lines.append("capture_verify: missing")
    heartbeat = report.get("heartbeat") or {}
    dt_stats = heartbeat.get("dt_s") or {}
    jitter_stats = heartbeat.get("jitter_s") or {}
    counts_over = heartbeat.get("counts_over_s") or {}
    lines.append(
        "hb_dt_s: p95={:.3f} max={:.3f} count_over_1.25s={} count_over_1.5s={} count_over_2.0s={}".format(
            float(dt_stats.get("p95", 0.0)),
            float(dt_stats.get("max", 0.0)),
            int(counts_over.get("1.25", 0)),
            int(counts_over.get("1.5", 0)),
            int(counts_over.get("2.0", 0)),
        )
    )
    lines.append(
        "hb_jitter_s: p95={:.3f} max={:.3f}".format(
            float(jitter_stats.get("p95", 0.0)),
            float(jitter_stats.get("max", 0.0)),
        )
    )
    reconnects = report.get("reconnects") or {}
    lines.append(f"reconnects_total: {reconnects.get('total', 0)}")
    disconnected = report.get("disconnected") or {}
    startup = disconnected.get("startup_connect_time_s") or {}
    startup_stats = startup.get("stats") or {}
    lines.append(
        "startup_connect_time_s: p95={:.3f} max={:.3f} pending_shards={}".format(
            float(startup_stats.get("p95", 0.0)),
            float(startup_stats.get("max", 0.0)),
            int(startup.get("pending_shards", 0)),
        )
    )
    midrun = disconnected.get("midrun") or {}
    lines.append(
        "midrun_disconnected: time_s_total={:.3f} incidents={} "
        "duration_s_p95={:.3f} duration_s_max={:.3f}".format(
            float(midrun.get("time_s_total", 0.0)),
            int(midrun.get("incidents", 0)),
            float(midrun.get("incident_duration_s_p95", 0.0)),
            float(midrun.get("incident_duration_s_max", 0.0)),
        )
    )
    rx_idle = report.get("rx_idle") or {}
    lines.append(
        "rx_idle_any_shard_s: p95={:.3f} max={:.3f}".format(
            float((rx_idle.get("global_any_shard_s") or {}).get("p95", 0.0)),
            float((rx_idle.get("global_any_shard_s") or {}).get("max", 0.0)),
        )
    )
    lines.append(
        "rx_idle_all_shards_s: p95={:.3f} max={:.3f}".format(
            float((rx_idle.get("global_all_shards_s") or {}).get("p95", 0.0)),
            float((rx_idle.get("global_all_shards_s") or {}).get("max", 0.0)),
        )
    )
    loop_lag = report.get("loop_lag_ms") or {}
    lines.append(
        "loop_lag_ms: p95={:.3f} p99={:.3f} max={:.3f}".format(
            float(loop_lag.get("p95", 0.0)),
            float(loop_lag.get("p99", 0.0)),
            float(loop_lag.get("max", 0.0)),
        )
    )
    hb_work = report.get("heartbeat_work_ms") or {}
    lines.append(
        "heartbeat_work_ms: p95={:.3f} p99={:.3f} max={:.3f}".format(
            float(hb_work.get("p95", 0.0)),
            float(hb_work.get("p99", 0.0)),
            float(hb_work.get("max", 0.0)),
        )
    )
    disk_check = report.get("disk_check_ms") or {}
    disk_check_count = int(report.get("disk_check_performed_count", 0) or 0)
    lines.append(
        "disk_check_ms(performed): p95={:.3f} p99={:.3f} max={:.3f} count={}".format(
            float(disk_check.get("p95", 0.0)),
            float(disk_check.get("p99", 0.0)),
            float(disk_check.get("max", 0.0)),
            disk_check_count,
        )
    )
    refresh = report.get("refresh") or {}
    refresh_duration = refresh.get("duration_ms") or {}
    lines.append(
        "refresh_duration_ms: p95={:.3f} max={:.3f} count={} errors={}".format(
            float(refresh_duration.get("p95", 0.0)),
            float(refresh_duration.get("max", 0.0)),
            int(refresh.get("count", 0)),
            int(refresh.get("errors", 0)),
        )
    )
    return "\n".join(lines) + "\n"


def write_latency_report(run_dir: Path) -> dict[str, Any]:
    report = build_latency_report(run_dir)
    json_payload = json.dumps(report, ensure_ascii=True, separators=(",", ":"))
    (run_dir / "latency_report.json").write_text(json_payload + "\n", encoding="utf-8")
    (run_dir / "latency_report.txt").write_text(
        _latency_report_text(report), encoding="utf-8"
    )
    return report
