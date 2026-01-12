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
        "loop_lag_ms: p95={:.3f} max={:.3f}".format(
            float(loop_lag.get("p95", 0.0)),
            float(loop_lag.get("max", 0.0)),
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
