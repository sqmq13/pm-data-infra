from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any

from .fixed import micro_to_cents


def _percentile(values: list[int], p: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = int(len(ordered) * p)
    idx = min(max(idx, 0), len(ordered) - 1)
    return ordered[idx]


def _latest_log_dir(data_dir: str) -> Path | None:
    base = Path(data_dir) / "logs"
    if not base.exists():
        return None
    dirs = [d for d in base.iterdir() if d.is_dir()]
    if not dirs:
        return None
    return sorted(dirs)[-1]


def load_records(log_dir: Path) -> list[dict[str, Any]]:
    path = log_dir / "events.ndjson"
    if not path.exists():
        raise FileNotFoundError(f"missing log file: {path}")
    records: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def build_summary(records: list[dict[str, Any]]) -> dict[str, Any]:
    heartbeats = [r for r in records if r.get("record_type") == "heartbeat"]
    alarms = [r for r in records if r.get("record_type") == "alarm"]
    reconcile_results = [r for r in records if r.get("record_type") == "reconcile_result"]
    window_ends = [r for r in records if r.get("record_type") == "window_end"]
    run_headers = [r for r in records if r.get("record_type") == "run_header"]

    runtime_hours = 0.0
    ws_downtime_s = 0.0
    if heartbeats:
        heartbeats_sorted = sorted(heartbeats, key=lambda r: r.get("ts_mono_ns", 0))
        start_ns = heartbeats_sorted[0].get("ts_mono_ns", 0)
        end_ns = heartbeats_sorted[-1].get("ts_mono_ns", 0)
        runtime_hours = max(0.0, (end_ns - start_ns) / 3_600_000_000_000)
        for idx, hb in enumerate(heartbeats_sorted[:-1]):
            next_ns = heartbeats_sorted[idx + 1].get("ts_mono_ns", 0)
            if hb.get("ws_disconnected"):
                ws_downtime_s += max(0, (next_ns - hb.get("ts_mono_ns", 0)) / 1_000_000_000)

    taint_fraction = 0.0
    if heartbeats:
        tainted_count = sum(1 for hb in heartbeats if hb.get("tainted"))
        taint_fraction = tainted_count / len(heartbeats)

    alarm_counts: dict[str, int] = {}
    for alarm in alarms:
        code = alarm.get("code", "unknown")
        alarm_counts[code] = alarm_counts.get(code, 0) + 1

    desync_count = sum(1 for r in reconcile_results if r.get("desynced"))
    no_feed_count = alarm_counts.get("no_feed", 0)
    rest_unavailable_count = alarm_counts.get("reconcile_failed", 0)

    by_size: dict[str, Any] = {}
    for win in window_ends:
        size = str(win.get("size"))
        by_size.setdefault(size, {"durations": [], "edges": [], "end_reasons": {}})
        by_size[size]["durations"].append(int(win.get("duration_ms", 0)))
        by_size[size]["edges"].append(int(win.get("best_edge_per_share_micro", 0)))
        reason = win.get("end_reason", "unknown")
        by_size[size]["end_reasons"][reason] = by_size[size]["end_reasons"].get(reason, 0) + 1

    size_summary: dict[str, Any] = {}
    for size, data in by_size.items():
        durations = data["durations"]
        edges = data["edges"]
        size_summary[size] = {
            "windows": len(durations),
            "windows_per_hour": (len(durations) / runtime_hours) if runtime_hours else 0.0,
            "duration_p50_ms": _percentile(durations, 0.50),
            "duration_p95_ms": _percentile(durations, 0.95),
            "duration_p99_ms": _percentile(durations, 0.99),
            "edge_p50_cents": micro_to_cents(_percentile(edges, 0.50)),
            "edge_p95_cents": micro_to_cents(_percentile(edges, 0.95)),
            "edge_p99_cents": micro_to_cents(_percentile(edges, 0.99)),
            "end_reasons": data["end_reasons"],
        }

    market_counts: dict[str, int] = {}
    for win in window_ends:
        market_id = win.get("market_id", "unknown")
        market_counts[market_id] = market_counts.get(market_id, 0) + 1
    top_markets = [
        {
            "market_id": mid,
            "windows": count,
            "windows_per_hour": (count / runtime_hours) if runtime_hours else 0.0,
        }
        for mid, count in sorted(market_counts.items(), key=lambda kv: kv[1], reverse=True)[:20]
    ]

    auto_sizes = {}
    if run_headers:
        last = run_headers[-1]
        if last.get("auto_sizes_frozen"):
            auto_sizes = {
                "sizes": last.get("sizes"),
                "sizes_auto_warmup_minutes": last.get("sizes_auto_warmup_minutes"),
            }

    return {
        "taint_fraction": taint_fraction,
        "runtime_hours": runtime_hours,
        "ws_downtime_s": ws_downtime_s,
        "alarms": alarm_counts,
        "by_size": size_summary,
        "top_markets": top_markets,
        "auto_sizes": auto_sizes,
        "desync_count": desync_count,
        "no_feed_count": no_feed_count,
        "rest_unavailable_count": rest_unavailable_count,
    }


def write_summary(data_dir: str, summary: dict[str, Any]) -> None:
    reports_dir = Path(data_dir) / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    json_path = reports_dir / "summary.json"
    csv_path = reports_dir / "summary.csv"
    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(summary, handle, indent=2)
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "size",
                "windows",
                "windows_per_hour",
                "duration_p50_ms",
                "duration_p95_ms",
                "duration_p99_ms",
                "edge_p50_cents",
                "edge_p95_cents",
                "edge_p99_cents",
                "end_reasons",
            ]
        )
        for size, data in summary.get("by_size", {}).items():
            writer.writerow(
                [
                    size,
                    data.get("windows"),
                    data.get("windows_per_hour"),
                    data.get("duration_p50_ms"),
                    data.get("duration_p95_ms"),
                    data.get("duration_p99_ms"),
                    data.get("edge_p50_cents"),
                    data.get("edge_p95_cents"),
                    data.get("edge_p99_cents"),
                    json.dumps(data.get("end_reasons", {}), separators=(",", ":")),
                ]
            )


def generate_report(data_dir: str) -> dict[str, Any]:
    log_dir = _latest_log_dir(data_dir)
    if log_dir is None:
        raise FileNotFoundError("no logs found")
    records = load_records(log_dir)
    summary = build_summary(records)
    write_summary(data_dir, summary)
    return summary
