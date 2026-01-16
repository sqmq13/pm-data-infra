import json
from pathlib import Path

from pm_data.capture_inspect import audit_capture_quality
from pm_data.cli import main


def _write_runlog(run_dir: Path, records: list[dict]) -> None:
    path = run_dir / "runlog.ndjson"
    path.write_text(
        "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n",
        encoding="utf-8",
    )


def _write_minimal_manifest_and_metrics(run_dir: Path, *, run_id: str = "run") -> None:
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "manifest_version": 1,
                "config": {"capture_heartbeat_interval_seconds": 1.0},
            },
            separators=(",", ":"),
        )
        + "\n",
        encoding="utf-8",
    )
    metrics_dir = run_dir / "metrics"
    metrics_dir.mkdir(exist_ok=True)
    (metrics_dir / "global.ndjson").write_text(
        "\n".join(
            json.dumps(record, separators=(",", ":"))
            for record in [
                {
                    "record_type": "heartbeat",
                    "run_id": run_id,
                    "hb_mono_ns": 0,
                    "loop_lag_ms_max_last_interval": 0.0,
                    "dropped_messages_total": 0,
                    "dropped_messages_count": 0,
                    "ws_in_q_depth_max": 0,
                    "parse_q_depth_max": 0,
                    "write_q_depth_max": 0,
                    "ws_rx_to_parsed_ms_p99": 0.0,
                    "ws_rx_to_applied_ms_p99": 0.0,
                    "backpressure_ns_p99": 0,
                },
                {
                    "record_type": "heartbeat",
                    "run_id": run_id,
                    "hb_mono_ns": 1_000_000_000,
                    "loop_lag_ms_max_last_interval": 0.0,
                    "dropped_messages_total": 0,
                    "dropped_messages_count": 0,
                    "ws_in_q_depth_max": 0,
                    "parse_q_depth_max": 0,
                    "write_q_depth_max": 0,
                    "ws_rx_to_parsed_ms_p99": 0.0,
                    "ws_rx_to_applied_ms_p99": 0.0,
                    "backpressure_ns_p99": 0,
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )


def test_audit_capture_quality_from_capture_stop(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_minimal_manifest_and_metrics(run_dir)
    records = [
        {"record_type": "run_start", "run_id": "run"},
        {"record_type": "heartbeat", "hb_mono_ns": 0},
        {
            "record_type": "capture_stop",
            "run_id": "run",
            "quality_status": "degraded",
            "quality_ok": False,
            "quality_reasons": ["parse_queue_drops"],
            "parse_worker_dropped": 3,
            "parse_results_dropped": 0,
            "loss_parse_queue_drops": 3,
            "loss_parse_results_drops": 0,
            "loss_metrics_drops": 0,
        },
    ]
    _write_runlog(run_dir, records)

    summary = audit_capture_quality(run_dir)
    assert summary["quality_ok"] is False
    assert summary["quality_status"] == "degraded"
    assert "parse_queue_drops" in summary["quality_reasons"]
    assert summary["parse_worker_dropped"] == 3


def test_cli_capture_audit_nonzero_on_degraded(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_minimal_manifest_and_metrics(run_dir)
    records = [
        {"record_type": "run_start", "run_id": "run"},
        {"record_type": "heartbeat", "hb_mono_ns": 0},
        {"record_type": "heartbeat", "hb_mono_ns": 1_000_000_000},
        {
            "record_type": "capture_stop",
            "run_id": "run",
            "quality_status": "degraded",
            "quality_ok": False,
            "quality_reasons": ["parse_queue_drops"],
            "parse_worker_dropped": 1,
            "parse_results_dropped": 0,
            "loss_parse_queue_drops": 1,
            "loss_parse_results_drops": 0,
            "loss_metrics_drops": 0,
        },
    ]
    _write_runlog(run_dir, records)

    rc = main(["capture-audit", "--run-dir", str(run_dir)])
    _ = capsys.readouterr()
    assert rc == 1


def test_cli_capture_audit_zero_on_ok(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_minimal_manifest_and_metrics(run_dir)
    records = [
        {"record_type": "run_start", "run_id": "run"},
        {"record_type": "heartbeat", "hb_mono_ns": 0},
        {"record_type": "heartbeat", "hb_mono_ns": 1_000_000_000},
        {
            "record_type": "capture_stop",
            "run_id": "run",
            "quality_status": "ok",
            "quality_ok": True,
            "quality_reasons": [],
            "parse_worker_dropped": 0,
            "parse_results_dropped": 0,
            "loss_parse_queue_drops": 0,
            "loss_parse_results_drops": 0,
            "loss_metrics_drops": 0,
        },
    ]
    _write_runlog(run_dir, records)

    rc = main(["capture-audit", "--run-dir", str(run_dir)])
    _ = capsys.readouterr()
    assert rc == 0


def test_cli_capture_audit_nonzero_on_loop_lag_gate(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_minimal_manifest_and_metrics(run_dir)
    (run_dir / "metrics" / "global.ndjson").write_text(
        "\n".join(
            json.dumps(record, separators=(",", ":"))
            for record in [
                {
                    "record_type": "heartbeat",
                    "run_id": "run",
                    "hb_mono_ns": 0,
                    "loop_lag_ms_max_last_interval": 200.0,
                    "dropped_messages_total": 0,
                    "dropped_messages_count": 0,
                },
                {
                    "record_type": "heartbeat",
                    "run_id": "run",
                    "hb_mono_ns": 1_000_000_000,
                    "loop_lag_ms_max_last_interval": 200.0,
                    "dropped_messages_total": 0,
                    "dropped_messages_count": 0,
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    records = [
        {"record_type": "run_start", "run_id": "run"},
        {"record_type": "heartbeat", "hb_mono_ns": 0},
        {"record_type": "heartbeat", "hb_mono_ns": 1_000_000_000},
        {
            "record_type": "capture_stop",
            "run_id": "run",
            "quality_status": "ok",
            "quality_ok": True,
            "quality_reasons": [],
            "parse_worker_dropped": 0,
            "parse_results_dropped": 0,
            "loss_parse_queue_drops": 0,
            "loss_parse_results_drops": 0,
            "loss_metrics_drops": 0,
        },
    ]
    _write_runlog(run_dir, records)

    rc = main(["capture-audit", "--run-dir", str(run_dir)])
    _ = capsys.readouterr()
    assert rc == 1


def test_cli_capture_audit_nonzero_on_pressure_streak(tmp_path: Path, capsys) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text(
        json.dumps({"run_id": "run", "manifest_version": 1, "config": {}}, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    metrics_dir = run_dir / "metrics"
    metrics_dir.mkdir(exist_ok=True)
    metrics_records = []
    for idx in range(6):
        metrics_records.append(
            {
                "record_type": "heartbeat",
                "run_id": "run",
                "hb_mono_ns": idx * 1_000_000_000,
                "loop_lag_ms_max_last_interval": 0.0,
                "dropped_messages_total": 0,
                "dropped_messages_count": 0,
                "parse_q_depth_max": 250,
            }
        )
    (metrics_dir / "global.ndjson").write_text(
        "\n".join(json.dumps(record, separators=(",", ":")) for record in metrics_records)
        + "\n",
        encoding="utf-8",
    )
    runlog_records = [{"record_type": "run_start", "run_id": "run"}]
    for idx in range(7):
        runlog_records.append({"record_type": "heartbeat", "hb_mono_ns": idx * 1_000_000_000})
    runlog_records.append(
        {
            "record_type": "capture_stop",
            "run_id": "run",
            "quality_status": "ok",
            "quality_ok": True,
            "quality_reasons": [],
            "parse_worker_dropped": 0,
            "parse_results_dropped": 0,
            "loss_parse_queue_drops": 0,
            "loss_parse_results_drops": 0,
            "loss_metrics_drops": 0,
        }
    )
    _write_runlog(run_dir, runlog_records)

    rc = main(["capture-audit", "--run-dir", str(run_dir)])
    _ = capsys.readouterr()
    assert rc == 1
