import json
from pathlib import Path

from pm_arb.capture_inspect import audit_heartbeat_gaps, inspect_run, write_latency_report
from pm_arb.capture_offline import run_capture_offline
from pm_arb.config import Config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_capture_inspect_summary(tmp_path: Path) -> None:
    fixtures_dir = _repo_root() / "testdata" / "fixtures"
    config = Config(data_dir=str(tmp_path), offline=True)
    result = run_capture_offline(config, fixtures_dir, run_id="inspect-run")
    summary = inspect_run(result.run.run_dir)
    payload = summary.payload

    assert payload["run_id"] == result.run.run_id
    assert payload["shards"]["count"] == 1
    assert payload["shards"]["records"] == result.stats.frames
    assert payload["metrics"]["global"]["count"] >= 1
    shard = payload["shards"]["by_shard"]["shard_00"]
    assert shard["rx_wall_ns_utc_first"] is not None
    assert shard["rx_wall_ns_utc_last"] is not None


def test_audit_heartbeat_gaps(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    runlog_path = run_dir / "runlog.ndjson"
    records = [
        {"record_type": "run_start", "run_id": "run"},
        {"record_type": "heartbeat", "hb_mono_ns": 0},
        {"record_type": "universe_refresh", "reason": "APPLIED"},
        {"record_type": "heartbeat", "hb_mono_ns": 3_000_000_000},
        {"record_type": "heartbeat", "hb_mono_ns": 4_000_000_000},
    ]
    runlog_path.write_text(
        "\n".join(json.dumps(record, separators=(",", ":")) for record in records) + "\n",
        encoding="utf-8",
    )
    summary = audit_heartbeat_gaps(run_dir, threshold_seconds=2.0)
    assert summary["hb_count"] == 3
    assert summary["gaps_over_threshold"] == 1
    assert summary["max_gap_seconds"] == 3.0
    assert summary["max_gap"]["record_types_between"]["universe_refresh"] == 1


def test_latency_report_writes(tmp_path: Path) -> None:
    fixtures_dir = _repo_root() / "testdata" / "fixtures"
    config = Config(data_dir=str(tmp_path), offline=True)
    result = run_capture_offline(config, fixtures_dir, run_id="latency-report-run")
    report = write_latency_report(result.run.run_dir)
    assert report["run_id"] == result.run.run_id
    json_path = result.run.run_dir / "latency_report.json"
    text_path = result.run.run_dir / "latency_report.txt"
    assert json_path.exists()
    assert text_path.exists()
