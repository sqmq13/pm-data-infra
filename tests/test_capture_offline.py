from pathlib import Path

from pm_data.capture_format import verify_frames
from pm_data.capture_offline import run_capture_offline
from pm_data.config import Config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_capture_offline_writes_run_bundle(tmp_path: Path) -> None:
    fixtures_dir = _repo_root() / "testdata" / "fixtures"
    config = Config(data_dir=str(tmp_path), offline=True)
    result = run_capture_offline(config, fixtures_dir, run_id="test-run")
    run_dir = result.run.run_dir

    manifest_path = run_dir / "manifest.json"
    assert manifest_path.exists()
    runlog_path = run_dir / "runlog.ndjson"
    assert runlog_path.exists()
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    assert frames_path.exists()
    assert idx_path.exists()

    summary = verify_frames(frames_path, idx_path=idx_path)
    assert summary["ok"] is True

    runlog_lines = runlog_path.read_text(encoding="utf-8").strip().splitlines()
    assert any('"record_type":"run_start"' in line for line in runlog_lines)
    assert any('"record_type":"heartbeat"' in line for line in runlog_lines)

    metrics_global = run_dir / "metrics" / "global.ndjson"
    metrics_shard = run_dir / "metrics" / "shard_00.ndjson"
    assert metrics_global.exists()
    assert metrics_shard.exists()
