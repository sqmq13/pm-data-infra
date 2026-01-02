import json
from pathlib import Path

from pm_arb.config import Config
from pm_arb.engine import Engine


def _load_records(log_dir: Path) -> list[dict]:
    path = log_dir / "events.ndjson"
    records = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            records.append(json.loads(line))
    return records


def test_scan_offline_creates_logs(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    config = Config(offline=True, data_dir=str(data_dir))
    engine = Engine(config, fixtures_dir="testdata/fixtures")
    exit_code = engine.scan_offline()
    assert exit_code == 0
    log_root = data_dir / "logs"
    log_dirs = [d for d in log_root.iterdir() if d.is_dir()]
    assert log_dirs
    records = _load_records(sorted(log_dirs)[-1])
    record_types = {rec["record_type"] for rec in records}
    assert "run_header" in record_types
    assert "market_poll_summary" in record_types
    assert "window_start" in record_types
    assert "window_end" in record_types
    assert "alarm" in record_types
    ws_samples = data_dir / "debug" / "ws_samples.jsonl"
    assert ws_samples.exists()
    lines = ws_samples.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) <= config.ws_sample_capture_n
