import math
from pathlib import Path

from pm_data.capture_offline import quantile, run_capture_offline
from pm_data.config import Config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _bench_summary(config: Config, fixtures_dir: Path, *, multiplier: int) -> dict[str, float]:
    result = run_capture_offline(
        config,
        fixtures_dir,
        run_id="bench-run",
        emit_metrics=False,
        multiplier=multiplier,
    )
    elapsed_sec = max(result.elapsed_ns / 1_000_000_000.0, 1e-9)
    return {
        "frames": float(result.stats.frames),
        "msgs_per_sec": result.stats.frames / elapsed_sec,
        "bytes_per_sec": result.stats.bytes_written / elapsed_sec,
        "write_ns_p99": float(quantile(result.stats.write_durations_ns, 99)),
        "ingest_ns_p99": float(quantile(result.stats.ingest_latencies_ns, 99)),
    }


def test_capture_bench_regression_multiplier(tmp_path: Path) -> None:
    fixtures_dir = _repo_root() / "testdata" / "fixtures"
    config_v2 = Config(data_dir=str(tmp_path / "v2"), offline=True, capture_frames_schema_version=2)
    config_v1 = Config(data_dir=str(tmp_path / "v1"), offline=True, capture_frames_schema_version=1)
    summary_v2 = _bench_summary(config_v2, fixtures_dir, multiplier=200)
    summary_v1 = _bench_summary(config_v1, fixtures_dir, multiplier=200)

    assert summary_v2["msgs_per_sec"] > 0
    assert math.isfinite(summary_v2["write_ns_p99"])
    assert math.isfinite(summary_v2["ingest_ns_p99"])

    ratio = summary_v2["msgs_per_sec"] / summary_v1["msgs_per_sec"]
    assert math.isfinite(ratio)
    assert ratio > 0.1
