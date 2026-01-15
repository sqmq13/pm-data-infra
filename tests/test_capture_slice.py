from pathlib import Path

from pm_data.capture_format import read_idx, verify_frames
from pm_data.capture_offline import run_capture_offline
from pm_data.capture_slice import slice_run
from pm_data.config import Config


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_capture_slice_offset_window(tmp_path: Path) -> None:
    fixtures_dir = _repo_root() / "testdata" / "fixtures"
    config = Config(data_dir=str(tmp_path), offline=True)
    result = run_capture_offline(config, fixtures_dir, run_id="slice-run")
    run_dir = result.run.run_dir

    idx_path = run_dir / "capture" / "shard_00.idx"
    entries = read_idx(idx_path, frames_path=run_dir / "capture" / "shard_00.frames")
    assert len(entries) >= 5
    start_offset = entries[2].offset_frames
    end_offset = entries[4].offset_frames

    slice_dir = tmp_path / "slice_out"
    slice_result = slice_run(
        run_dir,
        out_dir=slice_dir,
        start_offset=start_offset,
        end_offset=end_offset,
    )

    slice_frames = slice_result.slice_dir / "capture" / "shard_00.frames"
    slice_idx = slice_result.slice_dir / "capture" / "shard_00.idx"
    summary = verify_frames(slice_frames, idx_path=slice_idx)
    assert summary["ok"] is True
    assert summary["records"] == 3
