from pathlib import Path

import pytest

from pm_data.capture_format import FRAMES_SCHEMA_VERSION, append_record, frames_header_len
from pm_data.runtime.replay import ReplayDataSource


def _write_single_record(run_dir: Path, payload: bytes) -> Path:
    capture_dir = run_dir / "capture"
    capture_dir.mkdir(parents=True)
    frames_path = capture_dir / "shard_00.frames"
    idx_path = capture_dir / "shard_00.idx"
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        append_record(
            frames_fh,
            idx_fh,
            payload,
            rx_mono_ns=1_000,
            rx_wall_ns_utc=1_000_000_000,
            schema_version=FRAMES_SCHEMA_VERSION,
        )
    return frames_path


def test_replay_crc_mismatch_fails(tmp_path: Path) -> None:
    run_dir = tmp_path / "mini_run"
    payload = (
        b'{"event_type":"book","asset_id":"tokenA","timestamp":1000,'
        b'"bids":[{"price":"0.40","size":"10"}],"asks":[{"price":"0.46","size":"8"}]}'
    )
    frames_path = _write_single_record(run_dir, payload)

    header_len = frames_header_len(FRAMES_SCHEMA_VERSION)
    with frames_path.open("r+b") as handle:
        handle.seek(header_len + len(payload) - 1)
        last_byte = handle.read(1)
        assert last_byte
        handle.seek(-1, 1)
        handle.write(bytes([last_byte[0] ^ 0xFF]))

    data_source = ReplayDataSource(run_dir=run_dir)
    with pytest.raises(ValueError, match="payload_crc32 mismatch"):
        list(data_source.iter_frames())

