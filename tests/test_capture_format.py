from pathlib import Path

from pm_data.capture_format import (
    FRAMES_HEADER_LEN,
    append_record,
    read_frames,
    read_idx,
    verify_frames,
)


def _write_single_record(
    frames_path: Path,
    idx_path: Path,
    payload: bytes,
    rx_mono_ns: int,
    *,
    rx_wall_ns_utc: int = 0,
    schema_version: int = 2,
) -> None:
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        append_record(
            frames_fh,
            idx_fh,
            payload,
            rx_mono_ns,
            rx_wall_ns_utc,
            schema_version=schema_version,
        )


def test_frames_round_trip_v1(tmp_path: Path) -> None:
    frames_path = tmp_path / "shard_00.frames"
    idx_path = tmp_path / "shard_00.idx"
    payloads = [b"alpha", b"bravo"]
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        append_record(frames_fh, idx_fh, payloads[0], 123, schema_version=1)
        append_record(frames_fh, idx_fh, payloads[1], 456, schema_version=1)
    records = read_frames(frames_path)
    assert [record.payload for record in records] == payloads
    idx_records = read_idx(idx_path, frames_path=frames_path)
    assert idx_records[0].offset_frames == records[0].offset
    assert idx_records[1].offset_frames == records[1].offset
    summary = verify_frames(frames_path, idx_path=idx_path)
    assert summary["ok"] is True
    assert summary["schema_versions"] == [1]


def test_frames_round_trip_v2(tmp_path: Path) -> None:
    frames_path = tmp_path / "shard_00.frames"
    idx_path = tmp_path / "shard_00.idx"
    payloads = [b"alpha", b"bravo"]
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        append_record(frames_fh, idx_fh, payloads[0], 123, 999, schema_version=2)
        append_record(frames_fh, idx_fh, payloads[1], 456, 1001, schema_version=2)
    records = read_frames(frames_path)
    assert [record.payload for record in records] == payloads
    assert [record.rx_wall_ns_utc for record in records] == [999, 1001]
    idx_records = read_idx(idx_path, frames_path=frames_path)
    assert idx_records[0].offset_frames == records[0].offset
    assert idx_records[1].offset_frames == records[1].offset
    summary = verify_frames(frames_path, idx_path=idx_path)
    assert summary["ok"] is True
    assert summary["schema_versions"] == [2]


def test_frames_mixed_versions_idx_rejected(tmp_path: Path) -> None:
    frames_path = tmp_path / "mixed.frames"
    idx_path = tmp_path / "mixed.idx"
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        append_record(frames_fh, idx_fh, b"alpha", 123, schema_version=1)
        append_record(frames_fh, idx_fh, b"bravo", 456, 999, schema_version=2)
    summary_no_idx = verify_frames(frames_path)
    assert summary_no_idx["ok"] is True
    assert summary_no_idx["schema_versions"] == [1, 2]
    summary = verify_frames(frames_path, idx_path=idx_path)
    assert summary["ok"] is False
    assert summary["idx_ok"] is False


def test_frames_truncation_detected(tmp_path: Path) -> None:
    frames_path = tmp_path / "good.frames"
    idx_path = tmp_path / "good.idx"
    _write_single_record(frames_path, idx_path, b"alpha", 123, schema_version=2)
    data = frames_path.read_bytes()
    truncated_path = tmp_path / "truncated.frames"
    truncated_path.write_bytes(data[:-1])
    summary = verify_frames(truncated_path)
    assert summary["ok"] is False
    assert summary["truncated"] is True
    assert summary["records"] == 0
    assert summary["first_bad_offset"] == 0


def test_frames_crc_mismatch_detected(tmp_path: Path) -> None:
    frames_path = tmp_path / "good.frames"
    idx_path = tmp_path / "good.idx"
    _write_single_record(frames_path, idx_path, b"alpha", 123, schema_version=2)
    data = bytearray(frames_path.read_bytes())
    data[FRAMES_HEADER_LEN] ^= 0xFF
    bad_path = tmp_path / "bad_crc.frames"
    bad_path.write_bytes(data)
    summary = verify_frames(bad_path)
    assert summary["ok"] is False
    assert summary["crc_mismatch"] == 1
    assert summary["records"] == 0


def test_idx_mismatch_detected(tmp_path: Path) -> None:
    frames_path = tmp_path / "good.frames"
    idx_path = tmp_path / "good.idx"
    _write_single_record(frames_path, idx_path, b"alpha", 123, schema_version=2)
    idx_data = bytearray(idx_path.read_bytes())
    idx_data[-1] ^= 0xFF
    bad_idx_path = tmp_path / "bad.idx"
    bad_idx_path.write_bytes(idx_data)
    summary = verify_frames(frames_path, idx_path=bad_idx_path)
    assert summary["idx_ok"] is False
    assert summary["ok"] is False
