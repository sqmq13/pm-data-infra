from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .capture_format import (
    append_record,
    frames_header_len,
    frames_header_struct,
    frames_magic,
    read_idx,
    _schema_from_magic,
)


@dataclass(frozen=True)
class SliceResult:
    slice_dir: Path
    records: int
    shards: int


def _default_slice_id() -> str:
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    suffix = uuid.uuid4().hex[:8]
    return f"{stamp}-{suffix}"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
    path.write_text(data + "\n", encoding="utf-8")


def _select_entries(
    entries,
    *,
    start_mono_ns: int | None,
    end_mono_ns: int | None,
    start_offset: int | None,
    end_offset: int | None,
) -> list:
    selected = []
    for entry in entries:
        if start_mono_ns is not None or end_mono_ns is not None:
            if start_mono_ns is not None and entry.rx_mono_ns < start_mono_ns:
                continue
            if end_mono_ns is not None and entry.rx_mono_ns > end_mono_ns:
                continue
            selected.append(entry)
            continue
        if start_offset is not None and entry.offset_frames < start_offset:
            continue
        if end_offset is not None and entry.offset_frames > end_offset:
            continue
        selected.append(entry)
    return selected


def slice_run(
    run_dir: Path,
    *,
    out_dir: Path | None = None,
    slice_id: str | None = None,
    shard: int | None = None,
    start_mono_ns: int | None = None,
    end_mono_ns: int | None = None,
    start_offset: int | None = None,
    end_offset: int | None = None,
) -> SliceResult:
    run_dir = run_dir.resolve()
    manifest_path = run_dir / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(str(manifest_path))
    if (start_mono_ns is None and end_mono_ns is None) and (
        start_offset is None and end_offset is None
    ):
        raise ValueError("provide a mono-ns window or an offset window")
    if (start_mono_ns is not None or end_mono_ns is not None) and (
        start_offset is not None or end_offset is not None
    ):
        raise ValueError("choose mono-ns window or offset window, not both")

    slice_id = slice_id or _default_slice_id()
    if out_dir is None:
        base_dir = run_dir.parent.parent if run_dir.parent.name == "runs" else run_dir.parent
        out_dir = base_dir / "slices" / slice_id
    out_dir = out_dir.resolve()
    capture_out = out_dir / "capture"
    capture_out.mkdir(parents=True, exist_ok=False)

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    slice_manifest = {
        "slice_id": slice_id,
        "source_run_id": manifest.get("run_id"),
        "source_run_dir": str(run_dir),
        "slice": {
            "start_mono_ns": start_mono_ns,
            "end_mono_ns": end_mono_ns,
            "start_offset": start_offset,
            "end_offset": end_offset,
            "shard": shard,
        },
        "source_manifest": manifest,
    }
    _write_json(out_dir / "manifest.json", slice_manifest)

    capture_dir = run_dir / "capture"
    idx_paths = sorted(capture_dir.glob("*.idx"))
    if shard is not None:
        idx_paths = [path for path in idx_paths if path.stem.endswith(f"_{shard:02d}")]
    if not idx_paths:
        raise ValueError("no idx files found for slicing")

    total_records = 0
    for idx_path in idx_paths:
        frames_path = idx_path.with_suffix(".frames")
        entries = read_idx(idx_path, frames_path=frames_path)
        selected = _select_entries(
            entries,
            start_mono_ns=start_mono_ns,
            end_mono_ns=end_mono_ns,
            start_offset=start_offset,
            end_offset=end_offset,
        )
        if not selected:
            continue
        out_frames_path = capture_out / frames_path.name
        out_idx_path = capture_out / idx_path.name
        with frames_path.open("rb") as frames_fh, out_frames_path.open(
            "ab"
        ) as out_frames_fh, out_idx_path.open("ab") as out_idx_fh:
            for entry in selected:
                frames_fh.seek(entry.offset_frames)
                magic = frames_fh.read(8)
                if len(magic) != 8:
                    raise ValueError("truncated frames header")
                schema_version = _schema_from_magic(magic)
                header_len = frames_header_len(schema_version)
                header_bytes = magic + frames_fh.read(header_len - 8)
                if len(header_bytes) != header_len:
                    raise ValueError("truncated frames header")
                header_struct = frames_header_struct(schema_version)
                if schema_version == 1:
                    magic, schema_field, flags, rx_mono_ns, payload_len, payload_crc32 = (
                        header_struct.unpack(header_bytes)
                    )
                    rx_wall_ns_utc = 0
                else:
                    (
                        magic,
                        schema_field,
                        flags,
                        rx_mono_ns,
                        rx_wall_ns_utc,
                        payload_len,
                        payload_crc32,
                    ) = header_struct.unpack(header_bytes)
                if magic != frames_magic(schema_version):
                    raise ValueError("bad frames magic")
                if schema_field != schema_version:
                    raise ValueError("schema version mismatch")
                if (
                    payload_len != entry.payload_len
                    or payload_crc32 != entry.payload_crc32
                    or rx_mono_ns != entry.rx_mono_ns
                    or rx_wall_ns_utc != entry.rx_wall_ns_utc
                ):
                    raise ValueError("idx/frames mismatch")
                payload = frames_fh.read(payload_len)
                if len(payload) != payload_len:
                    raise ValueError("truncated frames payload")
                append_record(
                    out_frames_fh,
                    out_idx_fh,
                    payload,
                    rx_mono_ns,
                    rx_wall_ns_utc,
                    flags=flags,
                    schema_version=schema_version,
                )
                total_records += 1

    return SliceResult(out_dir, total_records, len(idx_paths))
