from __future__ import annotations

import heapq
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncIterator, Iterable

from pm_arb.capture_format import (
    frames_header_len,
    frames_header_struct,
    frames_magic,
    read_idx,
    _schema_from_magic,
)


@dataclass(slots=True)
class RawFrame:
    payload: bytes
    rx_mono_ns: int
    rx_wall_ns_utc: int
    shard_id: int | None
    idx_i: int | None = None


@dataclass(slots=True)
class _ShardStream:
    shard_id: int
    frames_path: Path
    idx_entries: list
    frames_fh: object


def _parse_shard_id(path: Path) -> int:
    stem = path.stem
    if "_" not in stem:
        raise ValueError(f"invalid shard filename: {path.name}")
    suffix = stem.rsplit("_", 1)[-1]
    return int(suffix)


def _read_frame_from_entry(stream: _ShardStream, entry, idx_i: int) -> RawFrame:
    frames_fh = stream.frames_fh
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
        magic, schema_field, _flags, rx_mono_ns, payload_len, payload_crc32 = (
            header_struct.unpack(header_bytes)
        )
        rx_wall_ns_utc = 0
    else:
        (
            magic,
            schema_field,
            _flags,
            rx_mono_ns,
            rx_wall_ns_utc,
            payload_len,
            payload_crc32,
        ) = header_struct.unpack(header_bytes)
    if magic != frames_magic(schema_version) or schema_field != schema_version:
        raise ValueError("frames header schema mismatch")
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
    return RawFrame(
        payload=payload,
        rx_mono_ns=rx_mono_ns,
        rx_wall_ns_utc=rx_wall_ns_utc,
        shard_id=stream.shard_id,
        idx_i=idx_i,
    )


class ReplayDataSource:
    def __init__(
        self,
        *,
        run_dir: Path,
        max_seconds: float | None = None,
    ) -> None:
        self._run_dir = Path(run_dir).resolve()
        self._max_seconds = max_seconds

    def iter_frames(self) -> Iterable[RawFrame]:
        capture_dir = self._run_dir / "capture"
        idx_paths = sorted(capture_dir.glob("*.idx"))
        if not idx_paths:
            raise ValueError(f"no idx files found under {capture_dir}")

        streams: list[_ShardStream] = []
        for idx_path in idx_paths:
            frames_path = idx_path.with_suffix(".frames")
            entries = read_idx(idx_path, frames_path=frames_path)
            if not entries:
                continue
            shard_id = _parse_shard_id(idx_path)
            frames_fh = frames_path.open("rb")
            streams.append(
                _ShardStream(
                    shard_id=shard_id,
                    frames_path=frames_path,
                    idx_entries=entries,
                    frames_fh=frames_fh,
                )
            )

        heap: list[tuple[int, int, int, _ShardStream]] = []
        for stream in streams:
            entry = stream.idx_entries[0]
            heapq.heappush(heap, (entry.rx_mono_ns, stream.shard_id, 0, stream))
        if not heap:
            for stream in streams:
                stream.frames_fh.close()
            return

        start_mono_ns = min(item[0] for item in heap)
        max_ns = None
        if self._max_seconds is not None:
            max_ns = int(self._max_seconds * 1_000_000_000)

        try:
            while heap:
                rx_mono_ns, _shard_id, idx_i, stream = heapq.heappop(heap)
                if max_ns is not None and rx_mono_ns - start_mono_ns > max_ns:
                    break
                entry = stream.idx_entries[idx_i]
                yield _read_frame_from_entry(stream, entry, idx_i)
                next_i = idx_i + 1
                if next_i < len(stream.idx_entries):
                    next_entry = stream.idx_entries[next_i]
                    heapq.heappush(
                        heap,
                        (next_entry.rx_mono_ns, stream.shard_id, next_i, stream),
                    )
        finally:
            for stream in streams:
                stream.frames_fh.close()

    async def stream(self) -> AsyncIterator[RawFrame]:
        for frame in self.iter_frames():
            yield frame
