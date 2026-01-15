from __future__ import annotations

import struct
import zlib
from dataclasses import dataclass
from pathlib import Path

FRAMES_MAGIC_V1 = b"PMCAPv01"
FRAMES_MAGIC_V2 = b"PMCAPv02"
FRAMES_SCHEMA_VERSION = 2

FRAMES_HEADER_STRUCT_V1 = struct.Struct("<8sHHQII")
FRAMES_HEADER_STRUCT_V2 = struct.Struct("<8sHHQQII")
FRAMES_HEADER_STRUCT = FRAMES_HEADER_STRUCT_V2
FRAMES_HEADER_LEN = FRAMES_HEADER_STRUCT.size

IDX_STRUCT_V1 = struct.Struct("<QQII")
IDX_STRUCT_V2 = struct.Struct("<QQQII")
IDX_STRUCT = IDX_STRUCT_V2
IDX_ENTRY_LEN = IDX_STRUCT.size

FLAG_TEXT_PAYLOAD = 1
FLAG_BINARY_PAYLOAD = 2
FLAG_DECODED_PAYLOAD = 4


@dataclass(frozen=True)
class FrameRecord:
    offset: int
    schema_version: int
    flags: int
    rx_mono_ns: int
    rx_wall_ns_utc: int
    payload_len: int
    payload_crc32: int
    payload: bytes


@dataclass(frozen=True)
class IdxRecord:
    offset_frames: int
    rx_mono_ns: int
    rx_wall_ns_utc: int
    payload_len: int
    payload_crc32: int


def frames_magic(schema_version: int) -> bytes:
    if schema_version == 1:
        return FRAMES_MAGIC_V1
    if schema_version == 2:
        return FRAMES_MAGIC_V2
    raise ValueError(f"unsupported frames schema version: {schema_version}")


def frames_header_struct(schema_version: int) -> struct.Struct:
    if schema_version == 1:
        return FRAMES_HEADER_STRUCT_V1
    if schema_version == 2:
        return FRAMES_HEADER_STRUCT_V2
    raise ValueError(f"unsupported frames schema version: {schema_version}")


def frames_header_len(schema_version: int) -> int:
    return frames_header_struct(schema_version).size


def idx_struct(schema_version: int) -> struct.Struct:
    if schema_version == 1:
        return IDX_STRUCT_V1
    if schema_version == 2:
        return IDX_STRUCT_V2
    raise ValueError(f"unsupported frames schema version: {schema_version}")


def idx_entry_len(schema_version: int) -> int:
    return idx_struct(schema_version).size


def _schema_from_magic(magic: bytes) -> int:
    if magic == FRAMES_MAGIC_V1:
        return 1
    if magic == FRAMES_MAGIC_V2:
        return 2
    raise ValueError("bad magic")


def _unpack_header_from(data: bytes, offset: int) -> tuple[int, int, int, int, int, int, int]:
    if len(data) - offset < 8:
        raise ValueError(f"truncated header at offset {offset}")
    magic = data[offset : offset + 8]
    schema_version = _schema_from_magic(magic)
    header_struct = frames_header_struct(schema_version)
    header_len = header_struct.size
    if len(data) - offset < header_len:
        raise ValueError(f"truncated header at offset {offset}")
    if schema_version == 1:
        magic, schema_field, flags, rx_mono_ns, payload_len, payload_crc32 = (
            header_struct.unpack_from(data, offset)
        )
        rx_wall_ns_utc = 0
    else:
        magic, schema_field, flags, rx_mono_ns, rx_wall_ns_utc, payload_len, payload_crc32 = (
            header_struct.unpack_from(data, offset)
        )
    if schema_field != schema_version:
        raise ValueError(f"schema version mismatch at offset {offset}")
    return (
        schema_version,
        flags,
        rx_mono_ns,
        rx_wall_ns_utc,
        payload_len,
        payload_crc32,
        header_len,
    )


def _crc32(payload: bytes) -> int:
    return zlib.crc32(payload) & 0xFFFFFFFF


def encode_frame(
    payload: bytes,
    rx_mono_ns: int,
    rx_wall_ns_utc: int = 0,
    *,
    flags: int = 0,
    schema_version: int = FRAMES_SCHEMA_VERSION,
) -> bytes:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError("payload must be bytes-like")
    payload_bytes = bytes(payload)
    payload_crc32 = _crc32(payload_bytes)
    header_struct = frames_header_struct(schema_version)
    if schema_version == 1:
        header = header_struct.pack(
            frames_magic(schema_version),
            schema_version,
            flags,
            rx_mono_ns,
            len(payload_bytes),
            payload_crc32,
        )
    else:
        header = header_struct.pack(
            frames_magic(schema_version),
            schema_version,
            flags,
            rx_mono_ns,
            rx_wall_ns_utc,
            len(payload_bytes),
            payload_crc32,
        )
    return header + payload_bytes


def append_record(
    frames_fh,
    idx_fh,
    payload: bytes,
    rx_mono_ns: int,
    rx_wall_ns_utc: int = 0,
    *,
    flags: int = 0,
    schema_version: int = FRAMES_SCHEMA_VERSION,
) -> FrameRecord:
    if not isinstance(payload, (bytes, bytearray, memoryview)):
        raise TypeError("payload must be bytes-like")
    payload_bytes = bytes(payload)
    payload_crc32 = _crc32(payload_bytes)
    offset = frames_fh.tell()
    header_struct = frames_header_struct(schema_version)
    if schema_version == 1:
        header = header_struct.pack(
            frames_magic(schema_version),
            schema_version,
            flags,
            rx_mono_ns,
            len(payload_bytes),
            payload_crc32,
        )
    else:
        header = header_struct.pack(
            frames_magic(schema_version),
            schema_version,
            flags,
            rx_mono_ns,
            rx_wall_ns_utc,
            len(payload_bytes),
            payload_crc32,
        )
    frames_fh.write(header)
    frames_fh.write(payload_bytes)
    if idx_fh is not None:
        idx_struct_used = idx_struct(schema_version)
        if schema_version == 1:
            idx_fh.write(
                idx_struct_used.pack(offset, rx_mono_ns, len(payload_bytes), payload_crc32)
            )
        else:
            idx_fh.write(
                idx_struct_used.pack(
                    offset, rx_mono_ns, rx_wall_ns_utc, len(payload_bytes), payload_crc32
                )
            )
    return FrameRecord(
        offset=offset,
        schema_version=schema_version,
        flags=flags,
        rx_mono_ns=rx_mono_ns,
        rx_wall_ns_utc=rx_wall_ns_utc,
        payload_len=len(payload_bytes),
        payload_crc32=payload_crc32,
        payload=payload_bytes,
    )


def read_frames(frames_path: Path) -> list[FrameRecord]:
    data = frames_path.read_bytes()
    size = len(data)
    offset = 0
    records: list[FrameRecord] = []
    while offset < size:
        (
            schema_version,
            flags,
            rx_mono_ns,
            rx_wall_ns_utc,
            payload_len,
            payload_crc32,
            header_len,
        ) = _unpack_header_from(data, offset)
        record_len = header_len + payload_len
        if offset + record_len > size:
            raise ValueError(f"truncated payload at offset {offset}")
        payload = data[offset + header_len : offset + record_len]
        if _crc32(payload) != payload_crc32:
            raise ValueError(f"crc mismatch at offset {offset}")
        records.append(
            FrameRecord(
                offset=offset,
                schema_version=schema_version,
                flags=flags,
                rx_mono_ns=rx_mono_ns,
                rx_wall_ns_utc=rx_wall_ns_utc,
                payload_len=payload_len,
                payload_crc32=payload_crc32,
                payload=payload,
            )
        )
        offset += record_len
    return records


def _infer_idx_schema_from_size(idx_size: int) -> int:
    v1_ok = idx_size % IDX_STRUCT_V1.size == 0
    v2_ok = idx_size % IDX_STRUCT_V2.size == 0
    if v1_ok and not v2_ok:
        return 1
    if v2_ok and not v1_ok:
        return 2
    if v1_ok and v2_ok:
        raise ValueError("ambiguous idx length; provide schema_version or frames_path")
    raise ValueError("idx size is not a multiple of entry length")


def _schema_from_frames_path(frames_path: Path) -> int | None:
    if not frames_path.exists():
        return None
    with frames_path.open("rb") as handle:
        magic = handle.read(8)
    if len(magic) < 8:
        return None
    return _schema_from_magic(magic)


def read_idx(
    idx_path: Path,
    *,
    schema_version: int | None = None,
    frames_path: Path | None = None,
) -> list[IdxRecord]:
    data = idx_path.read_bytes()
    size = len(data)
    if schema_version is None and frames_path is not None:
        schema_version = _schema_from_frames_path(frames_path)
    if schema_version is None:
        schema_version = _infer_idx_schema_from_size(size)
    entry_len = idx_entry_len(schema_version)
    if size % entry_len != 0:
        raise ValueError("idx size is not a multiple of entry length")
    records: list[IdxRecord] = []
    for offset in range(0, size, entry_len):
        if schema_version == 1:
            offset_frames, rx_mono_ns, payload_len, payload_crc32 = IDX_STRUCT_V1.unpack_from(
                data, offset
            )
            rx_wall_ns_utc = 0
        else:
            (
                offset_frames,
                rx_mono_ns,
                rx_wall_ns_utc,
                payload_len,
                payload_crc32,
            ) = IDX_STRUCT_V2.unpack_from(data, offset)
        records.append(
            IdxRecord(
                offset_frames=offset_frames,
                rx_mono_ns=rx_mono_ns,
                rx_wall_ns_utc=rx_wall_ns_utc,
                payload_len=payload_len,
                payload_crc32=payload_crc32,
            )
        )
    return records


def verify_frames(frames_path: Path, idx_path: Path | None = None) -> dict:
    data = frames_path.read_bytes()
    size = len(data)
    offset = 0
    records_meta: list[tuple[int, int, int, int]] = []
    errors = 0
    crc_mismatch = 0
    truncated = False
    first_bad_offset: int | None = None
    schema_versions: set[int] = set()

    while offset < size:
        try:
            (
                schema_version,
                flags,
                rx_mono_ns,
                rx_wall_ns_utc,
                payload_len,
                payload_crc32,
                header_len,
            ) = _unpack_header_from(data, offset)
        except ValueError as exc:
            if "bad magic" in str(exc):
                errors += 1
                if first_bad_offset is None:
                    first_bad_offset = offset
                next_offset = -1
                next_v1 = data.find(FRAMES_MAGIC_V1, offset + 1)
                next_v2 = data.find(FRAMES_MAGIC_V2, offset + 1)
                candidates = [pos for pos in (next_v1, next_v2) if pos != -1]
                if candidates:
                    next_offset = min(candidates)
                if next_offset == -1:
                    break
                offset = next_offset
                continue
            truncated = True
            errors += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            break
        schema_versions.add(schema_version)
        record_len = header_len + payload_len
        if offset + record_len > size:
            truncated = True
            errors += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            break
        payload = data[offset + header_len : offset + record_len]
        actual_crc32 = _crc32(payload)
        if actual_crc32 != payload_crc32:
            errors += 1
            crc_mismatch += 1
            if first_bad_offset is None:
                first_bad_offset = offset
            next_offset = -1
            next_v1 = data.find(FRAMES_MAGIC_V1, offset + 1)
            next_v2 = data.find(FRAMES_MAGIC_V2, offset + 1)
            candidates = [pos for pos in (next_v1, next_v2) if pos != -1]
            if candidates:
                next_offset = min(candidates)
            if next_offset == -1:
                break
            offset = next_offset
            continue
        records_meta.append(
            (offset, rx_mono_ns, rx_wall_ns_utc, payload_len, payload_crc32)
        )
        offset += record_len

    last_good_offset = records_meta[-1][0] if records_meta else None
    summary = {
        "ok": errors == 0 and not truncated,
        "records": len(records_meta),
        "errors": errors,
        "crc_mismatch": crc_mismatch,
        "truncated": truncated,
        "first_bad_offset": first_bad_offset,
        "last_good_offset": last_good_offset,
        "frames_bytes": size,
        "schema_versions": sorted(schema_versions),
    }

    if idx_path is not None:
        if not idx_path.exists():
            raise FileNotFoundError(str(idx_path))
        idx_data = idx_path.read_bytes()
        idx_size = len(idx_data)
        idx_ok = True
        idx_errors = 0
        idx_first_bad_offset: int | None = None
        idx_records = 0
        if len(schema_versions) != 1:
            idx_ok = False
            idx_errors += 1
            idx_first_bad_offset = 0
        else:
            schema_version = next(iter(schema_versions))
            entry_len = idx_entry_len(schema_version)
            if idx_size % entry_len != 0:
                idx_ok = False
                idx_errors += 1
                idx_first_bad_offset = idx_size - (idx_size % entry_len)
            idx_records = idx_size // entry_len
        idx_entries: list[tuple[int, int, int, int, int]] = []
        if idx_ok:
            for idx_offset in range(0, idx_records * entry_len, entry_len):
                if schema_version == 1:
                    offset_frames, rx_mono_ns, payload_len, payload_crc32 = (
                        IDX_STRUCT_V1.unpack_from(idx_data, idx_offset)
                    )
                    rx_wall_ns_utc = 0
                else:
                    (
                        offset_frames,
                        rx_mono_ns,
                        rx_wall_ns_utc,
                        payload_len,
                        payload_crc32,
                    ) = IDX_STRUCT_V2.unpack_from(idx_data, idx_offset)
                idx_entries.append(
                    (offset_frames, rx_mono_ns, rx_wall_ns_utc, payload_len, payload_crc32)
                )
        if idx_ok:
            if idx_records != len(records_meta):
                idx_ok = False
                idx_errors += 1
                if idx_first_bad_offset is None:
                    idx_first_bad_offset = 0
            else:
                for idx, entry in enumerate(idx_entries):
                    if entry != records_meta[idx]:
                        idx_ok = False
                        idx_errors += 1
                        if idx_first_bad_offset is None:
                            idx_first_bad_offset = idx * entry_len
                        break
        if not idx_ok and idx_first_bad_offset is None:
            idx_ok = False
            idx_errors += 1
            idx_first_bad_offset = 0
        summary.update(
            {
                "idx_ok": idx_ok,
                "idx_records": idx_records,
                "idx_errors": idx_errors,
                "idx_first_bad_offset": idx_first_bad_offset,
            }
        )
        summary["ok"] = summary["ok"] and idx_ok

    return summary
