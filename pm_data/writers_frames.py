import contextlib
import queue
import threading
from dataclasses import dataclass
from typing import Any

from .capture import monotonic_ns
from .capture_format import append_record, frames_header_struct, frames_magic
from .capture_state import ShardState

_FRAME_WRITE_STOP = object()


@dataclass(frozen=True)
class _FrameWriteJob:
    shard_id: int
    payload_bytes: bytes
    rx_mono_ns: int
    rx_wall_ns_utc: int
    flags: int


@dataclass(frozen=True)
class _FrameWriteResult:
    shard_id: int
    payload_bytes: bytes
    header_bytes: bytes
    schema_version: int
    payload_len: int
    rx_mono_ns: int
    write_duration_ns: int
    ingest_latency_ns: int
    backpressure_ns: int


class _FrameWriter:
    def __init__(
        self,
        *,
        shards: list[ShardState],
        schema_version: int,
        max_queue: int = 5000,
        max_results: int = 5000,
    ) -> None:
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._results: queue.Queue[object] = queue.Queue(maxsize=max_results)
        self._handles: dict[int, tuple[Any, Any]] = {
            shard.shard_id: (shard.frames_fh, shard.idx_fh) for shard in shards
        }
        self._schema_version = schema_version
        self._dropped = 0
        self._results_dropped = 0
        self._processed = 0
        self._error: Exception | None = None
        self._error_count = 0
        self._thread = threading.Thread(
            target=self._run,
            name="frame-writer",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, job: _FrameWriteJob) -> bool:
        if self._error is not None:
            return False
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            self._dropped += 1
            return False
        return True

    def get_nowait(self) -> _FrameWriteResult | None:
        try:
            item = self._results.get_nowait()
        except queue.Empty:
            return None
        if item is _FRAME_WRITE_STOP:
            return None
        return item

    def close(self, timeout_seconds: float = 5.0) -> None:
        try:
            self._queue.put(_FRAME_WRITE_STOP, timeout=timeout_seconds)
        except queue.Full:
            return
        self._thread.join(timeout=timeout_seconds)

    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "results_size": self._results.qsize(),
            "dropped": self._dropped,
            "results_dropped": self._results_dropped,
            "processed": self._processed,
            "errors": self._error_count,
        }

    def error(self) -> Exception | None:
        return self._error

    def _record_error(self, exc: Exception) -> None:
        if self._error is None:
            self._error = exc
        self._error_count += 1

    def queue_size(self) -> int:
        return self._queue.qsize()

    def results_size(self) -> int:
        return self._results.qsize()

    def _run(self) -> None:
        try:
            while True:
                job = self._queue.get()
                if job is _FRAME_WRITE_STOP:
                    with contextlib.suppress(queue.Full):
                        self._results.put_nowait(_FRAME_WRITE_STOP)
                    break
                assert isinstance(job, _FrameWriteJob)
                handles = self._handles.get(job.shard_id)
                if handles is None:
                    continue
                frames_fh, idx_fh = handles
                write_start_ns = monotonic_ns()
                record = append_record(
                    frames_fh,
                    idx_fh,
                    job.payload_bytes,
                    job.rx_mono_ns,
                    job.rx_wall_ns_utc,
                    flags=job.flags,
                    schema_version=self._schema_version,
                )
                write_end_ns = monotonic_ns()
                backpressure_ns = max(0, write_start_ns - job.rx_mono_ns)
                ingest_latency_ns = write_end_ns - job.rx_mono_ns
                header_struct = frames_header_struct(record.schema_version)
                if record.schema_version == 1:
                    header_bytes = header_struct.pack(
                        frames_magic(record.schema_version),
                        record.schema_version,
                        record.flags,
                        record.rx_mono_ns,
                        record.payload_len,
                        record.payload_crc32,
                    )
                else:
                    header_bytes = header_struct.pack(
                        frames_magic(record.schema_version),
                        record.schema_version,
                        record.flags,
                        record.rx_mono_ns,
                        record.rx_wall_ns_utc,
                        record.payload_len,
                        record.payload_crc32,
                    )
                result = _FrameWriteResult(
                    shard_id=job.shard_id,
                    payload_bytes=record.payload,
                    header_bytes=header_bytes,
                    schema_version=record.schema_version,
                    payload_len=record.payload_len,
                    rx_mono_ns=record.rx_mono_ns,
                    write_duration_ns=write_end_ns - write_start_ns,
                    ingest_latency_ns=ingest_latency_ns,
                    backpressure_ns=backpressure_ns,
                )
                try:
                    self._results.put_nowait(result)
                    self._processed += 1
                except queue.Full:
                    self._results_dropped += 1
        except Exception as exc:
            self._record_error(exc)
            with contextlib.suppress(queue.Full):
                self._results.put_nowait(_FRAME_WRITE_STOP)
