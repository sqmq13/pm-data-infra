import contextlib
import os
import queue
import threading
import time
from pathlib import Path
from typing import Any

import orjson

_NDJSON_WRITER_STOP = object()
_ORJSON_NDJSON_OPTIONS = orjson.OPT_APPEND_NEWLINE
if hasattr(orjson, "OPT_ESCAPE_NON_ASCII"):
    _ORJSON_NDJSON_OPTIONS |= orjson.OPT_ESCAPE_NON_ASCII


class _NdjsonWriter:
    def __init__(
        self,
        *,
        max_queue: int = 10000,
        flush_interval_seconds: float = 0.5,
        batch_size: int = 200,
        thread_name: str = "ndjson-writer",
        fsync_on_close: bool = False,
        fsync_interval_seconds: float | None = None,
    ) -> None:
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._flush_interval_seconds = flush_interval_seconds
        self._batch_size = batch_size
        self._fsync_on_close = fsync_on_close
        self._fsync_interval_seconds = (
            None
            if fsync_interval_seconds is None or fsync_interval_seconds <= 0
            else fsync_interval_seconds
        )
        self._dropped = 0
        self._enqueue_timeouts = 0
        self._error: Exception | None = None
        self._error_count = 0
        self._thread = threading.Thread(
            target=self._run,
            name=thread_name,
            daemon=True,
        )
        self._thread.start()

    def enqueue_nowait(self, path: Path, record: dict[str, Any]) -> bool:
        if self._error is not None:
            return False
        try:
            self._queue.put_nowait((str(path), record))
        except queue.Full:
            self._dropped += 1
            return False
        return True

    def enqueue_blocking(
        self,
        path: Path,
        record: dict[str, Any],
        *,
        timeout_seconds: float,
    ) -> bool:
        if self._error is not None:
            return False
        timeout = max(0.0, timeout_seconds)
        try:
            self._queue.put((str(path), record), timeout=timeout)
        except queue.Full:
            self._enqueue_timeouts += 1
            return False
        return True

    def close(self, timeout_seconds: float = 5.0) -> None:
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_NDJSON_WRITER_STOP)
        self._thread.join(timeout=timeout_seconds)

    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "dropped": self._dropped,
            "enqueue_timeouts": self._enqueue_timeouts,
            "errors": self._error_count,
        }

    def error(self) -> Exception | None:
        return self._error

    def _record_error(self, exc: Exception) -> None:
        if self._error is None:
            self._error = exc
        self._error_count += 1

    def _run(self) -> None:
        files: dict[str, Any] = {}
        pending: dict[str, list[bytes]] = {}
        pending_count = 0
        last_flush = time.monotonic()
        last_fsync = last_flush
        try:
            while True:
                item = None
                try:
                    item = self._queue.get(timeout=self._flush_interval_seconds)
                except queue.Empty:
                    item = None
                if item is _NDJSON_WRITER_STOP:
                    self._flush_pending(pending, files)
                    break
                if item is not None:
                    path_text, record = item
                    line = orjson.dumps(record, option=_ORJSON_NDJSON_OPTIONS)
                    pending.setdefault(path_text, []).append(line)
                    pending_count += 1
                now = time.monotonic()
                if (
                    pending_count >= self._batch_size
                    or now - last_flush >= self._flush_interval_seconds
                ):
                    if pending_count:
                        self._flush_pending(pending, files)
                        pending_count = 0
                    if (
                        self._fsync_interval_seconds is not None
                        and now - last_fsync >= self._fsync_interval_seconds
                    ):
                        self._fsync_files(files)
                        last_fsync = now
                    last_flush = now
        except Exception as exc:
            self._record_error(exc)
        finally:
            with contextlib.suppress(Exception):
                self._flush_pending(pending, files)
            if self._fsync_on_close:
                with contextlib.suppress(Exception):
                    self._fsync_files(files)
            for handle in files.values():
                with contextlib.suppress(Exception):
                    handle.flush()
                    handle.close()

    def _fsync_files(self, files: dict[str, Any]) -> None:
        for handle in files.values():
            try:
                handle.flush()
                os.fsync(handle.fileno())
            except Exception as exc:
                self._record_error(exc)

    def _flush_pending(self, pending: dict[str, list[bytes]], files: dict[str, Any]) -> None:
        for path_text, lines in list(pending.items()):
            if not lines:
                continue
            handle = files.get(path_text)
            if handle is None:
                path = Path(path_text)
                path.parent.mkdir(parents=True, exist_ok=True)
                handle = path.open("ab")
                files[path_text] = handle
            try:
                handle.write(b"".join(lines))
                handle.flush()
            except Exception as exc:
                self._record_error(exc)
        pending.clear()
