import asyncio
import contextlib
import queue
import threading
from dataclasses import dataclass
from typing import Any, Iterable

import orjson

from .capture import monotonic_ns
from .capture_state import CaptureState, _append_sample_float

_PARSE_WORKER_STOP = object()


def _iter_items(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
    elif isinstance(payload, dict):
        yield payload


def _get_token_id(item: dict[str, Any]) -> str | None:
    token_id = item.get("asset_id")
    if token_id is None:
        token_id = item.get("token_id") or item.get("tokenId") or item.get("assetId")
    if token_id is None:
        return None
    return str(token_id)


def _get_msg_type(item: dict[str, Any]) -> str:
    msg_type = item.get("type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    msg_type = item.get("event_type")
    if isinstance(msg_type, str):
        return msg_type.lower()
    if "bids" in item or "asks" in item:
        return "book"
    if "price_changes" in item:
        return "price_change"
    return "unknown"


def _extract_minimal_fields(payload: Any) -> tuple[list[tuple[str, str]], dict[str, int]]:
    token_pairs: list[tuple[str, str]] = []
    msg_type_counts: dict[str, int] = {}
    for item in _iter_items(payload):
        msg_type = _get_msg_type(item)
        msg_type_counts[msg_type] = msg_type_counts.get(msg_type, 0) + 1
        token_id = _get_token_id(item)
        if token_id is not None:
            token_pairs.append((token_id, msg_type))
        price_changes = item.get("price_changes")
        if isinstance(price_changes, list):
            for change in price_changes:
                if not isinstance(change, dict):
                    continue
                change_token_id = _get_token_id(change)
                if change_token_id is None:
                    continue
                token_pairs.append((change_token_id, msg_type))
    return token_pairs, msg_type_counts


@dataclass(frozen=True)
class _ParseJob:
    shard_id: int
    payload_bytes: bytes
    rx_mono_ns: int


@dataclass(frozen=True)
class _ParseResult:
    shard_id: int
    rx_mono_ns: int
    t_parsed_mono_ns: int
    token_ids: list[str]
    msg_type_counts: dict[str, int]
    decode_error: bool


class _ParseWorker:
    def __init__(
        self,
        *,
        max_queue: int = 5000,
        max_results: int = 5000,
    ) -> None:
        self._queue: queue.Queue[object] = queue.Queue(maxsize=max_queue)
        self._results: queue.Queue[object] = queue.Queue(maxsize=max_results)
        self._dropped = 0
        self._results_dropped = 0
        self._processed = 0
        self._thread = threading.Thread(
            target=self._run,
            name="payload-parse",
            daemon=True,
        )
        self._thread.start()

    def enqueue(self, job: _ParseJob) -> bool:
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            self._dropped += 1
            return False
        return True

    def get_nowait(self) -> _ParseResult | None:
        try:
            item = self._results.get_nowait()
        except queue.Empty:
            return None
        if item is _PARSE_WORKER_STOP:
            return None
        return item

    def close(self, timeout_seconds: float = 5.0) -> None:
        with contextlib.suppress(queue.Full):
            self._queue.put_nowait(_PARSE_WORKER_STOP)
        self._thread.join(timeout=timeout_seconds)

    def stats(self) -> dict[str, int]:
        return {
            "queue_size": self._queue.qsize(),
            "results_size": self._results.qsize(),
            "dropped": self._dropped,
            "results_dropped": self._results_dropped,
            "processed": self._processed,
        }

    def queue_size(self) -> int:
        return self._queue.qsize()

    def results_size(self) -> int:
        return self._results.qsize()

    def _run(self) -> None:
        while True:
            job = self._queue.get()
            if job is _PARSE_WORKER_STOP:
                with contextlib.suppress(queue.Full):
                    self._results.put_nowait(_PARSE_WORKER_STOP)
                break
            assert isinstance(job, _ParseJob)
            try:
                payload = orjson.loads(job.payload_bytes)
                token_pairs, msg_type_counts = _extract_minimal_fields(payload)
                token_ids = [token_id for token_id, _ in token_pairs]
                decode_error = False
            except orjson.JSONDecodeError:
                token_ids = []
                msg_type_counts = {}
                decode_error = True
            t_parsed_mono_ns = monotonic_ns()
            result = _ParseResult(
                shard_id=job.shard_id,
                rx_mono_ns=job.rx_mono_ns,
                t_parsed_mono_ns=t_parsed_mono_ns,
                token_ids=token_ids,
                msg_type_counts=msg_type_counts,
                decode_error=decode_error,
            )
            try:
                self._results.put_nowait(result)
                self._processed += 1
            except queue.Full:
                self._results_dropped += 1


async def _parse_results_loop(state: CaptureState) -> None:
    worker = state.parse_worker
    if worker is None:
        return
    max_batch = 500
    sleep_seconds = 0.01
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        processed = 0
        while processed < max_batch:
            result = worker.get_nowait()
            if result is None:
                break
            processed += 1
            parsed_latency_ms = max(
                0.0, (result.t_parsed_mono_ns - result.rx_mono_ns) / 1_000_000.0
            )
            _append_sample_float(
                state.ws_rx_to_parsed_ms_samples,
                parsed_latency_ms,
                state.config.capture_metrics_max_samples,
            )
            shard = state.shards[result.shard_id]
            if result.decode_error:
                shard.stats.decode_errors += 1
                continue
            shard.stats.record_counts(result.token_ids, result.msg_type_counts)
            for token_id in result.token_ids:
                shard.last_seen[token_id] = result.rx_mono_ns
                if token_id in state.universe.token_added_mono_ns:
                    state.universe.token_added_mono_ns.pop(token_id, None)
        if processed == 0:
            await asyncio.sleep(sleep_seconds)
