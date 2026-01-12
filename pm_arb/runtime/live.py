from __future__ import annotations

import asyncio
import hashlib
import inspect
import time
from dataclasses import dataclass, field
from typing import Any, AsyncIterator

import orjson
import websockets

from pm_arb.clob_ws import build_subscribe_payload
from pm_arb.config import Config
from pm_arb.gamma import fetch_markets, parse_clob_token_ids, select_active_binary_markets

from .replay import RawFrame

SUBSCRIBE_VARIANTS = ("A", "B", "C")


def _monotonic_ns() -> int:
    return time.perf_counter_ns()


def _normalize_ws_keepalive(config: Config) -> tuple[float | None, float | None, float | None]:
    ping_interval = config.ws_ping_interval_seconds
    if ping_interval <= 0:
        ping_interval = None
    ping_timeout = config.ws_ping_timeout_seconds
    if ping_timeout <= 0:
        ping_timeout = None
    data_idle_reconnect = config.ws_data_idle_reconnect_seconds
    if data_idle_reconnect <= 0:
        data_idle_reconnect = None
    return ping_interval, ping_timeout, data_idle_reconnect


def _stable_hash(token_id: str) -> int:
    digest = hashlib.sha256(token_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


def _assign_shards_by_token(token_ids: list[str], shard_count: int) -> dict[int, list[str]]:
    if shard_count <= 0:
        raise ValueError("ws_shards must be >= 1")
    shards: dict[int, list[str]] = {idx: [] for idx in range(shard_count)}
    seen: set[str] = set()
    for token_id in token_ids:
        token_key = str(token_id)
        if token_key in seen:
            continue
        seen.add(token_key)
        shard_id = _stable_hash(token_key) % shard_count
        shards[shard_id].append(token_key)
    return shards


def _split_subscribe_groups(
    token_ids: list[str],
    max_tokens: int,
    max_bytes: int,
    variant: str,
) -> list[list[str]]:
    groups: list[list[str]] = []
    current: list[str] = []
    for token_id in token_ids:
        candidate = current + [token_id]
        payload = build_subscribe_payload(variant, candidate)
        payload_bytes = orjson.dumps(payload)
        if len(candidate) > max_tokens or len(payload_bytes) > max_bytes:
            if not current:
                raise ValueError("single subscribe payload exceeds limits")
            groups.append(current)
            current = [token_id]
        else:
            current = candidate
    if current:
        groups.append(current)
    return groups


def _is_confirm_payload(payload_bytes: bytes) -> bool:
    text = payload_bytes.strip()
    if not text:
        return False
    if text.upper() in {b"PONG", b"PING"}:
        return False
    if text == b"[]":
        return False
    return True


class _ConfirmTimeout(TimeoutError):
    pass


@dataclass(slots=True)
class LiveStats:
    reconnects: int = 0
    frames: int = 0
    dropped: int = 0
    confirm_timeouts: int = 0


@dataclass(slots=True)
class _ShardConfig:
    shard_id: int
    token_ids: list[str]
    variant_index: int = 0


class LiveDataSource:
    def __init__(
        self,
        *,
        config: Config,
        duration_seconds: float,
        max_queue: int = 1000,
    ) -> None:
        self._config = config
        self._duration_seconds = duration_seconds
        self._max_queue = max_queue
        self._stats = LiveStats()

    @property
    def stats(self) -> LiveStats:
        return self._stats

    def _load_tokens(self) -> list[str]:
        markets = fetch_markets(
            self._config.gamma_base_url,
            self._config.rest_timeout,
            limit=self._config.gamma_limit,
            max_markets=self._config.capture_max_markets,
        )
        selected = select_active_binary_markets(
            markets, max_markets=self._config.capture_max_markets
        )
        tokens: set[str] = set()
        for market in selected:
            token_ids = parse_clob_token_ids(
                market.get("clobTokenIds") or market.get("clob_token_ids")
            )
            if len(token_ids) != 2:
                continue
            for token_id in token_ids:
                tokens.add(str(token_id))
        return sorted(tokens)

    async def stream(self) -> AsyncIterator[RawFrame]:
        tokens = self._load_tokens()
        if not tokens:
            raise ValueError("no tokens selected for live subscription")
        shard_map = _assign_shards_by_token(tokens, self._config.ws_shards)
        shards = [
            _ShardConfig(shard_id=shard_id, token_ids=token_ids)
            for shard_id, token_ids in shard_map.items()
        ]

        queue: asyncio.Queue[RawFrame] = asyncio.Queue(maxsize=self._max_queue)
        stop_event = asyncio.Event()
        deadline_ns = _monotonic_ns() + int(self._duration_seconds * 1_000_000_000)
        tasks = [
            asyncio.create_task(self._run_shard(shard, queue, stop_event, deadline_ns))
            for shard in shards
        ]

        try:
            while True:
                now_ns = _monotonic_ns()
                remaining_ns = deadline_ns - now_ns
                if remaining_ns <= 0:
                    break
                try:
                    frame = await asyncio.wait_for(queue.get(), remaining_ns / 1_000_000_000)
                except asyncio.TimeoutError:
                    break
                yield frame
        finally:
            stop_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _run_shard(
        self,
        shard: _ShardConfig,
        queue: asyncio.Queue[RawFrame],
        stop_event: asyncio.Event,
        deadline_ns: int,
    ) -> None:
        ping_interval, ping_timeout, data_idle_reconnect = _normalize_ws_keepalive(
            self._config
        )
        connect_kwargs: dict[str, Any] = {
            "ping_interval": ping_interval,
            "ping_timeout": ping_timeout,
        }
        if "close_timeout" in inspect.signature(websockets.connect).parameters:
            connect_kwargs["close_timeout"] = 5.0

        while not stop_event.is_set():
            if _monotonic_ns() >= deadline_ns:
                break
            variant = SUBSCRIBE_VARIANTS[shard.variant_index % len(SUBSCRIBE_VARIANTS)]
            shard.variant_index += 1
            try:
                async with websockets.connect(
                    self._config.clob_ws_url,
                    **connect_kwargs,
                ) as ws:
                    groups = _split_subscribe_groups(
                        shard.token_ids,
                        max_tokens=self._config.ws_subscribe_max_tokens,
                        max_bytes=self._config.ws_subscribe_max_bytes,
                        variant=variant,
                    )
                    for token_group in groups:
                        payload = build_subscribe_payload(variant, token_group)
                        payload_bytes = orjson.dumps(payload)
                        await ws.send(payload_bytes.decode("utf-8"))

                    confirm_deadline_ns = _monotonic_ns() + int(
                        self._config.capture_confirm_timeout_seconds * 1_000_000_000
                    )
                    confirm_events_seen = 0
                    data_idle_reconnect_ns = None
                    if data_idle_reconnect is not None:
                        data_idle_reconnect_ns = int(data_idle_reconnect * 1_000_000_000)
                    last_rx_mono_ns = _monotonic_ns()

                    while not stop_event.is_set():
                        now_ns = _monotonic_ns()
                        if now_ns >= deadline_ns:
                            return
                        recv_timeout = 1.0
                        if (
                            confirm_events_seen < self._config.capture_confirm_min_events
                            and now_ns >= confirm_deadline_ns
                        ):
                            self._stats.confirm_timeouts += 1
                            raise _ConfirmTimeout("subscribe confirmation timeout")
                        if data_idle_reconnect_ns is not None:
                            idle_elapsed_ns = now_ns - last_rx_mono_ns
                            remaining_idle_ns = data_idle_reconnect_ns - idle_elapsed_ns
                            if remaining_idle_ns <= 0:
                                raise TimeoutError("data idle timeout")
                            recv_timeout = min(
                                recv_timeout, remaining_idle_ns / 1_000_000_000
                            )
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                        except asyncio.TimeoutError:
                            continue

                        payload_bytes: bytes
                        if isinstance(raw, (bytes, bytearray, memoryview)):
                            payload_bytes = bytes(raw)
                        elif isinstance(raw, str):
                            payload_bytes = raw.encode("utf-8")
                        else:
                            payload_bytes = str(raw).encode("utf-8")

                        rx_mono_ns = _monotonic_ns()
                        last_rx_mono_ns = rx_mono_ns
                        if _is_confirm_payload(payload_bytes):
                            confirm_events_seen += 1
                        rx_wall_ns_utc = time.time_ns()
                        frame = RawFrame(
                            payload=payload_bytes,
                            rx_mono_ns=rx_mono_ns,
                            rx_wall_ns_utc=rx_wall_ns_utc,
                            shard_id=shard.shard_id,
                            idx_i=None,
                        )
                        try:
                            queue.put_nowait(frame)
                            self._stats.frames += 1
                        except asyncio.QueueFull:
                            self._stats.dropped += 1
            except asyncio.CancelledError:
                raise
            except Exception:
                if stop_event.is_set():
                    return
                self._stats.reconnects += 1
                await asyncio.sleep(self._config.ws_reconnect_backoff_seconds)
