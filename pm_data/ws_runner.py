import asyncio
import contextlib
import inspect
import shutil
import time
from typing import Any

import orjson
import websockets

from .capture import monotonic_ns
from .capture_format import (
    FLAG_BINARY_PAYLOAD,
    FLAG_TEXT_PAYLOAD,
    append_record,
    frames_header_len,
    frames_header_struct,
    frames_magic,
    idx_entry_len,
)
from .capture_state import CaptureState, ShardState, _append_sample, _append_sample_float
from .clob_ws import build_subscribe_payload
from .config import Config
from .fatal_policy import (
    FATAL_LOW_DISK,
    FATAL_RECONNECT_STORM,
    FATAL_SUBSCRIBE_CONFIRM,
    FATAL_WRITE_QUEUE,
    _build_missing_tokens_dump,
    _trigger_fatal,
    _write_runlog,
)
from .parse_pipeline import _ParseJob, _extract_minimal_fields
from .ws_primitives import (
    SUBSCRIBE_VARIANTS,
    ReconnectPolicy,
    is_confirm_payload,
    normalize_ws_keepalive,
    split_subscribe_groups,
)
from .writers_frames import _FrameWriteJob

CONNECT_SUPPORTS_CLOSE_TIMEOUT = (
    "close_timeout" in inspect.signature(websockets.connect).parameters
)
CONNECT_HEADERS_PARAM: str | None
if "extra_headers" in inspect.signature(websockets.connect).parameters:
    CONNECT_HEADERS_PARAM = "extra_headers"
elif "additional_headers" in inspect.signature(websockets.connect).parameters:
    CONNECT_HEADERS_PARAM = "additional_headers"
else:
    CONNECT_HEADERS_PARAM = None
DEFAULT_WS_CLOSE_TIMEOUT_SECONDS = 5.0


def _payload_bytes(raw: Any) -> tuple[bytes, int]:
    if isinstance(raw, (bytes, bytearray, memoryview)):
        return bytes(raw), FLAG_BINARY_PAYLOAD
    if isinstance(raw, str):
        return raw.encode("utf-8"), FLAG_TEXT_PAYLOAD
    return str(raw).encode("utf-8"), FLAG_TEXT_PAYLOAD



def _mark_reconnecting(shard: ShardState) -> None:
    shard.confirmed = False
    shard.confirm_events_seen = 0
    shard.confirm_deadline_mono_ns = None
    shard.last_subscribe_variant = None
    shard.last_subscribe_group_index = None



class _DataIdleTimeout(TimeoutError):
    pass



def _looks_like_ping_timeout(exc: Exception) -> bool:
    reason = getattr(exc, "reason", None)
    if reason:
        reason_text = str(reason).lower()
        if "ping" in reason_text and "timeout" in reason_text:
            return True
    message = str(exc).lower()
    return "ping" in message and "timeout" in message



def _close_was_clean(exc: Exception) -> bool | None:
    if isinstance(exc, websockets.exceptions.ConnectionClosedOK):
        return True
    if isinstance(exc, websockets.exceptions.ConnectionClosedError):
        return False
    return None



def _extract_close_details(exc: Exception) -> tuple[int | None, str | None, bool | None]:
    if isinstance(exc, websockets.exceptions.ConnectionClosed):
        return (
            getattr(exc, "code", None),
            getattr(exc, "reason", None),
            _close_was_clean(exc),
        )
    return None, None, None



def _classify_reconnect_trigger(exc: Exception) -> str:
    if isinstance(exc, _DataIdleTimeout):
        return "data_idle_timeout"
    if isinstance(exc, TimeoutError) and str(exc) == "subscribe confirmation timeout":
        return "confirm_timeout"
    if isinstance(exc, websockets.exceptions.ConnectionClosed):
        if _looks_like_ping_timeout(exc):
            return "ping_timeout"
        return "exception"
    return "exception"



def _confirm_failure_message(shard: ShardState, config: Config) -> str:
    variant = shard.last_subscribe_variant or "unknown"
    group_index = (
        "unknown"
        if shard.last_subscribe_group_index is None
        else str(shard.last_subscribe_group_index)
    )
    return (
        "confirmation deadline exceeded "
        f"shard_id={shard.shard_id} "
        f"failures={shard.confirm_failures} "
        f"timeout_seconds={config.capture_confirm_timeout_seconds} "
        f"variant={variant} "
        f"group_index={group_index}"
    )



async def _handle_confirm_deadline_miss(
    state: CaptureState,
    shard: ShardState,
    *,
    variant: str,
    now_ns: int,
) -> None:
    shard.confirm_failures += 1
    shard.subscribe_variant_index += 1
    if shard.subscribe_variant_locked == variant:
        shard.subscribe_variant_locked = None
    _write_runlog(
        state,
        {
            "record_type": "subscribe_confirm_fail",
            "run_id": state.run.run_id,
            "shard_id": shard.shard_id,
            "variant": variant,
            "confirm_events_seen": shard.confirm_events_seen,
        },
    )
    if shard.confirm_failures > state.config.capture_confirm_max_failures:
        message = _confirm_failure_message(shard, state.config)
        missing = _build_missing_tokens_dump(
            state,
            FATAL_SUBSCRIBE_CONFIRM,
            now_ns,
            include_missing_list=False,
            window_ns=None,
        )
        await _trigger_fatal(
            state,
            FATAL_SUBSCRIBE_CONFIRM,
            message,
            missing_tokens=missing,
        )
    raise TimeoutError("subscribe confirmation timeout")



def _handle_payload(
    state: CaptureState,
    shard: ShardState,
    raw: Any,
    *,
    rx_mono_ns: int,
    rx_wall_ns_utc: int,
) -> None:
    payload_bytes, flags = _payload_bytes(raw)
    confirm_event = is_confirm_payload(payload_bytes)
    if confirm_event:
        shard.confirm_events_seen += 1
        if not shard.confirmed and (
            shard.confirm_events_seen >= state.config.capture_confirm_min_events
        ):
            shard.confirmed = True
            if not shard.ready_once:
                ready_ns = monotonic_ns()
                shard.ready_once = True
                shard.ready_mono_ns = ready_ns
                shard.startup_connect_time_s = max(
                    0.0, (ready_ns - state.run.t0_mono_ns) / 1_000_000_000.0
                )
                state.startup_connect_time_s_samples.append(shard.startup_connect_time_s)
            if shard.last_subscribe_variant is not None:
                shard.subscribe_variant_locked = shard.last_subscribe_variant
            variant = shard.last_subscribe_variant
            if variant is None:
                variant = shard.subscribe_variant_locked
                if variant is None:
                    variant = SUBSCRIBE_VARIANTS[
                        shard.subscribe_variant_index % len(SUBSCRIBE_VARIANTS)
                    ]
                shard.last_subscribe_variant = variant
            runlog_record = {
                "record_type": "subscribe_confirm_success",
                "run_id": state.run.run_id,
                "shard_id": shard.shard_id,
                "confirm_events_seen": shard.confirm_events_seen,
                "confirm_deadline_mono_ns": shard.confirm_deadline_mono_ns,
                "variant": variant,
            }
            if shard.last_subscribe_group_index is not None:
                runlog_record["group_index"] = shard.last_subscribe_group_index
            _write_runlog(state, runlog_record)

    t_enq_mono_ns = monotonic_ns()
    enqueue_latency_ms = max(0.0, (t_enq_mono_ns - rx_mono_ns) / 1_000_000.0)
    _append_sample_float(
        state.ws_rx_to_enqueue_ms_samples,
        enqueue_latency_ms,
        state.config.capture_metrics_max_samples,
    )

    if state.frame_writer is not None:
        if not state.frame_writer.enqueue(
            _FrameWriteJob(
                shard_id=shard.shard_id,
                payload_bytes=payload_bytes,
                rx_mono_ns=rx_mono_ns,
                rx_wall_ns_utc=rx_wall_ns_utc,
                flags=flags,
            )
        ):
            state.loss_budget.frame_write_queue_pressure_events.bump()
            if not state.fatal_event.is_set():
                asyncio.get_running_loop().create_task(
                    _trigger_fatal(
                        state,
                        FATAL_WRITE_QUEUE,
                        "frame write queue full",
                    )
                )
            return
        _append_sample(
            state.ws_in_q_depth_samples,
            state.frame_writer.queue_size(),
            state.config.capture_metrics_max_samples,
        )
    else:
        write_start_ns = monotonic_ns()
        record = append_record(
            shard.frames_fh,
            shard.idx_fh,
            payload_bytes,
            rx_mono_ns,
            rx_wall_ns_utc,
            flags=flags,
            schema_version=state.config.capture_frames_schema_version,
        )
        write_end_ns = monotonic_ns()
        backpressure_ns = max(0, write_start_ns - rx_mono_ns)
        shard.stats.record(
            record.payload_len,
            frames_header_len(record.schema_version),
            idx_entry_len(record.schema_version),
            write_end_ns - write_start_ns,
            write_end_ns - rx_mono_ns,
            backpressure_ns,
            [],
            {},
            state.config.capture_metrics_max_samples,
        )
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
        shard.ring.append((header_bytes, record.payload))

    if payload_bytes in (b"PONG", b"PING"):
        return

    if state.parse_worker is None:
        try:
            payload = orjson.loads(payload_bytes)
        except orjson.JSONDecodeError:
            shard.stats.decode_errors += 1
            return
        token_pairs, msg_type_counts = _extract_minimal_fields(payload)
        token_ids = [token_id for token_id, _ in token_pairs]
        shard.stats.record_counts(token_ids, msg_type_counts)
        for token_id in token_ids:
            shard.last_seen[token_id] = rx_mono_ns
            if token_id in state.universe.token_added_mono_ns:
                state.universe.token_added_mono_ns.pop(token_id, None)
        return

    if not state.parse_worker.enqueue(
        _ParseJob(
            shard_id=shard.shard_id,
            payload_bytes=payload_bytes,
            rx_mono_ns=rx_mono_ns,
        )
    ):
        state.loss_budget.parse_queue_drops.bump()
    else:
        _append_sample(
            state.parse_q_depth_samples,
            state.parse_worker.queue_size(),
            state.config.capture_metrics_max_samples,
        )



def _refresh_requested(shard: ShardState) -> bool:
    return shard.target is not None and shard.target.refresh_requested.is_set()



def _apply_shard_refresh(state: CaptureState, shard: ShardState) -> bool:
    target = shard.target
    if target is None or not target.refresh_requested.is_set():
        return False
    shard.token_ids = list(target.token_ids)
    shard.groups = list(target.groups)
    shard.confirm_token_ids = list(target.confirm_token_ids)
    _mark_reconnecting(shard)
    target.refresh_requested.clear()
    _write_runlog(
        state,
        {
            "record_type": "shard_refresh_applied",
            "run_id": state.run.run_id,
            "shard_id": shard.shard_id,
            "to_version": target.target_version,
            "confirm_reset": True,
            "confirm_events_seen": shard.confirm_events_seen,
        },
    )
    return True



async def _wait_for_refresh_or_fatal(state: CaptureState, shard: ShardState) -> None:
    if shard.target is None:
        await asyncio.wait(
            [
                asyncio.create_task(state.fatal_event.wait()),
                asyncio.create_task(state.stop_event.wait()),
            ],
            return_when=asyncio.FIRST_COMPLETED,
        )
        return
    refresh_task = asyncio.create_task(shard.target.refresh_requested.wait())
    fatal_task = asyncio.create_task(state.fatal_event.wait())
    stop_task = asyncio.create_task(state.stop_event.wait())
    done, pending = await asyncio.wait(
        [refresh_task, fatal_task, stop_task],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task



async def _run_shard(state: CaptureState, shard: ShardState) -> None:
    yield_interval_ns = 5_000_000
    reconnect_policy = ReconnectPolicy(
        max_reconnects=state.config.ws_reconnect_max,
        backoff_seconds=state.config.ws_reconnect_backoff_seconds,
    )
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        if not shard.token_ids:
            await _wait_for_refresh_or_fatal(state, shard)
            if state.fatal_event.is_set() or state.stop_event.is_set():
                break
            _apply_shard_refresh(state, shard)
            continue
        if _refresh_requested(shard):
            _apply_shard_refresh(state, shard)
            continue
        if shard.subscribe_variant_locked:
            variant = shard.subscribe_variant_locked
        else:
            variant = SUBSCRIBE_VARIANTS[
                shard.subscribe_variant_index % len(SUBSCRIBE_VARIANTS)
            ]
        refresh_applied = False
        ping_interval, ping_timeout, data_idle_reconnect_seconds = normalize_ws_keepalive(
            state.config
        )
        connect_kwargs: dict[str, Any] = {
            "ping_interval": ping_interval,
            "ping_timeout": ping_timeout,
        }
        if CONNECT_SUPPORTS_CLOSE_TIMEOUT:
            connect_kwargs["close_timeout"] = DEFAULT_WS_CLOSE_TIMEOUT_SECONDS
        if state.config.ws_user_agent and CONNECT_HEADERS_PARAM is not None:
            connect_kwargs[CONNECT_HEADERS_PARAM] = [
                ("User-Agent", state.config.ws_user_agent)
            ]
        try:
            async with websockets.connect(
                state.config.clob_ws_url,
                **connect_kwargs,
            ) as ws:
                connected_ns = monotonic_ns()
                if shard.disconnected_since_mono_ns is not None:
                    duration_s = (
                        connected_ns - shard.disconnected_since_mono_ns
                    ) / 1_000_000_000.0
                    if shard.ready_once:
                        shard.midrun_disconnected_time_s_total += duration_s
                        shard.midrun_disconnected_incidents += 1
                        state.midrun_disconnected_time_s_total += duration_s
                        state.midrun_disconnected_incidents += 1
                        state.midrun_disconnected_durations_s.append(duration_s)
                    shard.disconnected_since_mono_ns = None
                shard.connected = True
                _mark_reconnecting(shard)
                _write_runlog(
                    state,
                    {
                        "record_type": "ws_connect",
                        "run_id": state.run.run_id,
                        "shard_id": shard.shard_id,
                        "ws_url": state.config.clob_ws_url,
                        "user_agent_header": state.config.ws_user_agent,
                        "ping_interval_seconds": ping_interval,
                        "ping_timeout_seconds": ping_timeout,
                        "data_idle_reconnect_seconds": data_idle_reconnect_seconds,
                        "ts_mono_ns": connected_ns,
                        "ts_wall_ns_utc": time.time_ns(),
                    },
                )
                shard.last_subscribe_variant = variant
                shard.last_subscribe_group_index = None
                shard.confirm_deadline_mono_ns = (
                    monotonic_ns()
                    + int(state.config.capture_confirm_timeout_seconds * 1_000_000_000)
                )

                async def _check_confirm_deadline(now_ns: int) -> None:
                    if shard.confirmed or shard.confirm_deadline_mono_ns is None:
                        return
                    if now_ns < shard.confirm_deadline_mono_ns:
                        return
                    await _handle_confirm_deadline_miss(
                        state,
                        shard,
                        variant=variant,
                        now_ns=now_ns,
                    )

                for group_index, token_group in enumerate(shard.groups):
                    payload = build_subscribe_payload(variant, token_group)
                    payload_bytes = orjson.dumps(payload)
                    shard.last_subscribe_variant = variant
                    shard.last_subscribe_group_index = group_index
                    await ws.send(payload_bytes.decode("utf-8"))
                    _write_runlog(
                        state,
                        {
                            "record_type": "subscribe_attempt",
                            "run_id": state.run.run_id,
                            "shard_id": shard.shard_id,
                            "variant": variant,
                            "group_index": group_index,
                            "token_count": len(token_group),
                            "payload_bytes": len(payload_bytes),
                        },
                    )

                data_idle_reconnect_ns = None
                if data_idle_reconnect_seconds is not None:
                    data_idle_reconnect_ns = int(
                        data_idle_reconnect_seconds * 1_000_000_000
                    )
                last_rx_mono_ns = monotonic_ns()

                while not state.fatal_event.is_set() and not state.stop_event.is_set():
                    if _refresh_requested(shard):
                        refresh_applied = _apply_shard_refresh(state, shard)
                        await ws.close()
                        break
                    if state.stop_event.is_set():
                        await ws.close()
                        break
                    now_ns = monotonic_ns()
                    await _check_confirm_deadline(now_ns)
                    recv_timeout = 1.0
                    if data_idle_reconnect_ns is not None:
                        idle_elapsed_ns = now_ns - last_rx_mono_ns
                        remaining_idle_ns = data_idle_reconnect_ns - idle_elapsed_ns
                        if remaining_idle_ns <= 0:
                            raise _DataIdleTimeout("data idle timeout")
                        recv_timeout = min(
                            recv_timeout,
                            remaining_idle_ns / 1_000_000_000,
                        )
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=recv_timeout)
                    except asyncio.TimeoutError:
                        now_ns = monotonic_ns()
                        await _check_confirm_deadline(now_ns)
                        if data_idle_reconnect_ns is not None:
                            if now_ns - last_rx_mono_ns >= data_idle_reconnect_ns:
                                raise _DataIdleTimeout("data idle timeout")
                        continue

                    rx_mono_ns = monotonic_ns()
                    last_rx_mono_ns = rx_mono_ns
                    shard.last_rx_mono_ns = rx_mono_ns
                    rx_wall_ns_utc = time.time_ns()
                    await _check_confirm_deadline(monotonic_ns())
                    _handle_payload(
                        state,
                        shard,
                        raw,
                        rx_mono_ns=rx_mono_ns,
                        rx_wall_ns_utc=rx_wall_ns_utc,
                    )
                    if rx_mono_ns - shard.last_yield_mono_ns >= yield_interval_ns:
                        shard.last_yield_mono_ns = rx_mono_ns
                        await asyncio.sleep(0)
            if refresh_applied:
                continue
        except Exception as exc:
            if state.stop_event.is_set():
                break
            now_ns = monotonic_ns()
            if shard.connected:
                shard.connected = False
                shard.disconnected_since_mono_ns = now_ns
            _mark_reconnecting(shard)
            shard.reconnects += 1
            trigger = _classify_reconnect_trigger(exc)
            close_code, close_reason, close_was_clean = _extract_close_details(exc)
            _write_runlog(
                state,
                {
                    "record_type": "reconnect",
                    "run_id": state.run.run_id,
                    "shard_id": shard.shard_id,
                    "reason": type(exc).__name__,
                    "trigger": trigger,
                    "close_code": close_code,
                    "close_reason": close_reason,
                    "close_was_clean": close_was_clean,
                    "ping_interval_seconds": ping_interval,
                    "ping_timeout_seconds": ping_timeout,
                    "data_idle_reconnect_seconds": data_idle_reconnect_seconds,
                    "reconnects": shard.reconnects,
                    "ts_mono_ns": now_ns,
                    "ts_wall_ns_utc": time.time_ns(),
                },
            )
            if not reconnect_policy.can_reconnect(shard.reconnects):
                await _trigger_fatal(
                    state,
                    FATAL_RECONNECT_STORM,
                    "reconnect limit exceeded",
                )
                break
            await asyncio.sleep(reconnect_policy.backoff())


