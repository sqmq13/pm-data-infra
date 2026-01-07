import asyncio
import json
import time
from collections import deque

import pytest

from pm_arb.capture import RunBootstrap
from pm_arb.capture import monotonic_ns
from pm_arb.capture_format import FrameRecord
from pm_arb.capture_online import (
    CaptureState,
    KEEPALIVE_PAYLOAD,
    ShardState,
    UniverseState,
    _check_backpressure_fatal,
    _confirm_event_from_payload,
    _handle_confirm_deadline_miss,
    _handle_payload,
    _heartbeat_loop,
    _mark_reconnecting,
)
from pm_arb.config import Config


@pytest.mark.asyncio
async def test_silent_universe_confirm_no_warmup_fatal(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(capture_heartbeat_interval_seconds=0.01)
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    frames_path.parent.mkdir()
    shard = ShardState(
        shard_id=0,
        token_ids=["t1", "t2"],
        groups=[],
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=5),
        confirmed=True,
    )
    universe = UniverseState(
        universe_version=1,
        current_token_ids={"t1", "t2"},
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets={},
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=["t1", "t2"],
        universe=universe,
    )
    task = asyncio.create_task(_heartbeat_loop(state))
    await asyncio.sleep(0.03)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not state.fatal_event.is_set()


def test_confirm_event_ignores_keepalive_then_confirms():
    keepalive_confirm, keepalive_payload = _confirm_event_from_payload(b"PONG")
    assert keepalive_confirm is False
    assert keepalive_payload is KEEPALIVE_PAYLOAD
    confirm, payload = _confirm_event_from_payload(b'{"event_type":"book"}')
    assert confirm is True
    assert isinstance(payload, dict)


@pytest.mark.asyncio
async def test_backpressure_fatal_emits_missing_tokens(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        capture_backpressure_fatal_ms=0.1,
        capture_metrics_max_samples=5,
    )
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    frames_path.parent.mkdir()
    shard = ShardState(
        shard_id=0,
        token_ids=["t1", "t2"],
        groups=[],
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=5),
    )
    universe = UniverseState(
        universe_version=1,
        current_token_ids={"t1", "t2"},
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets={},
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=["t1", "t2"],
        universe=universe,
    )

    def slow_append_record(*args, **kwargs):
        time.sleep(0.001)
        rx_mono_ns = args[3] if len(args) > 3 else kwargs.get("rx_mono_ns", 0)
        rx_wall_ns_utc = args[4] if len(args) > 4 else kwargs.get("rx_wall_ns_utc", 0)
        return FrameRecord(
            offset=0,
            schema_version=1,
            flags=0,
            rx_mono_ns=rx_mono_ns,
            rx_wall_ns_utc=rx_wall_ns_utc,
            payload_len=len(args[2]),
            payload_crc32=0,
            payload=args[2],
        )

    monkeypatch.setattr("pm_arb.capture_online.append_record", slow_append_record)

    for _ in range(5):
        rx_mono_ns = monotonic_ns() - 1_000_000_000
        _handle_payload(
            state,
            shard,
            b'{"event_type":"book","asset_id":"t1"}',
            rx_mono_ns=rx_mono_ns,
            rx_wall_ns_utc=rx_mono_ns + 1,
        )

    backpressure_p99 = max(shard.stats.backpressure_ns)
    for _ in range(3):
        await _check_backpressure_fatal(state, monotonic_ns(), backpressure_p99)

    missing_path = run_dir / "missing_tokens.json"
    assert state.fatal_event.is_set()
    assert missing_path.exists()


@pytest.mark.asyncio
async def test_heartbeat_does_not_fatal_on_expired_confirm_deadline(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(capture_heartbeat_interval_seconds=0.01)
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=["t1"],
        groups=[],
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=5),
        confirmed=False,
    )
    shard.confirm_deadline_mono_ns = monotonic_ns() - 1_000_000
    universe = UniverseState(
        universe_version=1,
        current_token_ids={"t1"},
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets={},
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=["t1"],
        universe=universe,
    )

    def fake_write_metrics(path, record):
        state.fatal_event.set()

    def fail_trigger(*args, **kwargs):
        raise AssertionError("unexpected fatal trigger")

    monkeypatch.setattr("pm_arb.capture_online._write_metrics", fake_write_metrics)
    monkeypatch.setattr("pm_arb.capture_online._trigger_fatal", fail_trigger)

    await _heartbeat_loop(state)

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_confirm_failures_fatal_after_threshold(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        capture_confirm_max_failures=2,
        capture_confirm_timeout_seconds=5.0,
    )
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=["t1"],
        groups=[],
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=5),
        confirmed=False,
    )
    shard.confirm_deadline_mono_ns = monotonic_ns() - 1
    shard.last_subscribe_variant = "A"
    shard.last_subscribe_group_index = 0
    universe = UniverseState(
        universe_version=1,
        current_token_ids={"t1"},
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets={},
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=["t1"],
        universe=universe,
    )

    for _ in range(config.capture_confirm_max_failures):
        with pytest.raises(TimeoutError):
            await _handle_confirm_deadline_miss(
                state,
                shard,
                variant="A",
                now_ns=monotonic_ns(),
            )
        assert not state.fatal_event.is_set()

    with pytest.raises(TimeoutError):
        await _handle_confirm_deadline_miss(
            state,
            shard,
            variant="A",
            now_ns=monotonic_ns(),
        )

    fatal_path = run_dir / "fatal.json"
    assert state.fatal_event.is_set()
    assert fatal_path.exists()
    fatal = json.loads(fatal_path.read_text(encoding="utf-8"))
    assert fatal["fatal_reason"] == "SUBSCRIBE_CONFIRM_FAIL"
    assert "shard_id=0" in fatal["fatal_message"]
    assert "variant=A" in fatal["fatal_message"]
    assert "group_index=0" in fatal["fatal_message"]

    shard.frames_fh.close()
    shard.idx_fh.close()


def test_reconnect_clears_confirm_deadline(tmp_path):
    frames_path = tmp_path / "shard_00.frames"
    idx_path = tmp_path / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=["t1"],
        groups=[],
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=1),
        confirmed=True,
        confirm_events_seen=5,
    )
    shard.confirm_deadline_mono_ns = monotonic_ns() - 1
    shard.last_subscribe_variant = "B"
    shard.last_subscribe_group_index = 2

    _mark_reconnecting(shard)

    assert shard.confirmed is False
    assert shard.confirm_events_seen == 0
    assert shard.confirm_deadline_mono_ns is None
    assert shard.last_subscribe_variant is None
    assert shard.last_subscribe_group_index is None

    shard.frames_fh.close()
    shard.idx_fh.close()
