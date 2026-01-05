import asyncio
import time
from collections import deque

import pytest

from pm_arb.capture import RunBootstrap
from pm_arb.capture import monotonic_ns
from pm_arb.capture_format import FrameRecord
from pm_arb.capture_online import (
    CaptureState,
    ShardState,
    UniverseState,
    _check_backpressure_fatal,
    _confirm_event_from_payload,
    _handle_payload,
    _heartbeat_loop,
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
    assert keepalive_payload == []
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
