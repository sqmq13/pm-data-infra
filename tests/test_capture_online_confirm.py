import asyncio
import json
import time
from collections import deque
from typing import Any

import pytest

from pm_arb.capture import RunBootstrap
from pm_arb.capture import monotonic_ns
from pm_arb.capture_format import FrameRecord
import pm_arb.capture_online as capture_online
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


class FakeWebSocket:
    def __init__(self, recv_events, *, on_send=None, on_recv=None):
        self._recv_events = deque(recv_events)
        self._on_send = on_send
        self._on_recv = on_recv
        self.sent = []
        self.closed = False

    async def send(self, data):
        self.sent.append(data)
        if self._on_send:
            self._on_send()

    async def recv(self):
        if not self._recv_events:
            raise asyncio.TimeoutError
        event = self._recv_events.popleft()
        if isinstance(event, Exception):
            raise event
        if self._on_recv:
            self._on_recv()
        return event

    async def close(self):
        self.closed = True


class FakeConnect:
    def __init__(self, ws):
        self._ws = ws

    async def __aenter__(self):
        return self._ws

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _build_state(tmp_path, *, groups, config):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    token_ids = [token for group in groups for token in group]
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=token_ids,
        groups=groups,
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=5),
    )
    universe = UniverseState(
        universe_version=1,
        current_token_ids=set(token_ids),
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets={},
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=token_ids,
        universe=universe,
    )
    return state, shard, run_dir


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
async def test_confirm_success_logs_variant_from_subscribe_attempt(tmp_path, monkeypatch):
    config = Config(capture_confirm_min_events=1, ws_reconnect_backoff_seconds=0.0)
    groups = [["t1"], ["t2"]]
    state, shard, run_dir = _build_state(tmp_path, groups=groups, config=config)
    ws = FakeWebSocket(
        [b'{"event_type":"book","asset_id":"t1"}'],
        on_recv=state.stop_event.set,
    )
    monkeypatch.setattr(
        capture_online.websockets,
        "connect",
        lambda *args, **kwargs: FakeConnect(ws),
    )

    await asyncio.wait_for(capture_online._run_shard(state, shard), timeout=2.0)

    records = [
        json.loads(line)
        for line in (run_dir / "runlog.ndjson")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    attempt_variants = [
        record["variant"]
        for record in records
        if record.get("record_type") == "subscribe_attempt"
    ]
    confirm_records = [
        record
        for record in records
        if record.get("record_type") == "subscribe_confirm_success"
    ]
    assert attempt_variants
    assert confirm_records
    assert confirm_records[-1]["variant"] == attempt_variants[-1]
    assert confirm_records[-1]["group_index"] == len(groups) - 1
    assert shard.subscribe_variant_locked == attempt_variants[-1]

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_connect_kwargs_include_keepalive(tmp_path, monkeypatch):
    config = Config(
        capture_confirm_min_events=1,
        ws_reconnect_backoff_seconds=0.0,
        ws_ping_interval_seconds=12.0,
        ws_ping_timeout_seconds=7.0,
    )
    groups = [["t1"]]
    state, shard, _run_dir = _build_state(tmp_path, groups=groups, config=config)
    ws = FakeWebSocket(
        [b'{"event_type":"book","asset_id":"t1"}'],
        on_recv=state.stop_event.set,
    )
    seen: dict[str, Any] = {}

    def fake_connect(*args, **kwargs):
        seen.update(kwargs)
        return FakeConnect(ws)

    monkeypatch.setattr(capture_online.websockets, "connect", fake_connect)

    await asyncio.wait_for(capture_online._run_shard(state, shard), timeout=2.0)

    assert "ping_interval" in seen
    assert "ping_timeout" in seen
    assert seen["ping_interval"] == config.ws_ping_interval_seconds
    assert seen["ping_timeout"] == config.ws_ping_timeout_seconds

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_variant_sticky_after_confirm_reconnect(tmp_path, monkeypatch):
    config = Config(capture_confirm_min_events=1, ws_reconnect_backoff_seconds=0.0)
    groups = [["t1"]]
    state, shard, run_dir = _build_state(tmp_path, groups=groups, config=config)
    first_ws = FakeWebSocket(
        [b'{"event_type":"book","asset_id":"t1"}', RuntimeError("closed")],
    )
    second_ws = FakeWebSocket([asyncio.TimeoutError()], on_send=state.stop_event.set)
    connections = deque([first_ws, second_ws])

    def fake_connect(*args, **kwargs):
        if not connections:
            raise AssertionError("unexpected websocket connect")
        return FakeConnect(connections.popleft())

    monkeypatch.setattr(capture_online.websockets, "connect", fake_connect)

    await asyncio.wait_for(capture_online._run_shard(state, shard), timeout=2.0)

    records = [
        json.loads(line)
        for line in (run_dir / "runlog.ndjson")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    attempt_records = [
        record for record in records if record.get("record_type") == "subscribe_attempt"
    ]
    assert len(attempt_records) >= 2
    first_variant = attempt_records[0]["variant"]
    assert all(record["variant"] == first_variant for record in attempt_records)

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_data_idle_watchdog_forces_reconnect_and_is_logged(tmp_path, monkeypatch):
    config = Config(
        capture_confirm_timeout_seconds=999.0,
        capture_confirm_min_events=1,
        ws_reconnect_backoff_seconds=0.0,
        ws_data_idle_reconnect_seconds=0.05,
    )
    groups = [["t1"]]
    state, shard, run_dir = _build_state(tmp_path, groups=groups, config=config)

    class BlockingWebSocket:
        def __init__(self):
            self._event = asyncio.Event()
            self.sent = []
            self.closed = False

        async def send(self, data):
            self.sent.append(data)

        async def recv(self):
            await self._event.wait()
            return b""

        async def close(self):
            self.closed = True

    first_ws = BlockingWebSocket()
    second_ws = FakeWebSocket(
        [b'{"event_type":"book","asset_id":"t1"}'],
        on_recv=state.stop_event.set,
    )
    connections = deque([first_ws, second_ws])
    connect_calls = {"count": 0}

    def fake_connect(*args, **kwargs):
        connect_calls["count"] += 1
        if not connections:
            raise AssertionError("unexpected websocket connect")
        return FakeConnect(connections.popleft())

    monkeypatch.setattr(capture_online.websockets, "connect", fake_connect)

    await asyncio.wait_for(capture_online._run_shard(state, shard), timeout=2.0)

    records = [
        json.loads(line)
        for line in (run_dir / "runlog.ndjson")
        .read_text(encoding="utf-8")
        .splitlines()
        if line.strip()
    ]
    reconnect_records = [
        record for record in records if record.get("record_type") == "reconnect"
    ]
    assert reconnect_records
    idle_records = [
        record
        for record in reconnect_records
        if record.get("trigger") == "data_idle_timeout"
    ]
    assert idle_records
    idle_record = idle_records[0]
    assert "close_code" in idle_record
    assert "close_reason" in idle_record
    assert connect_calls["count"] >= 2

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_confirm_deadline_miss_rotates_variant(tmp_path):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config()
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
        subscribe_variant_locked="B",
        subscribe_variant_index=0,
    )
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

    with pytest.raises(TimeoutError):
        await _handle_confirm_deadline_miss(
            state,
            shard,
            variant="B",
            now_ns=monotonic_ns(),
        )

    assert shard.subscribe_variant_index == 1
    assert shard.subscribe_variant_locked is None

    shard.frames_fh.close()
    shard.idx_fh.close()


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
