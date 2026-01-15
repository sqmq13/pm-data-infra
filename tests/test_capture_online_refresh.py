import asyncio
import contextlib
import json
import time
from collections import deque
from datetime import datetime, timezone

import pytest

from pm_data.capture import RunBootstrap
from pm_data.capture_online import (
    CaptureState,
    RefreshPlan,
    ShardState,
    UniverseState,
    _apply_churn_guard_policy,
    _build_shard_target_plans,
    _build_shard_targets,
    _compute_refresh_delta,
    _eligible_token_ids,
    _heartbeat_loop,
    _load_startup_universe,
    _loop_lag_monitor,
    _refresh_loop,
    _select_changed_shards,
    _should_apply_refresh,
    _stable_hash,
    split_subscribe_groups,
)
from pm_data.config import Config
from pm_data.fees import FeeRegimeState
from pm_data.gamma import UniverseSnapshot
from pm_data.segments import build_segment_maps


def test_refresh_delta_min_threshold():
    current = {"a", "b", "c"}
    desired = {"a", "b", "c", "d"}
    added, removed = _compute_refresh_delta(current, desired)
    assert added == {"d"}
    assert removed == set()
    assert _should_apply_refresh(added, removed, 2) is False
    assert _should_apply_refresh(added, removed, 1) is True


def test_refresh_changed_shards_only():
    config = Config(ws_shards=2)
    tokens = ["tokenA", "tokenB", "tokenC"]
    current_targets = _build_shard_targets(tokens, config, target_version=1, scores={})
    new_token = "tokenZ"
    next_targets = _build_shard_targets(
        tokens + [new_token],
        config,
        target_version=2,
        scores={},
    )
    changed = _select_changed_shards(current_targets, next_targets)
    expected_shard = _stable_hash(new_token) % config.ws_shards
    assert set(changed) == {expected_shard}


def test_refresh_grouping_reuses_subscribe_groups():
    config = Config(ws_shards=1, ws_subscribe_max_tokens=2, ws_subscribe_max_bytes=200)
    tokens = ["t1", "t2", "t3"]
    targets = _build_shard_targets(tokens, config, target_version=1, scores={})
    expected = split_subscribe_groups(
        sorted(set(tokens)),
        config.ws_subscribe_max_tokens,
        config.ws_subscribe_max_bytes,
        "A",
    )
    assert targets[0].groups == expected


def test_grace_excludes_new_tokens_until_seen_or_expired():
    token_ids = ["t1", "t2", "t3"]
    last_seen = {"t1": 100}
    token_added = {"t2": 95, "t3": 95}
    eligible = _eligible_token_ids(token_ids, last_seen, token_added, now_ns=100, grace_ns=10)
    assert eligible == ["t1"]
    assert "t2" in token_added
    assert "t3" in token_added

    last_seen["t3"] = 99
    eligible = _eligible_token_ids(token_ids, last_seen, token_added, now_ns=100, grace_ns=10)
    assert "t3" in eligible
    assert "t3" not in token_added

    eligible = _eligible_token_ids(token_ids, last_seen, token_added, now_ns=200, grace_ns=10)
    assert "t2" in eligible
    assert "t2" not in token_added


def test_churn_guard_backoff_bounded():
    next_interval, guard_count, fatal = _apply_churn_guard_policy(
        400.0,
        60.0,
        600.0,
        guard_triggered=True,
        guard_count=1,
        fatal_threshold=5,
    )
    assert next_interval == 600.0
    assert guard_count == 2
    assert fatal is False


def test_churn_guard_resets_interval_on_success():
    next_interval, guard_count, fatal = _apply_churn_guard_policy(
        240.0,
        60.0,
        600.0,
        guard_triggered=False,
        guard_count=3,
        fatal_threshold=5,
    )
    assert next_interval == 60.0
    assert guard_count == 0
    assert fatal is False


def test_churn_guard_sustained_fatal_threshold():
    next_interval, guard_count, fatal = _apply_churn_guard_policy(
        120.0,
        60.0,
        600.0,
        guard_triggered=True,
        guard_count=4,
        fatal_threshold=5,
    )
    assert guard_count == 5
    assert fatal is True


def test_startup_selection_uses_snapshot(monkeypatch):
    snapshot = UniverseSnapshot(
        universe_version=1,
        market_ids=["m2", "m1"],
        token_ids=["t3", "t4", "t1", "t2"],
        created_wall_ns_utc=1,
        created_mono_ns=2,
        selection={"max_markets": 10},
    )

    def fake_compute(_cfg, *, universe_version=0, **_kwargs):
        assert universe_version == 1
        return snapshot

    markets = [
        {"id": "m1", "active": True, "enableOrderBook": True, "clobTokenIds": ["t1", "t2"]},
        {"id": "m2", "active": True, "enableOrderBook": True, "clobTokenIds": ["t3", "t4"]},
    ]

    monkeypatch.setattr("pm_data.capture_online.compute_desired_universe", fake_compute)
    monkeypatch.setattr("pm_data.capture_online.fetch_markets", lambda *args, **kwargs: markets)

    config = Config(capture_max_markets=10)
    startup_snapshot, pinned_tokens, pinned_markets, selected_markets, universe_mode = (
        _load_startup_universe(config)
    )
    assert startup_snapshot is snapshot
    assert pinned_tokens == snapshot.token_ids
    assert [market["id"] for market in pinned_markets] == snapshot.market_ids
    assert [market["id"] for market in selected_markets] == snapshot.market_ids
    assert universe_mode == "active-binary"


@pytest.mark.asyncio
async def test_refresh_noop_does_not_reconnect(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        ws_shards=2,
        capture_universe_refresh_enable=True,
        capture_universe_refresh_interval_seconds=0.01,
        capture_universe_refresh_min_delta_tokens=1,
        capture_universe_refresh_stagger_seconds=0.0,
        capture_universe_refresh_max_churn_pct=100.0,
        fee_rate_enable=False,
    )
    tokens = ["t1", "t2", "t3", "t4"]
    shard_targets = _build_shard_targets(tokens, config, target_version=1, scores={})
    shards: list[ShardState] = []
    for shard_id, target in shard_targets.items():
        frames_path = run_dir / "capture" / f"shard_{shard_id:02d}.frames"
        idx_path = run_dir / "capture" / f"shard_{shard_id:02d}.idx"
        shard = ShardState(
            shard_id=shard_id,
            token_ids=target.token_ids,
            groups=target.groups,
            frames_path=frames_path,
            idx_path=idx_path,
            frames_fh=frames_path.open("ab"),
            idx_fh=idx_path.open("ab"),
            ring=deque(maxlen=8),
            target=target,
        )
        shards.append(shard)
    universe = UniverseState(
        universe_version=1,
        current_token_ids=set(tokens),
        current_market_ids={"m1"},
        token_added_mono_ns={},
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=shards,
        pinned_tokens=list(tokens),
        universe=universe,
    )

    snapshot = UniverseSnapshot(
        universe_version=2,
        market_ids=["m1"],
        token_ids=list(tokens),
        created_wall_ns_utc=1,
        created_mono_ns=2,
        selection={"max_markets": 10},
        selected_markets=[],
    )

    async def fake_sleep(_):
        return None

    def fake_compute(_cfg, *, universe_version=0, **_kwargs):
        state.universe.refresh_cancelled = True
        return snapshot

    monkeypatch.setattr("pm_data.capture_online.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pm_data.capture_online.compute_desired_universe", fake_compute)

    await _refresh_loop(state)

    assert state.universe.refresh_count == 0
    assert state.universe.universe_version == 1
    assert state.universe.shards_refreshed_last == 0
    assert state.universe.refresh_last_decision_reason == "SKIPPED_DELTA_BELOW_MIN"
    assert all(
        shard.target is not None and not shard.target.refresh_requested.is_set()
        for shard in shards
    )

    runlog_path = run_dir / "runlog.ndjson"
    if runlog_path.exists():
        record_types = {
            json.loads(line)["record_type"]
            for line in runlog_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        }
        assert "shard_refresh_begin" not in record_types
        assert "shard_refresh_applied" not in record_types

    for shard in shards:
        shard.frames_fh.close()
        shard.idx_fh.close()


@pytest.mark.asyncio
async def test_refresh_skip_below_min_updates_metrics(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        ws_shards=1,
        capture_universe_refresh_enable=True,
        capture_universe_refresh_interval_seconds=0.01,
        capture_universe_refresh_min_delta_tokens=2,
        capture_universe_refresh_stagger_seconds=0.0,
        capture_universe_refresh_max_churn_pct=100.0,
        capture_heartbeat_interval_seconds=0.01,
        fee_rate_enable=False,
    )
    tokens = ["t1", "t2"]
    shard_targets = _build_shard_targets(tokens, config, target_version=1, scores={})
    target = shard_targets[0]
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=target.token_ids,
        groups=target.groups,
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=8),
        target=target,
    )
    universe = UniverseState(
        universe_version=1,
        current_token_ids=set(tokens),
        current_market_ids={"m1"},
        token_added_mono_ns={},
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=list(tokens),
        universe=universe,
    )

    snapshot = UniverseSnapshot(
        universe_version=2,
        market_ids=["m1"],
        token_ids=["t1", "t2", "t3"],
        created_wall_ns_utc=1,
        created_mono_ns=2,
        selection={"max_markets": 10},
        selected_markets=[],
    )

    async def fake_sleep(_):
        return None

    def fake_compute(_cfg, *, universe_version=0, **_kwargs):
        state.universe.refresh_cancelled = True
        return snapshot

    monkeypatch.setattr("pm_data.capture_online.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pm_data.capture_online.compute_desired_universe", fake_compute)

    await _refresh_loop(state)

    captured: dict[str, dict[str, str | int]] = {}

    def fake_write_metrics(_state, path, record):
        if path.name == "global.ndjson":
            captured["record"] = record
            state.fatal_event.set()

    monkeypatch.setattr("pm_data.capture_online._write_metrics", fake_write_metrics)
    await _heartbeat_loop(state)

    record = captured["record"]
    assert record["refresh_skipped_delta_below_min_count"] == 1
    assert record["refresh_last_decision_reason"] == "SKIPPED_DELTA_BELOW_MIN"
    assert "expected_churn_bool" in record
    assert "fee_regime_state" in record

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_expected_churn_bypasses_guard(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        ws_shards=1,
        capture_universe_refresh_enable=True,
        capture_universe_refresh_interval_seconds=0.01,
        capture_universe_refresh_min_delta_tokens=1,
        capture_universe_refresh_stagger_seconds=0.0,
        capture_universe_refresh_max_churn_pct=1.0,
        capture_expected_churn_window_seconds_15m=120.0,
        fee_rate_enable=False,
    )
    tokens = ["t1", "t2"]
    shard_targets = _build_shard_targets(tokens, config, target_version=1, scores={})
    target = shard_targets[0]
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=target.token_ids,
        groups=target.groups,
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=8),
        target=target,
    )
    current_market = {
        "id": "m1",
        "question": "BTC 15 minute price?",
        "clobTokenIds": ["t1", "t2"],
        "active": True,
        "enableOrderBook": True,
    }
    segment_by_token, segment_by_market = build_segment_maps([current_market])
    universe = UniverseState(
        universe_version=1,
        current_token_ids=set(tokens),
        current_market_ids={"m1"},
        token_added_mono_ns={},
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
        segment_by_token_id=segment_by_token,
        segment_by_market_id=segment_by_market,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=list(tokens),
        universe=universe,
    )
    new_market = {
        "id": "m2",
        "question": "ETH 15 minute price?",
        "clobTokenIds": ["t3", "t4"],
        "active": True,
        "enableOrderBook": True,
    }
    snapshot = UniverseSnapshot(
        universe_version=2,
        market_ids=["m2"],
        token_ids=["t3", "t4"],
        created_wall_ns_utc=1,
        created_mono_ns=2,
        selection={"max_markets": 10},
        selected_markets=[new_market],
    )

    async def fake_sleep(_):
        return None

    def fake_compute(_cfg, *, universe_version=0, **_kwargs):
        state.universe.refresh_cancelled = True
        return snapshot

    boundary = datetime(2024, 1, 1, 0, 15, 30, tzinfo=timezone.utc)
    boundary_ns = int(boundary.timestamp() * 1_000_000_000)

    monkeypatch.setattr("pm_data.capture_online.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pm_data.capture_online.compute_desired_universe", fake_compute)
    monkeypatch.setattr("pm_data.capture_online.time.time_ns", lambda: boundary_ns)

    await _refresh_loop(state)

    assert state.universe.refresh_last_decision_reason == "BYPASSED_EXPECTED_CHURN"
    assert state.universe.refresh_churn_guard_count == 0
    assert state.universe.refresh_count == 1

    shard.frames_fh.close()
    shard.idx_fh.close()


@pytest.mark.asyncio
async def test_refresh_worker_does_not_block_heartbeat(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        ws_shards=1,
        capture_universe_refresh_enable=True,
        capture_universe_refresh_interval_seconds=0.01,
        capture_universe_refresh_timeout_seconds=1.0,
        capture_universe_refresh_min_delta_tokens=1,
        capture_universe_refresh_stagger_seconds=0.0,
        capture_universe_refresh_max_churn_pct=100.0,
        capture_heartbeat_interval_seconds=0.05,
        fee_rate_enable=False,
    )
    tokens = ["t1", "t2"]
    shard_targets = _build_shard_targets(tokens, config, target_version=1, scores={})
    target = shard_targets[0]
    frames_path = run_dir / "capture" / "shard_00.frames"
    idx_path = run_dir / "capture" / "shard_00.idx"
    shard = ShardState(
        shard_id=0,
        token_ids=target.token_ids,
        groups=target.groups,
        frames_path=frames_path,
        idx_path=idx_path,
        frames_fh=frames_path.open("ab"),
        idx_fh=idx_path.open("ab"),
        ring=deque(maxlen=8),
        target=target,
    )
    universe = UniverseState(
        universe_version=1,
        current_token_ids=set(tokens),
        current_market_ids={"m1"},
        token_added_mono_ns={},
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=list(tokens),
        universe=universe,
    )
    markets = [
        {
            "id": "m1",
            "question": "Market 1?",
            "clobTokenIds": ["t1", "t2"],
            "active": True,
            "enableOrderBook": True,
        },
        {
            "id": "m2",
            "question": "Market 2?",
            "clobTokenIds": ["t3", "t4"],
            "active": True,
            "enableOrderBook": True,
        },
    ]
    snapshot = UniverseSnapshot(
        universe_version=2,
        market_ids=["m1", "m2"],
        token_ids=["t1", "t2", "t3", "t4"],
        created_wall_ns_utc=1,
        created_mono_ns=2,
        selection={"max_markets": 10},
        selected_markets=markets,
    )

    def heavy_compute(
        config,
        *,
        universe_version=0,
        run_id="run",
        canary_tokens=None,
        session=None,
        loop=None,
        fee_rate_client=None,
        policy_selector=None,
        fee_regime_monitor=None,
        refresh_timeout_seconds=0.0,
    ):
        time.sleep(0.3)
        _ = sum(idx * idx for idx in range(20000))
        segment_by_token, segment_by_market = build_segment_maps(markets)
        ordered_tokens = sorted(set(snapshot.token_ids))
        shard_plans = _build_shard_target_plans(
            ordered_tokens,
            config,
            target_version=universe_version,
            scores={},
        )
        fee_regime_state = FeeRegimeState(
            state="OK",
            reason="DISABLED",
            sampled_tokens_count=0,
            unknown_fee_count=0,
            observed_fee_rates=[],
        )
        return RefreshPlan(
            snapshot=snapshot,
            desired_token_ids=set(snapshot.token_ids),
            desired_market_ids=set(snapshot.market_ids),
            ordered_tokens=ordered_tokens,
            shard_targets=shard_plans,
            segment_by_token_id=segment_by_token,
            segment_by_market_id=segment_by_market,
            policy_counts={},
            fee_regime_state=fee_regime_state,
            fee_regime_sampled_tokens_count=0,
            fee_rate_unknown_count=0,
            gamma_pages_fetched=1,
            markets_seen=len(markets),
            tokens_selected=len(snapshot.token_ids),
            worker_duration_ms=300.0,
        )

    monkeypatch.setattr("pm_data.capture_online._compute_refresh_plan_sync", heavy_compute)

    records: list[dict[str, float]] = []

    def fake_write_metrics(_state, path, record):
        if path.name == "global.ndjson":
            records.append(record)
            if len(records) >= 6:
                state.stop_event.set()

    monkeypatch.setattr("pm_data.capture_online._write_metrics", fake_write_metrics)

    tasks = [
        asyncio.create_task(_refresh_loop(state)),
        asyncio.create_task(_loop_lag_monitor(state)),
        asyncio.create_task(_heartbeat_loop(state)),
    ]
    await asyncio.wait_for(state.stop_event.wait(), timeout=2.0)
    state.universe.refresh_cancelled = True
    for task in tasks:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    assert len(records) >= 5
    max_lag = max(record["loop_lag_ms_max_last_interval"] for record in records)
    assert max_lag < 100.0

    shard.frames_fh.close()
    shard.idx_fh.close()


