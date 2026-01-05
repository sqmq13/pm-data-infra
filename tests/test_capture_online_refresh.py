import asyncio
import json
from collections import deque

import pytest

from pm_arb.capture import RunBootstrap
from pm_arb.capture_online import (
    CaptureState,
    ShardState,
    UniverseState,
    _apply_churn_guard_policy,
    _build_shard_targets,
    _compute_refresh_delta,
    _eligible_token_ids,
    _heartbeat_loop,
    _load_startup_universe,
    _refresh_loop,
    _select_changed_shards,
    _should_apply_refresh,
    _stable_hash,
    split_subscribe_groups,
)
from pm_arb.config import Config
from pm_arb.gamma import UniverseSnapshot


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
        selection={"max_markets": 10, "filters_enabled": False},
    )

    def fake_compute(_cfg, *, universe_version=0):
        assert universe_version == 1
        return snapshot

    markets = [
        {"id": "m1", "active": True, "enableOrderBook": True, "clobTokenIds": ["t1", "t2"]},
        {"id": "m2", "active": True, "enableOrderBook": True, "clobTokenIds": ["t3", "t4"]},
    ]

    monkeypatch.setattr("pm_arb.capture_online.compute_desired_universe", fake_compute)
    monkeypatch.setattr("pm_arb.capture_online.fetch_markets", lambda *args, **kwargs: markets)

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
        selection={"max_markets": 10, "filters_enabled": False},
    )

    async def fake_sleep(_):
        return None

    def fake_compute(_cfg, *, universe_version=0):
        state.universe.refresh_cancelled = True
        return snapshot

    monkeypatch.setattr("pm_arb.capture_online.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pm_arb.capture_online.compute_desired_universe", fake_compute)

    await _refresh_loop(state)

    assert state.universe.refresh_count == 0
    assert state.universe.universe_version == 1
    assert state.universe.shards_refreshed_last == 0
    assert state.universe.refresh_last_decision_reason == "SKIPPED_NO_CHANGE"
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
        selection={"max_markets": 10, "filters_enabled": False},
    )

    async def fake_sleep(_):
        return None

    def fake_compute(_cfg, *, universe_version=0):
        state.universe.refresh_cancelled = True
        return snapshot

    monkeypatch.setattr("pm_arb.capture_online.asyncio.sleep", fake_sleep)
    monkeypatch.setattr("pm_arb.capture_online.compute_desired_universe", fake_compute)

    await _refresh_loop(state)

    captured: dict[str, dict[str, str | int]] = {}

    def fake_write_metrics(path, record):
        if path.name == "global.ndjson":
            captured["record"] = record
            state.fatal_event.set()

    monkeypatch.setattr("pm_arb.capture_online._write_metrics", fake_write_metrics)
    await _heartbeat_loop(state)

    record = captured["record"]
    assert record["refresh_skipped_delta_below_min_count"] == 1
    assert record["refresh_last_decision_reason"] == "SKIPPED_BELOW_MIN"

    shard.frames_fh.close()
    shard.idx_fh.close()
