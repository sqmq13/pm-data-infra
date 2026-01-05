import asyncio
import contextlib
import json
from collections import deque

import pytest

from pm_arb.capture import RunBootstrap, monotonic_ns
from pm_arb.capture_online import (
    CaptureState,
    ShardState,
    UniverseState,
    _apply_shard_refresh,
    _build_shard_targets,
    _coverage_pct,
    _refresh_loop,
    _stable_hash,
)
from pm_arb.config import Config
from pm_arb.gamma import UniverseSnapshot


async def _wait_for_runlog_record(run_dir, record_type, timeout_seconds=1.0):
    deadline = asyncio.get_running_loop().time() + timeout_seconds
    runlog_path = run_dir / "runlog.ndjson"
    while asyncio.get_running_loop().time() < deadline:
        if runlog_path.exists():
            lines = runlog_path.read_text(encoding="utf-8").splitlines()
            for line in lines:
                if not line.strip():
                    continue
                record = json.loads(line)
                if record.get("record_type") == record_type:
                    return lines
        await asyncio.sleep(0.01)
    raise AssertionError(f"timed out waiting for {record_type}")


@pytest.mark.asyncio
async def test_refresh_integration_changed_shard_and_grace(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(
        ws_shards=2,
        ws_subscribe_max_tokens=2,
        ws_subscribe_max_bytes=200,
        capture_universe_refresh_enable=True,
        capture_universe_refresh_interval_seconds=0.01,
        capture_universe_refresh_stagger_seconds=0.0,
        capture_universe_refresh_min_delta_tokens=1,
        capture_universe_refresh_grace_seconds=10.0,
        capture_universe_refresh_max_churn_pct=100.0,
    )

    initial_tokens = ["t1", "t2", "t3", "t4"]
    shard_targets = _build_shard_targets(initial_tokens, config, target_version=1, scores={})
    shards = []
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
        current_token_ids=set(initial_tokens),
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=shards,
        pinned_tokens=list(initial_tokens),
        universe=universe,
    )

    new_tokens = initial_tokens + ["t5"]
    calls = {"count": 0}

    def fake_compute(_cfg, *, universe_version=0):
        calls["count"] += 1
        return UniverseSnapshot(
            universe_version=universe_version,
            market_ids=["m1"],
            token_ids=new_tokens,
            created_wall_ns_utc=1,
            created_mono_ns=1,
            selection={"max_markets": 1, "filters_enabled": False},
        )

    monkeypatch.setattr("pm_arb.capture_online.compute_desired_universe", fake_compute)
    monkeypatch.setattr("pm_arb.capture_online.fetch_markets", lambda *args, **kwargs: [])

    refresh_task = asyncio.create_task(_refresh_loop(state))
    await _wait_for_runlog_record(run_dir, "universe_refresh")
    while state.universe.refresh_count < 1:
        await asyncio.sleep(0.01)

    changed = [shard for shard in shards if shard.target and shard.target.refresh_requested.is_set()]
    assert len(changed) == 1
    assert changed[0].shard_id == _stable_hash("t5") % config.ws_shards
    _apply_shard_refresh(state, changed[0])

    runlog_lines = (run_dir / "runlog.ndjson").read_text(encoding="utf-8").splitlines()
    record_types = {json.loads(line)["record_type"] for line in runlog_lines}
    assert "universe_refresh" in record_types
    assert "shard_refresh_begin" in record_types
    assert "shard_refresh_applied" in record_types

    now_ns = monotonic_ns()
    last_seen = {token_id: now_ns for token_id in initial_tokens}
    coverage = _coverage_pct(
        state.pinned_tokens,
        last_seen,
        now_ns,
        None,
        token_added_mono_ns=state.universe.token_added_mono_ns,
        grace_ns=int(config.capture_universe_refresh_grace_seconds * 1_000_000_000),
    )
    assert coverage == 100.0

    state.universe.refresh_cancelled = True
    refresh_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await refresh_task

    for shard in shards:
        shard.frames_fh.close()
        shard.idx_fh.close()
