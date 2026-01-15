from collections import deque

import pytest

from pm_data.capture import RunBootstrap
from pm_data.capture_online import CaptureState, ShardState, UniverseState, _heartbeat_loop
from pm_data.config import Config
from pm_data.fees import FeeRegimeMonitor
from pm_data.segments import SegmentTag


def _segment(cadence_bucket: str, is_crypto: bool | None, fee_rate_bps: int) -> SegmentTag:
    return SegmentTag(
        cadence_bucket=cadence_bucket,
        is_crypto=is_crypto,
        fee_rate_bps=fee_rate_bps,
        fee_enabled=fee_rate_bps > 0,
        fee_rate_known=True,
        start_wall_ns_utc=None,
        end_wall_ns_utc=None,
        resolve_wall_ns_utc=None,
        rollover_group_key=None,
        rollover_instrument=None,
        rollover_boundary_key=None,
    )


def test_fee_regime_trips_on_unexpected_fee():
    monitor = FeeRegimeMonitor(
        expect_15m_crypto_fee_enabled=True,
        expect_other_fee_free=True,
        expect_unknown_fee_free=True,
        expected_fee_rate_bps_values=set(),
    )
    segments = {"t1": _segment("30m", False, 10)}
    state = monitor.evaluate(segments, token_ids_sampled=["t1"])
    assert state.state == "TRIPPED"
    assert state.reason == "FEE_UNEXPECTED_ON_FREE_SEGMENT"


def test_fee_regime_trips_on_missing_fee():
    monitor = FeeRegimeMonitor(
        expect_15m_crypto_fee_enabled=True,
        expect_other_fee_free=True,
        expect_unknown_fee_free=True,
        expected_fee_rate_bps_values=set(),
    )
    segments = {"t1": _segment("15m", True, 0)}
    state = monitor.evaluate(segments, token_ids_sampled=["t1"])
    assert state.state == "TRIPPED"
    assert state.reason == "FEE_EXPECTED_BUT_ZERO"


def test_fee_regime_trips_on_unexpected_fee_rate_value():
    monitor = FeeRegimeMonitor(
        expect_15m_crypto_fee_enabled=True,
        expect_other_fee_free=True,
        expect_unknown_fee_free=True,
        expected_fee_rate_bps_values={25},
    )
    segments = {"t1": _segment("15m", True, 50)}
    state = monitor.evaluate(segments, token_ids_sampled=["t1"])
    assert state.state == "TRIPPED"
    assert state.reason == "FEE_RATE_BPS_UNEXPECTED_VALUE"


def test_fee_regime_sampling_reproducible_and_capped():
    monitor = FeeRegimeMonitor(
        expect_15m_crypto_fee_enabled=True,
        expect_other_fee_free=True,
        expect_unknown_fee_free=True,
        expected_fee_rate_bps_values=set(),
    )
    tokens = [f"t{i}" for i in range(10)]
    first = monitor.sample_tokens(
        tokens,
        run_id="run",
        universe_version=1,
        sample_size=3,
        canary_token_ids=[],
    )
    second = monitor.sample_tokens(
        tokens,
        run_id="run",
        universe_version=1,
        sample_size=3,
        canary_token_ids=[],
    )
    assert first == second
    assert len(first) == 3


@pytest.mark.asyncio
async def test_fee_regime_trip_surfaces_in_metrics(tmp_path, monkeypatch):
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "metrics").mkdir()
    (run_dir / "capture").mkdir()
    run = RunBootstrap("run", run_dir, 0, 0)
    config = Config(capture_heartbeat_interval_seconds=0.01, fee_rate_enable=False)
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
    )
    universe = UniverseState(
        universe_version=1,
        current_token_ids={"t1"},
        current_market_ids=set(),
        token_added_mono_ns={},
        shard_targets={},
        fee_regime_state="TRIPPED",
        fee_regime_reason="FEE_EXPECTED_BUT_ZERO",
        fee_regime_sampled_tokens_count=2,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=[shard],
        pinned_tokens=["t1"],
        universe=universe,
    )
    captured = {}

    def fake_write_metrics(_state, path, record):
        if path.name == "global.ndjson":
            captured["record"] = record
            state.fatal_event.set()

    monkeypatch.setattr("pm_data.capture_online._write_metrics", fake_write_metrics)
    await _heartbeat_loop(state)

    record = captured["record"]
    assert record["fee_regime_state"] == "TRIPPED"
    assert record["circuit_breaker_reason"] == "FEE_EXPECTED_BUT_ZERO"

    shard.frames_fh.close()
    shard.idx_fh.close()

