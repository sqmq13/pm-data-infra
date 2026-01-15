import json
from pathlib import Path

from pm_data.capture_format import FRAMES_SCHEMA_VERSION, append_record
from pm_data.runtime.entrypoint import format_run_summary, run_replay_sim


def _payload(asset_id: str, bid: str, ask: str) -> bytes:
    payload = {
        "event_type": "book",
        "asset_id": asset_id,
        "timestamp": 1000,
        "bids": [{"price": bid, "size": "10"}],
        "asks": [{"price": ask, "size": "8"}],
    }
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def _write_shard(
    capture_dir: Path,
    shard_id: int,
    records: list[tuple[int, bytes, int]],
) -> None:
    frames_path = capture_dir / f"shard_{shard_id:02d}.frames"
    idx_path = capture_dir / f"shard_{shard_id:02d}.idx"
    with frames_path.open("ab") as frames_fh, idx_path.open("ab") as idx_fh:
        for rx_mono_ns, payload, rx_wall_ns_utc in records:
            append_record(
                frames_fh,
                idx_fh,
                payload,
                rx_mono_ns,
                rx_wall_ns_utc,
                schema_version=FRAMES_SCHEMA_VERSION,
            )


def test_replay_stable_json_output(tmp_path: Path):
    run_dir = tmp_path / "mini_run"
    capture_dir = run_dir / "capture"
    capture_dir.mkdir(parents=True)
    shard0 = [
        (1_000, _payload("tokenA", "0.40", "0.46"), 1_000_000_000),
        (1_200, _payload("tokenA", "0.41", "0.47"), 1_000_000_200),
    ]
    shard1 = [
        (1_000, _payload("tokenB", "0.39", "0.45"), 1_000_000_100),
        (1_300, _payload("tokenB", "0.42", "0.48"), 1_000_000_300),
    ]
    _write_shard(capture_dir, 0, shard0)
    _write_shard(capture_dir, 1, shard1)

    params = {
        "toy_spread": {
            "min_spread_e6": 0,
            "urgency": "taker",
            "order_type": "market",
            "tif": "ioc",
        }
    }
    first = run_replay_sim(
        run_dir=run_dir,
        strategy_names=["toy_spread"],
        strategy_params=params,
        include_pnl=True,
    )
    second = run_replay_sim(
        run_dir=run_dir,
        strategy_names=["toy_spread"],
        strategy_params=params,
        include_pnl=True,
    )

    out1 = format_run_summary(first, stable=True)
    out2 = format_run_summary(second, stable=True)

    assert out1 == out2
    payload = json.loads(out1)
    assert payload["elapsed_ms"] == 0
    assert payload["final_hash"] == first.final_hash
    assert payload["canonical_events"] > 0
    assert "pnl" in payload
