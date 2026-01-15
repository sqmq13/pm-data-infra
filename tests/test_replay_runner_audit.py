import hashlib
import json
from pathlib import Path

from pm_data.capture_format import FRAMES_SCHEMA_VERSION, append_record
from pm_data.runtime.entrypoint import run_replay_sim


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


def _snapshot_runs_dir() -> tuple[bool, list[str]]:
    runs_dir = Path("data") / "runs"
    if not runs_dir.exists():
        return False, []
    entries = sorted(path.name for path in runs_dir.iterdir())
    return True, entries


def test_replay_runner_determinism_and_no_runs_side_effects(tmp_path: Path):
    before_exists, before_entries = _snapshot_runs_dir()

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

    first = run_replay_sim(run_dir=run_dir, strategy_names=["toy_spread"])
    second = run_replay_sim(run_dir=run_dir, strategy_names=["toy_spread"])

    assert first.ok is True
    assert second.ok is True
    assert first.canonical_events > 0
    assert first.final_hash == second.final_hash
    expected_lines = [
        "1|toy_spread|place|tokenA|buy|400000|1000000|gtc|maker|toy_spread\n",
        "1|toy_spread|place|tokenA|sell|460000|1000000|gtc|maker|toy_spread\n",
        "2|toy_spread|place|tokenB|buy|390000|1000000|gtc|maker|toy_spread\n",
        "2|toy_spread|place|tokenB|sell|450000|1000000|gtc|maker|toy_spread\n",
        "3|toy_spread|place|tokenA|buy|410000|1000000|gtc|maker|toy_spread\n",
        "3|toy_spread|place|tokenA|sell|470000|1000000|gtc|maker|toy_spread\n",
        "4|toy_spread|place|tokenB|buy|420000|1000000|gtc|maker|toy_spread\n",
        "4|toy_spread|place|tokenB|sell|480000|1000000|gtc|maker|toy_spread\n",
    ]
    hasher = hashlib.sha256()
    for line in expected_lines:
        hasher.update(line.encode("utf-8"))
    assert first.final_hash == hasher.hexdigest()

    after_exists, after_entries = _snapshot_runs_dir()
    if before_exists:
        assert after_exists is True
        assert after_entries == before_entries
    else:
        assert after_exists is False
