import hashlib
from pathlib import Path

from pm_data.capture_offline import run_capture_offline
from pm_data.config import Config
from pm_data.runtime.normalize import Normalizer
from pm_data.runtime.replay import ReplayDataSource


def _build_run(tmp_path: Path) -> Path:
    config = Config()
    config.data_dir = str(tmp_path)
    fixtures_dir = Path("testdata/fixtures")
    result = run_capture_offline(
        config,
        fixtures_dir,
        run_id="test-run",
        emit_metrics=False,
    )
    return result.run.run_dir


def _hash_events(events) -> str:
    hasher = hashlib.sha256()
    for event in events:
        line = (
            f"{event.market_id}|{event.bid_px_e6}|{event.bid_sz_e6}|"
            f"{event.ask_px_e6}|{event.ask_sz_e6}|{event.ts_event}|{event.ts_recv}\n"
        )
        hasher.update(line.encode("utf-8"))
    return hasher.hexdigest()


def test_replay_determinism_hash(tmp_path: Path):
    run_dir = _build_run(tmp_path)
    normalizer = Normalizer()
    data_source = ReplayDataSource(run_dir=run_dir)
    first_events = []
    for frame in data_source.iter_frames():
        first_events.extend(normalizer.normalize(frame))
    first_hash = _hash_events(first_events)

    data_source = ReplayDataSource(run_dir=run_dir)
    second_events = []
    for frame in data_source.iter_frames():
        second_events.extend(normalizer.normalize(frame))
    second_hash = _hash_events(second_events)

    assert first_events
    assert first_hash == second_hash
