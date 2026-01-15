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


def test_replay_reads_events(tmp_path: Path):
    run_dir = _build_run(tmp_path)
    data_source = ReplayDataSource(run_dir=run_dir)
    normalizer = Normalizer()
    events = []
    for frame in data_source.iter_frames():
        events.extend(normalizer.normalize(frame))
    assert len(events) == 8
    first = events[0]
    assert first.market_id == "tokenA"
    assert first.bid_px_e6 is None
    assert first.ask_px_e6 == 450_000
    assert first.ask_sz_e6 == 100_000_000
