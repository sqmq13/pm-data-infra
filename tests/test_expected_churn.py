from datetime import datetime, timezone

from pm_data.capture_online import _classify_expected_churn, _expected_churn_for_refresh
from pm_data.config import Config
from pm_data.segments import SegmentTag


def _segment(cadence_bucket: str, end_ns: int | None = None) -> SegmentTag:
    return SegmentTag(
        cadence_bucket=cadence_bucket,
        is_crypto=True,
        fee_rate_bps=None,
        fee_enabled=None,
        fee_rate_known=False,
        start_wall_ns_utc=None,
        end_wall_ns_utc=end_ns,
        resolve_wall_ns_utc=None,
        rollover_group_key=None,
        rollover_instrument=None,
        rollover_boundary_key=None,
    )


def test_expected_churn_boundary_15m():
    config = Config(capture_expected_churn_window_seconds_15m=60.0)
    now_utc = datetime(2024, 1, 1, 0, 15, 30, tzinfo=timezone.utc)
    expected, reason, rollover_hit, expiry_hit = _classify_expected_churn(
        now_utc, _segment("15m"), config
    )
    assert expected is True
    assert reason == "EXPECTED_ROLLOVER_15M"
    assert rollover_hit is True
    assert expiry_hit is False


def test_expected_churn_expiry_window():
    config = Config(capture_expected_churn_expiry_window_seconds=120.0)
    now_utc = datetime(2024, 1, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_ns = int(now_utc.timestamp() * 1_000_000_000) + 30_000_000_000
    expected, reason, rollover_hit, expiry_hit = _classify_expected_churn(
        now_utc, _segment("60m", end_ns=end_ns), config
    )
    assert expected is True
    assert reason == "EXPECTED_EXPIRY"
    assert rollover_hit is False
    assert expiry_hit is True


def test_expected_churn_summary_ratio():
    config = Config(capture_expected_churn_min_ratio=0.5)
    now_utc = datetime(2024, 1, 1, 0, 30, 0, tzinfo=timezone.utc)
    now_ns = int(now_utc.timestamp() * 1_000_000_000)
    removed = {"m1", "m2"}
    added = {"m3"}
    current_segments = {"m1": _segment("15m"), "m2": _segment("unknown")}
    next_segments = {"m3": _segment("15m")}
    summary = _expected_churn_for_refresh(
        now_ns,
        removed,
        added,
        current_segments,
        next_segments,
        config,
    )
    assert summary.expected_churn is True
    assert summary.reason in {"EXPECTED_ROLLOVER_15M", "EXPECTED_EXPIRY"}
