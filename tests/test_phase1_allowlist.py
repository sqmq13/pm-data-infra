import pkgutil

import pm_data
import pm_data.cli


def test_phase1_allowlist() -> None:
    allowlist = {
        "cli",
        "config",
        "gamma",
        "clob_ws",
        "ws_primitives",
        "ws_decode",
        "capture",
        "capture_online",
        "capture_state",
        "capture_offline",
        "capture_format",
        "capture_inspect",
        "capture_slice",
        "fatal_policy",
        "fees",
        "metrics_heartbeat",
        "parse_pipeline",
        "policy",
        "runtime",
        "segments",
        "strategies",
        "ws_runner",
        "writers_frames",
        "writers_ndjson",
    }
    banned = {
        "book",
        "fixed",
        "engine",
        "sweep",
        "reconcile",
        "report",
        "base_scan",
        "scan",
        "strategy",
    }
    modules = {info.name for info in pkgutil.iter_modules(pm_data.__path__)}
    assert modules.issubset(allowlist)
    assert modules.isdisjoint(banned)
