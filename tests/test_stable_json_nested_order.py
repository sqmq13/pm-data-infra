import json

from pm_data.runtime.entrypoint import RunSummary, format_run_summary


def test_stable_json_sorts_nested_mapping_keys() -> None:
    summary = RunSummary(
        ok=True,
        mode="replay",
        execution="sim",
        canonical_events=1,
        intents=0,
        final_hash="abc",
        elapsed_ms=12.0,
        pnl_summary={
            "positions_e6": {"b": 1, "a": 2},
        },
    )
    payload = json.loads(format_run_summary(summary, stable=True))
    assert list(payload["pnl"]["positions_e6"].keys()) == ["a", "b"]

