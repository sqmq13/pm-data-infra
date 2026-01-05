# Universe Refresh Plan (Stage 0 Inventory)

## Current entrypoints
- CLI entrypoint: `pm_arb` in `pm_arb/cli.py`.
- Online capture entry: `pm_arb capture` → `pm_arb.capture_online.run_capture_online()`.
- Online capture implementation: `pm_arb/capture_online.py::_capture_online_async()`.

## Universe selection + shard assignment
- Universe selection: `pm_arb/capture_online.py::_load_pinned_markets()`.
  - Uses `pm_arb.gamma.fetch_markets()` and `pm_arb.gamma.select_active_binary_markets()`.
  - Extracts token ids via `pm_arb.gamma.parse_clob_token_ids()`.
- Shard assignment: `pm_arb/capture_online.py::assign_shards_by_token()`.
- Grouping/chunking: `pm_arb/capture_online.py::split_subscribe_groups()`.

## Coverage + confirm/health gates
- Coverage computation: `pm_arb/capture_online.py::_coverage_pct()` and `_heartbeat_loop()`.
- Confirm logic: `pm_arb/capture_online.py::_run_shard()` + `_confirm_event_from_payload()`.
- Health/fatal gates: `_heartbeat_loop()` (confirm deadline, low disk) and `_check_backpressure_fatal()`.

## Runlog/manifest/metrics writes
- Manifest + runlog bootstrap: `pm_arb/capture.py::bootstrap_run()`.
- Runlog writes: `pm_arb/capture_online.py::_write_runlog()`.
- Metrics writes: `pm_arb/capture_online.py::_write_metrics()` + `_heartbeat_loop()`.

## Files/functions to change (anticipated)
- `pm_arb/config.py` — add refresh config fields.
- `pm_arb/capture_online.py` - add universe state, refresh loop, shard refresh signals, grace-aware coverage, churn guard/backoff, new runlog/metrics fields, refresh metadata in manifest, token-hash sharding.
- `pm_arb/capture_inspect.py` — expose refresh metadata if needed.
- `pm_arb/gamma.py` — universe recompute helper.
- `README.md` — add “Universe Refresh” section + example commands.

## Stage plan (high level)
- Stage 1: Config + types + docs.
- Stage 2: Desired universe recompute + deterministic ordering tests.
- Stage 3: Refresh loop + shard reconnect plumbing.
- Stage 4: Grace-aware coverage + confirm reset behavior.
- Stage 5: Churn guard + bounded backoff + shutdown safety.
- Stage 6: Integration-style test + docs update.

## Stage 1 additions (config + types)

Config fields to add (defaults):
- `capture_universe_refresh_enable`: false
- `capture_universe_refresh_interval_seconds`: 60.0
- `capture_universe_refresh_stagger_seconds`: 0.25
- `capture_universe_refresh_grace_seconds`: 30.0
- `capture_universe_refresh_min_delta_tokens`: 2
- `capture_universe_refresh_max_churn_pct`: 10.0
- `capture_universe_refresh_max_interval_seconds`: 600.0
- `capture_universe_refresh_churn_guard_consecutive_fatal`: 5

UniverseSnapshot type:
- `universe_version` (int)
- `market_ids` (ordered list of strings)
- `token_ids` (ordered list of strings)
- `created_wall_ns_utc` (int)
- `created_mono_ns` (int)
- `selection` (dict with selection metadata, including max_markets and filter flags)
