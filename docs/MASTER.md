# MASTER

## 1. Scope / system overview
- Phase 1 capture plant for Polymarket CLOB WS frames (active-binary YES/NO).
- Multi-shard WS capture with raw payload bytes plus per-frame `rx_mono_ns` and `rx_wall_ns_utc`.
- Run bundles live under `data/runs/<run_id>/` (rooted at `data_dir`) with stable layout.
- Integrity verification, audit, and latency reporting are first-class (`capture-verify`, `capture-audit`, `capture-latency-report`).
- No trading, signing, or data-rewriting transforms; payload bytes are preserved as received.

Run bundle layout (online capture):
- `manifest.json` (run metadata, config snapshot, universe metadata).
- `runlog.ndjson` (heartbeats, reconnects, refresh records, lifecycle events).
- `capture/shard_<NN>.frames` and `capture/shard_<NN>.idx`.
- `metrics/global.ndjson` and `metrics/shard_<NN>.ndjson`.
- `latency_report.json` and `latency_report.txt` (created by `capture-latency-report`).
- On fatal runs: `fatal.json`, `missing_tokens.json`, `last_frames_shard_<NN>.bin`.

## 2. Quickstart (uv)
```bash
uv sync
uv run pm_arb --help
```

Optional uvloop install (non-Windows) and verification:
```bash
uv pip install uvloop
uv run python -c "import uvloop"
```

## 3. Configuration
Configuration comes from the `Config` dataclass in `pm_arb/config.py`.

Workflow:
- Defaults are the baseline capture profile; override via env/CLI only when needed.
- CLI flags use kebab-case (`--ws-shards`, `--capture-max-markets`).
- Boolean flags use `--<name>` and `--no-<name>`, except `--offline` which accepts `--offline` / `--no-offline`.
- Environment variables use `PM_ARB_` + uppercase field name (example: `PM_ARB_WS_SHARDS`).
- Environment variables override CLI overrides when both are set.

Key settings and defaults:
| Setting | Env var | CLI flag | Default | Notes |
| --- | --- | --- | --- | --- |
| Shard count | `PM_ARB_WS_SHARDS` | `--ws-shards` | `8` | Multi-shard WS capture. |
| Ping interval | `PM_ARB_WS_PING_INTERVAL_SECONDS` | `--ws-ping-interval-seconds` | `10.0` | Keepalive cadence. |
| Ping timeout | `PM_ARB_WS_PING_TIMEOUT_SECONDS` | `--ws-ping-timeout-seconds` | `10.0` | Ping response deadline. |
| Data-idle reconnect | `PM_ARB_WS_DATA_IDLE_RECONNECT_SECONDS` | `--ws-data-idle-reconnect-seconds` | `120.0` | Force reconnect if no data. |
| Confirm timeout | `PM_ARB_CAPTURE_CONFIRM_TIMEOUT_SECONDS` | `--capture-confirm-timeout-seconds` | `45.0` | Startup confirm window. |
| Confirm min events | `PM_ARB_CAPTURE_CONFIRM_MIN_EVENTS` | `--capture-confirm-min-events` | `1` | Minimum events to confirm. |
| Universe refresh enable | `PM_ARB_CAPTURE_UNIVERSE_REFRESH_ENABLE` | `--capture-universe-refresh-enable` | `true` | Enabled by default. |
| Universe refresh interval | `PM_ARB_CAPTURE_UNIVERSE_REFRESH_INTERVAL_SECONDS` | `--capture-universe-refresh-interval-seconds` | `60.0` | Base refresh cadence. |
| Max markets safety cap | `PM_ARB_CAPTURE_MAX_MARKETS` | `--capture-max-markets` | `2000` | Upper bound on active-binary universe. |
| Data directory | `PM_ARB_DATA_DIR` | `--data-dir` | `./data` | Run bundle root. |

Universe refresh tuning knobs (defaults are sane; change only when needed):
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_TIMEOUT_SECONDS` (30.0)
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_STAGGER_SECONDS` (0.25)
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_GRACE_SECONDS` (30.0)
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_MIN_DELTA_TOKENS` (2)
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_MAX_CHURN_PCT` (10.0)
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_MAX_INTERVAL_SECONDS` (600.0)
- `PM_ARB_CAPTURE_UNIVERSE_REFRESH_CHURN_GUARD_CONSECUTIVE_FATAL` (5)

## 4. CLI inventory
All CLI subcommands (from `pm_arb/cli.py`):
- `discover`: print active-binary universe candidates from Gamma.
- `capture`: online capture (or offline when `--offline`).
- `capture-verify`: validate frames/idx files and CRCs.
- `capture-bench`: offline ingest bench using fixtures.
- `capture-inspect`: summarize run bundle sizes and metrics tails.
- `capture-audit`: heartbeat gap audit on `runlog.ndjson`.
- `capture-latency-report`: generate `latency_report.json`/`.txt`.
- `capture-slice`: extract a slice from a run (by time or offset).

## 5. Operations playbook
tmux workflow:
```bash
tmux new -s pm-arb
```
Detach with `Ctrl-b d`, then reattach later:
```bash
tmux attach -t pm-arb
```

10-minute run (SIGINT via timeout):
```bash
timeout --signal=INT 10m uv run pm_arb capture --run-id run-10m
```

1-hour run (SIGINT via timeout):
```bash
timeout --signal=INT 1h uv run pm_arb capture --run-id run-1h
```

Post-run audits (set `RUN_DIR` to the printed run dir):
```bash
RUN_DIR="data/runs/<run_id>"
uv run pm_arb capture-verify --run-dir "$RUN_DIR" --summary-only
uv run pm_arb capture-audit --run-dir "$RUN_DIR"
uv run pm_arb capture-latency-report --run-dir "$RUN_DIR"
```

## 6. Data quality gates vs latency tuning
FAIL gates (any one is a failure):
- Heartbeat gaps over 2s (`capture-audit` with default threshold reports `gaps_over_threshold > 0`).
- Reconnects midrun (`capture-latency-report` shows `reconnects.total > 0` or `disconnected.midrun.incidents > 0`).
- Drops (`capture-latency-report` shows `drops.dropped_messages_total > 0`).
- CRC mismatch (`capture-verify` shows `totals.crc_mismatch > 0`).
- `fatal.json` exists in the run directory.

Loop lag alone is not automatic data loss; interpret loop lag alongside drop counters, queue depth, and reconnects.

## 7. Latency metrics interpretation
Primary focus:
- `ws_rx_to_applied_ms` p50/p95/p99/max from `latency_report.json`.
- Queue depth stats: `ws_in_q_depth`, `parse_q_depth`, `write_q_depth`.
- Drop counters: `drops.dropped_messages_total` and per-interval counts.

What "bad" looks like:
- Rising p95/p99/max together with growing queue depth.
- Any sustained queue growth paired with non-zero drops or reconnects.

## 8. Universe refresh behavior (current behavior)
When enabled, refresh periodically:
- Fetches Gamma, recomputes the active-binary universe, and computes deltas.
- Only shards with changed token sets reconnect; reconnects are staggered.
- New tokens are excluded from coverage calculations until the grace window elapses.

Runlog records to watch:
- `universe_refresh` includes `duration_ms`, `reason`, `added_count`, `removed_count`,
  `churn_pct`, `interval_seconds_used`, `gamma_pages_fetched`, `markets_seen`,
  and `tokens_selected`.
- `shard_refresh_begin` and `shard_refresh_applied` bracket shard-level updates.
- `universe_refresh_error` includes `duration_ms`, `error`, and `message`.

Churn guard behavior:
- If churn exceeds `capture_universe_refresh_max_churn_pct`, the effective interval backs off
  (up to `capture_universe_refresh_max_interval_seconds`) and may become fatal after
  `capture_universe_refresh_churn_guard_consecutive_fatal` consecutive guards.

## 9. Troubleshooting
- uvloop install/verify: `uv pip install uvloop`, then `uv run python -c "import uvloop"`.
- Missing files in `capture-verify`: confirm the run dir path and that `capture/*.frames` exists.
- Fatal runs: inspect `fatal.json`, `missing_tokens.json`, and `runlog.ndjson`.
- Metrics and reports live under `metrics/` and `latency_report.json`/`.txt`.
- Run bundles are under `data/runs/<run_id>/` unless `PM_ARB_DATA_DIR` overrides `data_dir`.
