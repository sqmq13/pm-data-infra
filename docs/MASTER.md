# MASTER

## 1. Scope / system overview
- Phase 1 capture plant for Polymarket CLOB WS frames (active-binary YES/NO).
- Phase 2 runtime pipeline that streams live or replay data into strategies with sim execution only.
- Strategy layer is transport-agnostic: it sees the same canonical event stream for live or replay.
- No live execution backend; simulation only.

Run bundle layout (online capture):
- `manifest.json` (run metadata, config snapshot, universe metadata).
- `runlog.ndjson` (heartbeats, reconnects, refresh records, lifecycle events).
- `capture/shard_<NN>.frames` and `capture/shard_<NN>.idx`.
- `metrics/global.ndjson` and `metrics/shard_<NN>.ndjson`.
- `latency_report.json` and `latency_report.txt` (created by `capture-latency-report`).
- On fatal runs: `fatal.json`, `missing_tokens.json`, `last_frames_shard_<NN>.bin`.

Runtime run outputs:
- No disk output by default for replay or live.
- Determinism hashes and optional PnL summaries are emitted to stdout as a single JSON line.

## 2. Runtime architecture (streaming)
Pipeline:
1) DataSource: `LiveDataSource` or `ReplayDataSource`.
2) Normalizer: raw payload bytes -> canonical events (deltas only).
3) StateStore: in-memory `MarketState` and `GlobalState`.
4) Strategy API: callbacks emit intents (no WS/capture/execution imports).
5) Allocator: deterministic merge and limits.
6) SimExecutionBackend: ack + conservative fills for taker intents only.
7) Ledger: fixed-point PnL, conservative mark.

Strategies are disabled by default and must be explicitly enabled via `--strategy`.

## 3. Canonical events + fixed-point e6
Canonical event (hot path):
```
TopOfBookUpdate:
  market_id
  bid_px_e6, bid_sz_e6, ask_px_e6, ask_sz_e6
  ts_event (exchange) or None
  ts_recv (local monotone)
  seq (assigned by orchestrator)
```
All price/size values are fixed-point e6 integers; no floats on the hot path.
`to_e6` uses round-half-up quantization in the normalizer.

ToB extraction rule (hybrid):
- For bids: if the list appears non-decreasing by price (sampled checks), take the last level as best bid; otherwise scan for the max-price valid level.
- For asks: if the list appears non-increasing by price (sampled checks), take the last level as best ask; otherwise scan for the min-price valid level.

## 4. Determinism rules
- Replay merge ordering across shards is `(rx_mono_ns, shard_id, idx_i)`.
- Canonical event seq is assigned by the orchestrator in processing order.
- Intent hash format (one line per intent) is defined in `pm_data/runtime/entrypoint.py`:
  `seq|strategy_id|intent_type|market_id|side|price_e6|size_e6|tif|urgency|tag`.
- Execution ack events are excluded from the hash.

Stable output:
- Use `--stable-json` for replay output to make the JSON line byte-identical across runs.
- Stable output keeps the same keys but forces `elapsed_ms` to a constant value and canonicalizes nested dict key ordering.

## 5. CLI inventory
Capture:
- `discover`
- `capture`
- `capture-verify`
- `capture-bench`
- `capture-inspect`
- `capture-audit`
- `capture-latency-report`
- `capture-slice`

Runtime:
- `run` (replay or live, sim execution only)

## 6. Runtime commands (no disk output)
Replay sim (prints hash, optional PnL):
```bash
uv run pm_data run --mode replay --execution sim --run-dir data/runs/<RUN_ID> --max-seconds 300 --print-hash
uv run pm_data run --mode replay --execution sim --run-dir data/runs/<RUN_ID> --max-seconds 300 --print-hash --print-pnl --strategy toy_spread
```

Live sim dry run (prints summary JSON):
```bash
uv run pm_data run --mode live --execution sim --duration-seconds 300 --print-summary-json
```

Live sim latency report (prints summary JSON + aggregated stage histograms):
```bash
uv run pm_data run --mode live --execution sim --duration-seconds 600 --print-summary-json --print-latency-report-json --strategy toy_spread
```

Notes:
- Replay and live runtime do not write to `data/runs/*`.
- `--max-events` caps canonical events (not raw frames).
- Strategies are enabled explicitly via `--strategy`.

## 7. Determinism audit snippet (macOS + Ubuntu)
```bash
RUN_DIR="data/runs/<RUN_ID>"
OUT1="$(uv run pm_data run --mode replay --execution sim --run-dir "$RUN_DIR" --max-seconds 300 --print-hash --print-pnl --strategy toy_spread --stable-json)"
OUT2="$(uv run pm_data run --mode replay --execution sim --run-dir "$RUN_DIR" --max-seconds 300 --print-hash --print-pnl --strategy toy_spread --stable-json)"
test "$OUT1" = "$OUT2" && echo PASS || echo FAIL
```

## 8. Sim execution + ledger (conservative)
- Taker intents fill against current ToB with a slippage buffer (bps) and optional size cap.
- Maker intents do not fill by default.
- Fees use a `FeeModel` hook (default flat bps).
- Ledger is fixed-point; marks are conservative (bid for longs, ask for shorts).

## 9. Capture configuration (Phase 1)
Configuration comes from the `Config` dataclass in `pm_data/config.py`.

Workflow:
- Defaults are the baseline capture profile; override via env/CLI only when needed.
- CLI flags use kebab-case (`--ws-shards`, `--capture-max-markets`).
- Boolean flags use `--<name>` and `--no-<name>`, except `--offline` which accepts `--offline` / `--no-offline`.
- Environment variables use `PM_DATA_` + uppercase field name (example: `PM_DATA_WS_SHARDS`).
- Environment variables override CLI overrides when both are set.

Key settings and defaults:
| Setting | Env var | CLI flag | Default | Notes |
| --- | --- | --- | --- | --- |
| Shard count | `PM_DATA_WS_SHARDS` | `--ws-shards` | `8` | Multi-shard WS capture. |
| Ping interval | `PM_DATA_WS_PING_INTERVAL_SECONDS` | `--ws-ping-interval-seconds` | `10.0` | Keepalive cadence. |
| Ping timeout | `PM_DATA_WS_PING_TIMEOUT_SECONDS` | `--ws-ping-timeout-seconds` | `10.0` | Ping response deadline. |
| Data-idle reconnect | `PM_DATA_WS_DATA_IDLE_RECONNECT_SECONDS` | `--ws-data-idle-reconnect-seconds` | `120.0` | Force reconnect if no data. |
| Confirm timeout | `PM_DATA_CAPTURE_CONFIRM_TIMEOUT_SECONDS` | `--capture-confirm-timeout-seconds` | `45.0` | Startup confirm window. |
| Confirm min events | `PM_DATA_CAPTURE_CONFIRM_MIN_EVENTS` | `--capture-confirm-min-events` | `1` | Minimum events to confirm. |
| Universe refresh enable | `PM_DATA_CAPTURE_UNIVERSE_REFRESH_ENABLE` | `--capture-universe-refresh-enable` | `true` | Enabled by default. |
| Universe refresh interval | `PM_DATA_CAPTURE_UNIVERSE_REFRESH_INTERVAL_SECONDS` | `--capture-universe-refresh-interval-seconds` | `60.0` | Base refresh cadence. |
| Max markets safety cap | `PM_DATA_CAPTURE_MAX_MARKETS` | `--capture-max-markets` | `2000` | Upper bound on active-binary universe. |
| Metrics sample cap | `PM_DATA_CAPTURE_METRICS_MAX_SAMPLES` | `--capture-metrics-max-samples` | `1024` | Caps per-metric sample deques (affects heartbeat quantiles cost). |
| Disk usage check interval | `PM_DATA_CAPTURE_DISK_USAGE_INTERVAL_SECONDS` | `--capture-disk-usage-interval-seconds` | `10.0` | Heartbeat disk free check cadence (performed off-thread). |
| Coverage metrics interval | `PM_DATA_CAPTURE_COVERAGE_METRICS_INTERVAL_SECONDS` | `--capture-coverage-metrics-interval-seconds` | `5.0` | Throttles expensive coverage_pct calculations. |
| Windows high-res timer | `PM_DATA_CAPTURE_WINDOWS_HIGH_RES_TIMER_ENABLE` | `--capture-windows-high-res-timer-enable` | `true` | On Windows only: calls `timeBeginPeriod(1)` to reduce loop lag jitter. |
| Runtime Windows high-res timer | `PM_DATA_RUNTIME_WINDOWS_HIGH_RES_TIMER_ENABLE` | `--runtime-windows-high-res-timer-enable` | `true` | On Windows only: wraps the live runtime event loop with `timeBeginPeriod(1)`. |
| Data directory | `PM_DATA_DATA_DIR` | `--data-dir` | `./data` | Run bundle root. |

## 10. Capture operations playbook (Phase 1)
Capture run (stop after ~10 minutes with Ctrl-C):
```bash
uv run pm_data capture --run-id run-10m
```

Post-run audit:
```bash
RUN_DIR="data/runs/<run_id>"
uv run pm_data capture-verify --run-dir "$RUN_DIR" --summary-only
uv run pm_data capture-audit --run-dir "$RUN_DIR"
uv run pm_data capture-latency-report --run-dir "$RUN_DIR"
```

## 11. Data quality gates vs latency tuning (capture)
FAIL gates (any one is a failure):
- Heartbeat gaps over 2s (`capture-audit` with default threshold reports `gaps_over_threshold > 0`).
- Reconnects midrun (`capture-latency-report` shows `reconnects.total > 0` or `disconnected.midrun.incidents > 0`).
- Drops (`capture-latency-report` shows `drops.dropped_messages_total > 0`).
- Loop lag (`capture-latency-report` shows `loop_lag_ms.p99 > 10` or `loop_lag_ms.max > 100`).
- Sustained pressure streaks (`capture-latency-report` shows `pressure_streaks.failed == true`).
- CRC mismatch (`capture-verify` shows `totals.crc_mismatch > 0`).
- `fatal.json` exists in the run directory.

## 12. Gate status (completed)
- Gate 0: repo audit + placement plan.
- Gate 1: canonical events + state store tests.
- Gate 2: strategy API + intents tests.
- Gate 3: allocator + sim execution + orchestrator determinism tests.
- Gate 4: replay datasource + normalizer + deterministic hash tests.
- Gate 5: live datasource dry run + summary JSON output.
- Gate 6: conservative sim fills + deterministic PnL.

## 13. Platform notes
- Development is on Windows; runtime targets Ubuntu.
- Commands in this doc are copy/pasteable on macOS and Ubuntu.
- Live runtime does not write to disk; capture does.
