# AGENTS.md — pm-arb-py (Phase 1 Scanner)

This repository is a **Phase-1 data collection and measurement system** for Polymarket “Strategy A” edge windows. It is **not** a trading bot. It must produce defensible metrics in 12–24 hours without fragile assumptions.

If you are an automated coding agent: treat this file as the authoritative scope and acceptance criteria.

## Scope

### What this repo builds
A Python CLI `pm_arb` that can:

- `scan` (online): discover eligible markets, ingest orderbooks via WS/REST, compute Strategy-A edge windows, log NDJSON.
- `scan --offline`: replay fixtures deterministically (development/test).
- `contract-test` (online/offline): validate WS subscription + book decoding + REST snapshot parsing + basic reconciliation sanity.
- `discover`: print the top candidate binary markets and show filter match reasons.
- `report`: aggregate NDJSON into `summary.json` and `summary.csv`.

### What this repo does NOT build
- No order placement, no signing, no wallet management, no execution logic.
- No geoblock / Cloudflare bypassing. Geoblock is **detection only**.

## Strategy A Definition (the only “strategy”)

A market is eligible if it has exactly **two** `clobTokenIds` (binary).

For each size `N`:
- `costA(N)` = sweep-cost to buy `N` shares of token A from asks
- `costB(N)` = sweep-cost to buy `N` shares of token B from asks
- Edge condition:  
  `costA(N) + costB(N) <= N * 1.00 - N * buffer_per_share`

The scanner must measure **edge windows**: continuous periods where the condition stays true, with:
- `duration_ms` (monotonic time)
- `best_edge_per_share` observed
- churn counters
- `end_reason`

## Endpoints (defaults; must be configurable)

- Gamma: `https://gamma-api.polymarket.com`
- CLOB REST: `https://clob.polymarket.com`
- CLOB WS: `wss://ws-subscriptions-clob.polymarket.com/ws/`
- Geoblock (detection only): `https://polymarket.com/api/geoblock`

## Non-negotiable Guardrails (avoid fake results)

### 1) No tick logging to disk
The scanner must **not** write every WS message to NDJSON. NDJSON is event-level:
- run header
- heartbeat (e.g., every 5s)
- alarms
- market poll summaries
- window start/end
- reconciliation results
- coverage dumps (debug)

Only exception: WS schema capture is capped to `ws_sample_capture_n` messages.

### 2) Log rotation + retention + disk kill switch
- Rotate `events.ndjson` by size (e.g., `log_rotate_mb`)
- Keep at most `log_rotate_keep` rotated files
- Per-day directories `data/logs/YYYY-MM-DD/`
- If free disk `< min_free_disk_gb`: emit alarm + taint + **exit non-zero cleanly** (better than corrupting output).

### 3) Taint propagation + alarms
If any of these occur, set `tainted=true`, emit ALARM, and ensure windows cannot look “good”:
- WS disconnect longer than `ws_disconnect_alarm_seconds`
- initial WS parse failures > threshold in first 100 messages
- coverage low for multiple polls
- reconciliation mismatch persists and marks tokens desynced
- disk pressure alarm

Taint must be visible in:
- heartbeats
- window_end records (if ended due to integrity)
- report outputs (taint_fraction, clean runtime)

### 4) Staleness termination
If either token in a market is stale (no update for `stale_seconds`), end active windows immediately with `end_reason=token_stale`.

### 5) Persist-based reconciliation (avoid false desync)
Do not mark a token desynced on a single mismatch. Require persistence:
- mismatch beyond tolerance must persist `reconcile_mismatch_persist_n` consecutive reconciliations
- allow tick tolerance and relative tolerance (time skew is real)

### 6) WS subscribe fallback + NO_FEED detection
Subscription payloads drift. Implement subscribe fallback variants and fail loud in `contract-test` if none work.

If a subscribed token receives **zero** `book` updates within 30 seconds:
- mark token state `NO_FEED`
- alarm + exclude from computations

## sizes=auto (must be precise and frozen)

If `sizes="auto"`:
- Warm-up for `sizes_auto_warmup_minutes`
- Collect per token: `max_fillable_shares_within_top_k`
- For each time sample and each market pair, compute `m(t) = min(maxA(t), maxB(t))`
- Aggregate `m(t)` across pairs and warm-up times and choose sizes as percentiles (e.g., p50/p75/p90), plus a small baseline if needed.
- Freeze sizes after warm-up and log them in `run_header`.

Offline mode must produce deterministic auto sizes from fixtures.

## Outputs

### NDJSON
Primary log file: `data/logs/YYYY-MM-DD/events.ndjson` (+ rotated)

All records include:
- `schema_version`
- `record_type`
- `run_id`
- `ts_wall` (RFC3339)
- `ts_mono_ns`
- `tainted`

Record types (minimum):
- `run_header`
- `heartbeat`
- `alarm`
- `market_poll_summary`
- `window_start`
- `window_end`
- `reconcile_result`
- `coverage_dump` (debug / when coverage low)

### WS schema capture
`data/debug/ws_samples.jsonl` containing only the first N raw WS messages (capped).

### Report
- `data/reports/summary.json`
- `data/reports/summary.csv`

Must include:
- windows/hour by size
- duration percentiles p50/p95/p99 by size
- edge percentiles p50/p95/p99 by size (cents/share)
- end_reason distributions
- taint_fraction and alarms
- ws downtime, desync/no_feed counts
- auto sizing summary (warm-up + chosen sizes)

## Offline mode and Fixtures (must exercise failures)

Fixtures must include:
- clean WS book messages
- extra/unknown fields
- malformed price/size strings (must taint + alarm without crashing)
- out-of-order timestamps (must increment ooo drops)
- Gamma markets fixture
- REST book fixture

Offline `scan --offline` must replay a mix to ensure taint paths work.

## Commands and Make Targets

Make targets (must exist):
- `make setup`
- `make test`
- `make run` (online scan)
- `make report` (latest logs)
- `make contract-test` (online)
- `make discover` (online)
- `make run-offline`
- `make report-offline`

All commands must exit non-zero on failure.

## Acceptance Checklist (must pass on fresh Ubuntu)

- `make setup`
- `make test`
- `make run-offline && make report-offline`
- `pm_arb contract-test --offline` passes
- Online `make contract-test` should pass when network access is available

## Development Rules for Agents

- Do not introduce live trading code.
- Do not “paper over” schema issues by swallowing exceptions silently.
- Prefer explicit ALARM + taint + exclusion over optimistic assumptions.
- Keep the hot path integer-based (no float arithmetic in sweep/cost logic).
- Keep logs bounded; never write per-tick messages.

If anything here conflicts with other docs, AGENTS.md wins for Phase 1.
