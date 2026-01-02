# pm-arb-py - Phase 1 Strategy-A Scanner

Phase-1 data collection and measurement only. This repository **does not** place orders, sign transactions, manage wallets, or bypass restrictions. Geoblock checks are **detection-only** and may fail.

## Quick Start

Offline (fixtures):
```bash
make setup
make run-offline
make report-offline
```

Online (requires network access):
```bash
make run
make report
```

## Outputs

- NDJSON logs: `data/logs/YYYY-MM-DD/events.ndjson` (+ rotated)
- WS schema capture: `data/debug/ws_samples.jsonl` (first N messages)
- Reports: `data/reports/summary.json` and `data/reports/summary.csv`

## Interpreting `summary.json` (go / no-go)

Focus on:
- `taint_fraction`: high values indicate integrity issues (no-go).
- `by_size.*.windows_per_hour` and `edge_p95_cents`: consistent, positive edge with low taint is needed.
- `end_reasons`: heavy `token_stale`, `token_no_feed`, or `reconcile_failed` is a no-go.
- `alarms`: any persistent `ws_disconnected`, `rest_unavailable`, or `parse_taint` should halt use.

## Coverage Troubleshooting

If coverage is low (alarms or frequent `coverage_low_taint`):
- Adjust `market_regex` via env var `PM_ARB_MARKET_REGEX` or `--market-regex`.
- Example:
```bash
PM_ARB_MARKET_REGEX="(?i)\\b5\\b.*(min|minute).*(BTC|ETH|SOL)" pm_arb discover
```

## Commands

- `pm_arb scan` (online) / `pm_arb scan --offline`
- `pm_arb report`
- `pm_arb contract-test` (online/offline)
- `pm_arb discover`

All commands exit non-zero on failure.

## Configuration

All config flags have env overrides (ENV wins). Example:
```bash
PM_ARB_SIZES="10,50,100" PM_ARB_BUFFER_PER_SHARE="0.004" pm_arb scan --offline
```

## Guardrails

- Never write per-tick WS messages to NDJSON (event-level only).
- Disk guardrail: exit non-zero if free disk is below `min_free_disk_gb`.
- Taint propagation on integrity failures; windows must not look “good”.
- Fixed-point integer math for sweep/cost logic.

See `AGENTS.md` for the authoritative Phase-1 scope and acceptance criteria.
