# AGENTS.md (authoritative)

This repo is a Phase 1 capture plant. It records raw Polymarket CLOB WS frames for the active-binary YES/NO universe and stores them in reproducible run bundles. The documentation front door is [docs/MASTER.md](docs/MASTER.md).

## Non-negotiables

- Do not claim or imply VPS access; you only have local access.
- Do not claim or imply git commits; provide diffs/edits only.
- Avoid shell pipelines; run sequential commands with visible console output.

## Scope

In scope:
- Capture raw WS payload bytes with per-frame `rx_mono_ns` and `rx_wall_ns_utc` (ns integers).
- Produce run bundles under `data/runs/<run_id>/` with manifest, frames, idx, runlog, and metrics.
- Universe inspection via `pm_arb discover`.
- Offline bench, verify, inspect, audit, latency report, and slice tools.

Out of scope:
- Trading or execution behavior (orders, signing, wallets, keys).
- Decision logic, modeling, or evaluation logic.
- Any transform that discards or rewrites WS payload bytes.

Capture defaults (documented in [docs/MASTER.md](docs/MASTER.md)):
`PM_ARB_CAPTURE_UNIVERSE_REFRESH_ENABLE=true`, `PM_ARB_WS_PING_INTERVAL_SECONDS=10`,
`PM_ARB_WS_PING_TIMEOUT_SECONDS=10`.

## Contracts

- CLI entrypoint: `pm_arb = "pm_arb.cli:main"`.
- Capture commands must remain: `capture`, `capture-verify`, `capture-bench`, `capture-inspect`,
  `capture-audit`, `capture-latency-report`, `capture-slice`.
- Capture run bundle layout is stable and must not change.
- Payload bytes are written exactly as received with provenance flags.
- Default universe is active-binary and capped by `capture_max_markets`.

## Hot Path Guardrails

- No per-frame file open/close.
- No per-frame disk usage checks.
- No unbounded queues between receive and write.
- Avoid heavy parsing in the receive loop.

## Capture quality fixes (required local checks)

When asked to fix capture quality, run locally:
```bash
uv run python -m pytest
timeout --signal=INT 10m uv run pm_arb capture --run-id local-10m
```

Then audit the run (set `RUN_DIR` to the printed path):
```bash
RUN_DIR="data/runs/<run_id>"
uv run pm_arb capture-verify --run-dir "$RUN_DIR" --summary-only
uv run pm_arb capture-audit --run-dir "$RUN_DIR"
uv run pm_arb capture-latency-report --run-dir "$RUN_DIR"
```

If 10 minutes is not feasible, run as long as feasible and note the duration.

## Workflow

After editing, include:
- PLAN
- BEHAVIOR CHANGES
- ROLLBACK PLAN
- Commands run for verification and pass/fail results.
- Any new tests added and what they assert.

Keep diffs tight; do not change unrelated code.

## Gates

- `make test` (fallback: `uv run python -m pytest`).
- `uv run pm_arb capture-bench --offline --fixtures-dir testdata/fixtures`.
