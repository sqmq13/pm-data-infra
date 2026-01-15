# AGENTS.md (authoritative)

This repo covers Phase 1 capture and Phase 2 runtime. Capture records raw Polymarket CLOB WS frames into run bundles; runtime streams live or replay data into strategies with sim execution only. The documentation front door is [docs/MASTER.md](docs/MASTER.md).

## Non-negotiables
- Do not claim or imply VPS access; you only have local access.
- Do not claim or imply git commits; provide diffs/edits only.
- Avoid shell pipelines; run sequential commands with visible console output.
- Do not break capture integrity or alter run bundle format.
- Do not introduce floats into hot-path contracts (use fixed-point e6 ints).
- Do not add disk coupling to runtime live mode (no `data/runs/*` writes).
- Do not add any live execution backend (simulation only).
- Do not change hashing inputs/format without updating tests and docs.

## Scope

In scope:
- Capture raw WS payload bytes with per-frame `rx_mono_ns` and `rx_wall_ns_utc` (ns integers).
- Produce run bundles under `data/runs/<run_id>/` with manifest, frames, idx, runlog, and metrics.
- Universe inspection via `pm_data discover`.
- Offline bench, verify, inspect, audit, latency report, and slice tools.
- Runtime pipeline: live/replay streaming -> normalize -> state -> strategy -> allocator -> sim execution.
- Determinism audits (final_hash and optional PnL), stable JSON output for replay.

Out of scope:
- Trading or execution behavior (orders, signing, wallets, keys).
- Decision logic, modeling, or evaluation logic beyond the defined strategy API.
- Any transform that discards or rewrites WS payload bytes.

Capture defaults (documented in [docs/MASTER.md](docs/MASTER.md)):
`PM_DATA_CAPTURE_UNIVERSE_REFRESH_ENABLE=true`, `PM_DATA_WS_PING_INTERVAL_SECONDS=10`,
`PM_DATA_WS_PING_TIMEOUT_SECONDS=10`.

## Contracts
- CLI entrypoint: `pm_data = "pm_data.cli:main"`.
- Capture commands must remain: `capture`, `capture-verify`, `capture-bench`, `capture-inspect`,
  `capture-audit`, `capture-latency-report`, `capture-slice`.
- Runtime command must remain: `run` with `--mode replay|live` and `--execution sim`.
- Capture run bundle layout is stable and must not change.
- Payload bytes are written exactly as received with provenance flags.
- Default universe is active-binary and capped by `capture_max_markets`.
- Replay ordering tie-breaker is `(rx_mono_ns, shard_id, idx_i)` for deterministic merges.
- Hash line format is defined in `pm_data/runtime/entrypoint.py` and must not drift silently.

## Hot Path Guardrails
- No per-frame file open/close.
- No per-frame disk usage checks.
- No unbounded queues between receive and write.
- Avoid heavy parsing in the receive loop.
- Runtime strategies consume streaming events; no batch materialization on the hot path.

## Capture quality fixes (required local checks)

When asked to fix capture quality, run locally:
```bash
uv run python -m pytest
timeout --signal=INT 10m uv run pm_data capture --run-id local-10m
```

Then audit the run (set `RUN_DIR` to the printed path):
```bash
RUN_DIR="data/runs/<run_id>"
uv run pm_data capture-verify --run-dir "$RUN_DIR" --summary-only
uv run pm_data capture-audit --run-dir "$RUN_DIR"
uv run pm_data capture-latency-report --run-dir "$RUN_DIR"
```

If 10 minutes is not feasible, run as long as feasible and note the duration.

## Workflow
Before edits:
- Survey the repo and confirm current behavior; do not assume.
- Keep diffs tight; do not change unrelated code.

During edits:
- Work gate-by-gate; do not proceed to the next gate until current gate tests pass.

After editing, include:
- PLAN
- BEHAVIOR CHANGES
- ROLLBACK PLAN
- Commands run for verification and pass/fail results
- Any new tests added and what they assert

## Gates
- `make test` (fallback: `uv run python -m pytest`).
- `uv run pm_data capture-bench --offline --fixtures-dir testdata/fixtures`.
- When touching runtime determinism, hashes, or output formatting, add/update replay
  determinism and stable-output tests.
