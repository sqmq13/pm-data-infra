# AGENTS.md (authoritative)

This repo is a Phase 1 capture plant. It records raw Polymarket CLOB WS frames for the active-binary YES/NO universe and stores them in reproducible run bundles.

## Scope

In scope:
- Capture raw WS payload bytes with per-frame `rx_mono_ns` and `rx_wall_ns_utc` (ns integers).
- Produce run bundles under `data/runs/<run_id>/` with manifest, frames, idx, runlog, and metrics.
- Universe inspection via `pm_arb discover`.
- Offline bench, verify, inspect, and slice tools.

Out of scope:
- Trading or execution behavior (orders, signing, wallets, keys).
- Decision logic, modeling, or evaluation logic.
- Any transform that discards or rewrites WS payload bytes.

## Contracts

- CLI entrypoint: `pm_arb = "pm_arb.cli:main"`.
- Capture commands must remain: `capture`, `capture-verify`, `capture-bench`, `capture-inspect`, `capture-slice`.
- Capture run bundle layout is stable and must not change.
- Payload bytes are written exactly as received with provenance flags.
- Default universe is active-binary and capped by `capture_max_markets`.

## Hot Path Guardrails

- No per-frame file open/close.
- No per-frame disk usage checks.
- No unbounded queues between receive and write.
- Avoid heavy parsing in the receive loop.

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
