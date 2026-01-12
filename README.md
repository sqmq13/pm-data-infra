# pm-arb-py

Capture + runtime stack for Polymarket CLOB data.
Phase 1 records raw WS frames into run bundles; Phase 2 streams live or replay data into a deterministic runtime (normalize -> state -> strategy -> allocator -> sim execution). The single source of truth is [docs/MASTER.md](docs/MASTER.md).

## Quickstart (uv)
```bash
uv sync
uv run pm_arb --help
```

Optional uvloop install (non-Windows) and verification:
```bash
uv pip install uvloop
uv run python -c "import uvloop"
```

## Capture (recording, disk output)
Start capture (stop after ~10 minutes with Ctrl-C):
```bash
uv run pm_arb capture --run-id run-10m
```

Post-run audit:
```bash
RUN_DIR="data/runs/<run_id>"
uv run pm_arb capture-verify --run-dir "$RUN_DIR" --summary-only
uv run pm_arb capture-audit --run-dir "$RUN_DIR"
uv run pm_arb capture-latency-report --run-dir "$RUN_DIR"
```

## Runtime replay sim (no disk output)
Replay an existing run bundle, streaming frames into the runtime:
```bash
uv run pm_arb run --mode replay --execution sim --run-dir data/runs/<RUN_ID> --max-seconds 300 --print-hash
```

Optional PnL output (sim fills, conservative rules):
```bash
uv run pm_arb run --mode replay --execution sim --run-dir data/runs/<RUN_ID> --max-seconds 300 --print-hash --print-pnl --strategy toy_spread
```

## Runtime live sim dry run (no disk output)
Live streaming into sim execution (strategies disabled unless explicitly enabled):
```bash
uv run pm_arb run --mode live --execution sim --duration-seconds 300 --print-summary-json
```

## Determinism auditing (stable JSON)
For replay determinism checks, use `--stable-json` to make the output line byte-identical across runs.

```bash
RUN_DIR="data/runs/<RUN_ID>"
OUT1="$(uv run pm_arb run --mode replay --execution sim --run-dir "$RUN_DIR" --max-seconds 300 --print-hash --print-pnl --strategy toy_spread --stable-json)"
OUT2="$(uv run pm_arb run --mode replay --execution sim --run-dir "$RUN_DIR" --max-seconds 300 --print-hash --print-pnl --strategy toy_spread --stable-json)"
test "$OUT1" = "$OUT2" && echo PASS || echo FAIL
```

## Notes
- No live execution backend; simulation only.
- Hot path uses fixed-point e6 integers (no floats) for price/size.
- Strategies are transport-agnostic and must be explicitly enabled via `--strategy`.
- Live runtime does not write to `data/runs/*`.
