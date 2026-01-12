# pm-arb-py

Phase 1 capture plant for Polymarket CLOB WS traffic: multi-shard WS capture, stable run artifacts, integrity verification, audit, and latency reporting. The single source of truth is [docs/MASTER.md](docs/MASTER.md).

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

Capture defaults (details in [docs/MASTER.md](docs/MASTER.md)):
`PM_ARB_CAPTURE_UNIVERSE_REFRESH_ENABLE=true`, `PM_ARB_WS_PING_INTERVAL_SECONDS=10`, `PM_ARB_WS_PING_TIMEOUT_SECONDS=10`.

## Run + Audit (10m / 1h)
Full operations guidance lives in [docs/MASTER.md](docs/MASTER.md).

10-minute run:
```bash
timeout --signal=INT 10m uv run pm_arb capture --run-id run-10m
```

1-hour run:
```bash
timeout --signal=INT 1h uv run pm_arb capture --run-id run-1h
```

Post-run audit:
```bash
RUN_DIR="data/runs/<run_id>"
uv run pm_arb capture-verify --run-dir "$RUN_DIR" --summary-only
uv run pm_arb capture-audit --run-dir "$RUN_DIR"
uv run pm_arb capture-latency-report --run-dir "$RUN_DIR"
```

## Notes
- Run capture under tmux so the terminal stays responsive.
- See [docs/MASTER.md](docs/MASTER.md) for configuration, run artifacts, and troubleshooting.
