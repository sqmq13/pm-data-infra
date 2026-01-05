# pm-arb-py Phase 1 Capture Plant

This repository records raw Polymarket CLOB WS frames for the active-binary YES/NO universe. It stores each frame as received, with per-frame monotonic and UTC wall timestamps (ns integers), to support deterministic replay in later phases.

## Universe Default

By default, capture selects active binary markets that:
- are marked active (when the field is present),
- do not disable depth updates (when the field is present),
- have exactly two outcome token ids.

The selection order is preserved and then truncated to the configured max market count.

## Sharding

- Tokens are assigned to shards by stable token id hash.
- A given token id maps to exactly one shard for the run.
- Shard assignment is deterministic and stable within the run.

## Universe Refresh

Universe refresh is disabled by default. When enabled, capture recomputes the desired universe on a fixed interval and applies only deltas:
- tokens that no longer qualify are pruned,
- newly qualifying tokens are added.

Only shards whose target token sets changed reconnect, and reconnects are staggered. Newly added tokens are excluded from coverage calculations for the grace window (or until first seen). The churn guard backs off refresh frequency and can fail the run after sustained churn.

Refresh disabled (baseline):
```bash
$env:PM_ARB_CAPTURE_UNIVERSE_REFRESH_ENABLE = "false"
pm_arb capture --run-id baseline-<id>
```

Refresh enabled (60s interval, 30s grace):
```bash
$env:PM_ARB_CAPTURE_UNIVERSE_REFRESH_ENABLE = "true"
$env:PM_ARB_CAPTURE_UNIVERSE_REFRESH_INTERVAL_SECONDS = "60"
$env:PM_ARB_CAPTURE_UNIVERSE_REFRESH_GRACE_SECONDS = "30"
pm_arb capture --run-id refresh-<id>
```

## Run Bundle Layout

All files live under `data/runs/<run_id>/`:
- `manifest.json`
- `runlog.ndjson`
- `capture/shard_<NN>.frames`
- `capture/shard_<NN>.idx`
- `metrics/global.ndjson`
- `metrics/shard_<NN>.ndjson`

On fatal runs:
- `fatal.json`
- `missing_tokens.json`
- `last_frames_shard_<NN>.bin`

## Verify, Inspect, Slice

Verify a run:
```bash
pm_arb capture-verify --run-dir data/runs/<run_id>
```

Inspect a run:
```bash
pm_arb capture-inspect --run-dir data/runs/<run_id>
```

Slice a window by monotonic time:
```bash
pm_arb capture-slice --run-dir data/runs/<run_id> --start-mono-ns <ns> --end-mono-ns <ns>
```

Slice a window by byte offsets:
```bash
pm_arb capture-slice --run-dir data/runs/<run_id> --start-offset <bytes> --end-offset <bytes>
```

## Timing Precision

- `rx_mono_ns` uses a high-resolution monotonic clock.
- Capture fails if the monotonic clock is quantized to 1ms resolution.

## Offline Bench

Run the offline ingest bench on fixtures:
```bash
pm_arb capture-bench --offline --fixtures-dir testdata/fixtures
```

Scale the workload:
```bash
pm_arb capture-bench --offline --fixtures-dir testdata/fixtures --multiplier 200
```

## Failure Modes and Forensics

When a run fails, use these files to diagnose:
- `fatal.json` for the failure reason and recent heartbeats.
- `missing_tokens.json` for shard confirmation state and a capped missing set.
- `runlog.ndjson` for reconnects and other lifecycle events.
- `last_frames_shard_<NN>.bin` for raw recent headers per shard.
