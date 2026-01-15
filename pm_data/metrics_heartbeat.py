import asyncio
import shutil
import time
from typing import Any

from .capture import monotonic_ns
from .capture_state import (
    CaptureState,
    _quantile_from_float_samples,
    _quantiles_from_samples,
    _quantiles_from_samples_any,
    _quantiles_with_mean,
)
from .fatal_policy import (
    FATAL_LOW_DISK,
    _check_backpressure_fatal,
    _coverage_pct,
    _trigger_fatal,
)


def _write_runlog(state: CaptureState, record: dict[str, Any]) -> None:
    from . import capture_online as _capture_online

    _capture_online._write_runlog(state, record)


def _write_metrics(state: CaptureState, path, record: dict[str, Any]) -> None:
    from . import capture_online as _capture_online

    _capture_online._write_metrics(state, path, record)


async def _loop_lag_monitor(state: CaptureState) -> None:
    interval_ns = 100_000_000
    next_tick = monotonic_ns() + interval_ns
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        now_ns = monotonic_ns()
        sleep_ns = next_tick - now_ns
        if sleep_ns > 0:
            await asyncio.sleep(sleep_ns / 1_000_000_000)
        now_ns = monotonic_ns()
        lag_ns = max(0, now_ns - next_tick)
        state.loop_lag_samples_ms.append(lag_ns / 1_000_000.0)
        next_tick += interval_ns
        if next_tick < now_ns:
            next_tick = now_ns + interval_ns



async def _heartbeat_loop(state: CaptureState) -> None:
    interval_ns = int(state.config.capture_heartbeat_interval_seconds * 1_000_000_000)
    run_dir = state.run.run_dir
    grace_ns = int(state.config.capture_universe_refresh_grace_seconds * 1_000_000_000)
    metrics_global = run_dir / "metrics" / "global.ndjson"
    metrics_shard_paths = {
        shard.shard_id: run_dir / "metrics" / f"shard_{shard.shard_id:02d}.ndjson"
        for shard in state.shards
    }
    next_tick = state.run.t0_mono_ns + interval_ns
    last_yield_ns = monotonic_ns()
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        now_ns = monotonic_ns()
        if now_ns < next_tick:
            await asyncio.sleep((next_tick - now_ns) / 1_000_000_000)
            continue
        now_ns = monotonic_ns()
        hb_wall_ns_utc = time.time_ns()
        elapsed_ns = now_ns - state.run.t0_mono_ns
        _write_runlog(
            state,
            {
                "record_type": "heartbeat",
                "run_id": state.run.run_id,
                "hb_wall_ns_utc": hb_wall_ns_utc,
                "hb_mono_ns": now_ns,
            },
        )

        rx_idle_values: list[float] = []
        midrun_disconnected_time_s_total = state.midrun_disconnected_time_s_total
        midrun_disconnected_incidents = state.midrun_disconnected_incidents
        midrun_active = 0
        midrun_durations: list[float] = list(state.midrun_disconnected_durations_s)
        startup_connect_samples: list[float] = list(
            state.startup_connect_time_s_samples
        )
        startup_pending_shards = 0
        for shard in state.shards:
            if shard.last_rx_mono_ns:
                idle_s = max(
                    0.0, (now_ns - shard.last_rx_mono_ns) / 1_000_000_000.0
                )
                rx_idle_values.append(idle_s)
            if not shard.ready_once:
                startup_pending_shards += 1
                startup_connect_samples.append(
                    max(0.0, (now_ns - state.run.t0_mono_ns) / 1_000_000_000.0)
                )
            if shard.disconnected_since_mono_ns is not None and shard.ready_once:
                midrun_active += 1
                duration_s = (
                    now_ns - shard.disconnected_since_mono_ns
                ) / 1_000_000_000.0
                midrun_disconnected_time_s_total += duration_s
                midrun_durations.append(duration_s)
        midrun_incidents_total = midrun_disconnected_incidents + midrun_active
        midrun_incident_duration_s_p95 = _quantile_from_float_samples(
            midrun_durations, 95
        )
        midrun_incident_duration_s_max = (
            max(midrun_durations) if midrun_durations else 0.0
        )
        startup_connect_time_s_p50 = _quantile_from_float_samples(
            startup_connect_samples, 50
        )
        startup_connect_time_s_p95 = _quantile_from_float_samples(
            startup_connect_samples, 95
        )
        startup_connect_time_s_max = (
            max(startup_connect_samples) if startup_connect_samples else 0.0
        )
        rx_idle_any_shard_s = min(rx_idle_values) if rx_idle_values else 0.0
        rx_idle_all_shards_s = max(rx_idle_values) if rx_idle_values else 0.0

        global_frames = 0
        global_bytes = 0
        global_write_samples: list[int] = []
        global_ingest_samples: list[int] = []
        global_backpressure_samples: list[int] = []
        global_decode_errors = 0
        global_msg_type_counts: dict[str, int] = {}
        global_reconnects = 0
        global_confirm_failures = 0
        global_unconfirmed_count = 0
        global_tokens_seen: set[str] = set()
        global_coverage_pct = 0.0

        for shard in state.shards:
            shard_stats = shard.stats
            global_frames += shard_stats.frames
            global_bytes += shard_stats.bytes_written
            global_decode_errors += shard_stats.decode_errors
            global_reconnects += shard.reconnects
            global_confirm_failures += shard.confirm_failures
            if shard.confirm_deadline_mono_ns is not None and not shard.confirmed:
                global_unconfirmed_count += 1
            global_tokens_seen.update(shard_stats.token_ids)
            global_write_samples.extend(shard_stats.write_durations_ns)
            global_ingest_samples.extend(shard_stats.ingest_latencies_ns)
            global_backpressure_samples.extend(shard_stats.backpressure_ns)
            for key, value in shard_stats.msg_type_counts.items():
                global_msg_type_counts[key] = global_msg_type_counts.get(key, 0) + value

            shard_rx_idle_s = None
            if shard.last_rx_mono_ns:
                shard_rx_idle_s = max(
                    0.0, (now_ns - shard.last_rx_mono_ns) / 1_000_000_000.0
                )
            shard_startup_connect_s = (
                shard.startup_connect_time_s
                if shard.ready_once
                else max(0.0, (now_ns - state.run.t0_mono_ns) / 1_000_000_000.0)
            )
            shard_midrun_disconnected_s = shard.midrun_disconnected_time_s_total
            shard_midrun_incidents = shard.midrun_disconnected_incidents
            if shard.disconnected_since_mono_ns is not None and shard.ready_once:
                shard_midrun_disconnected_s += (
                    now_ns - shard.disconnected_since_mono_ns
                ) / 1_000_000_000.0
                shard_midrun_incidents += 1

            shard_coverage_pct = _coverage_pct(
                shard.token_ids,
                shard.last_seen,
                now_ns,
                None,
                token_added_mono_ns=state.universe.token_added_mono_ns,
                grace_ns=grace_ns,
            )
            write_quantiles = _quantiles_from_samples(
                shard_stats.write_durations_ns, (50, 95, 99)
            )
            ingest_quantiles = _quantiles_from_samples(
                shard_stats.ingest_latencies_ns, (50, 95, 99)
            )
            backpressure_quantiles = _quantiles_from_samples(
                shard_stats.backpressure_ns, (50, 95, 99)
            )
            if shard.confirm_deadline_mono_ns is None:
                deadline_remaining_ms = -1
            else:
                deadline_remaining_ms = int(
                    (shard.confirm_deadline_mono_ns - now_ns) / 1_000_000
                )
            shard_record = {
                "record_type": "heartbeat",
                "run_id": state.run.run_id,
                "shard_id": shard.shard_id,
                "hb_wall_ns_utc": hb_wall_ns_utc,
                "hb_mono_ns": now_ns,
                "elapsed_ns": elapsed_ns,
                "frames": shard_stats.frames,
                "bytes_written": shard_stats.bytes_written,
                "msgs_per_sec": shard_stats.frames
                / max(elapsed_ns / 1_000_000_000.0, 1e-9),
                "bytes_per_sec": shard_stats.bytes_written
                / max(elapsed_ns / 1_000_000_000.0, 1e-9),
                "write_ns_p50": write_quantiles[50],
                "write_ns_p95": write_quantiles[95],
                "write_ns_p99": write_quantiles[99],
                "ingest_ns_p50": ingest_quantiles[50],
                "ingest_ns_p95": ingest_quantiles[95],
                "ingest_ns_p99": ingest_quantiles[99],
                "backpressure_ns_p50": backpressure_quantiles[50],
                "backpressure_ns_p95": backpressure_quantiles[95],
                "backpressure_ns_p99": backpressure_quantiles[99],
                "coverage_pct": shard_coverage_pct,
                "token_ids_seen": len(shard_stats.token_ids),
                "token_ids_assigned": len(shard.token_ids),
                "reconnects": shard.reconnects,
                "confirm_failures": shard.confirm_failures,
                "confirmed": shard.confirmed,
                "confirm_deadline_remaining_ms": deadline_remaining_ms,
                "confirm_events_seen": shard.confirm_events_seen,
                "decode_errors": shard_stats.decode_errors,
                "msg_type_counts": shard_stats.msg_type_counts,
                "rx_idle_s": shard_rx_idle_s,
                "startup_connect_time_s": shard_startup_connect_s,
                "midrun_disconnected_time_s_total": shard_midrun_disconnected_s,
                "midrun_disconnected_incidents": shard_midrun_incidents,
            }
            _write_metrics(state, metrics_shard_paths[shard.shard_id], shard_record)
            now_ns = monotonic_ns()
            if now_ns - last_yield_ns >= 5_000_000:
                last_yield_ns = now_ns
                await asyncio.sleep(0)

        if state.pinned_tokens:
            last_seen_global: dict[str, int] = {}
            for shard in state.shards:
                last_seen_global.update(shard.last_seen)
            global_coverage_pct = _coverage_pct(
                state.pinned_tokens,
                last_seen_global,
                now_ns,
                None,
                token_added_mono_ns=state.universe.token_added_mono_ns,
                grace_ns=grace_ns,
            )

        fee_rate_stats = (
            state.fee_rate_client.stats_snapshot() if state.fee_rate_client else {}
        )
        parse_stats = (
            state.parse_worker.stats() if state.parse_worker is not None else {}
        )
        frame_write_stats = (
            state.frame_writer.stats() if state.frame_writer is not None else {}
        )
        enqueue_samples = list(state.ws_rx_to_enqueue_ms_samples)
        parsed_samples = list(state.ws_rx_to_parsed_ms_samples)
        applied_samples = list(state.ws_rx_to_applied_ms_samples)
        write_end_to_apply_samples = list(state.write_end_to_apply_ms_samples)
        apply_update_samples = list(state.apply_update_ms_samples)
        ws_in_q_samples = list(state.ws_in_q_depth_samples)
        parse_q_samples = list(state.parse_q_depth_samples)
        write_q_samples = list(state.write_q_depth_samples)
        enqueue_stats = _quantiles_with_mean(enqueue_samples)
        parsed_stats = _quantiles_with_mean(parsed_samples)
        applied_stats = _quantiles_with_mean(applied_samples)
        write_end_to_apply_stats = _quantiles_with_mean(write_end_to_apply_samples)
        apply_update_stats = _quantiles_with_mean(apply_update_samples)
        ws_in_q_quantiles = _quantiles_from_samples_any(ws_in_q_samples, (95,))
        parse_q_quantiles = _quantiles_from_samples_any(parse_q_samples, (95,))
        write_q_quantiles = _quantiles_from_samples_any(write_q_samples, (95,))
        enqueue_max = enqueue_stats["max"]
        parsed_max = parsed_stats["max"]
        applied_max = applied_stats["max"]
        write_end_to_apply_max = write_end_to_apply_stats["max"]
        apply_update_max = apply_update_stats["max"]
        ws_in_q_max = max(ws_in_q_samples) if ws_in_q_samples else 0
        parse_q_max = max(parse_q_samples) if parse_q_samples else 0
        write_q_max = max(write_q_samples) if write_q_samples else 0
        loop_lag_samples = list(state.loop_lag_samples_ms)
        loop_lag_ms_max_last_interval = (
            max(loop_lag_samples) if loop_lag_samples else 0.0
        )
        state.loop_lag_samples_ms.clear()
        state.ws_rx_to_enqueue_ms_samples.clear()
        state.ws_rx_to_parsed_ms_samples.clear()
        state.ws_rx_to_applied_ms_samples.clear()
        state.write_end_to_apply_ms_samples.clear()
        state.apply_update_ms_samples.clear()
        state.ws_in_q_depth_samples.clear()
        state.parse_q_depth_samples.clear()
        state.write_q_depth_samples.clear()
        parse_dropped = parse_stats.get("dropped", 0)
        parse_results_dropped = parse_stats.get("results_dropped", 0)
        frame_write_dropped = frame_write_stats.get("dropped", 0)
        frame_write_results_dropped = frame_write_stats.get("results_dropped", 0)
        dropped_messages_total = (
            parse_dropped
            + parse_results_dropped
            + frame_write_dropped
            + frame_write_results_dropped
        )
        dropped_messages_count = max(
            0, dropped_messages_total - state.dropped_messages_total_last
        )
        state.dropped_messages_total_last = dropped_messages_total
        loss_budget = state.loss_budget
        if parse_dropped > loss_budget.parse_queue_drops.total:
            loss_budget.parse_queue_drops.total = parse_dropped
        if parse_results_dropped > loss_budget.parse_results_drops.total:
            loss_budget.parse_results_drops.total = parse_results_dropped
        loss_total = (
            loss_budget.parse_queue_drops.total
            + loss_budget.parse_results_drops.total
            + loss_budget.metrics_drops.total
            + loss_budget.runlog_enqueue_timeouts.total
            + loss_budget.frame_write_queue_pressure_events.total
        )
        global_write_quantiles = _quantiles_from_samples(global_write_samples, (50, 95, 99))
        global_ingest_quantiles = _quantiles_from_samples(
            global_ingest_samples, (50, 95, 99)
        )
        global_backpressure_quantiles = _quantiles_from_samples(
            global_backpressure_samples, (50, 95, 99)
        )
        metrics_writer_stats = (
            state.metrics_writer.stats() if state.metrics_writer is not None else {}
        )
        global_record = {
            "record_type": "heartbeat",
            "run_id": state.run.run_id,
            "hb_wall_ns_utc": hb_wall_ns_utc,
            "hb_mono_ns": now_ns,
            "elapsed_ns": elapsed_ns,
            "frames": global_frames,
            "bytes_written": global_bytes,
            "msgs_per_sec": global_frames / max(elapsed_ns / 1_000_000_000.0, 1e-9),
            "bytes_per_sec": global_bytes / max(elapsed_ns / 1_000_000_000.0, 1e-9),
            "write_ns_p50": global_write_quantiles[50],
            "write_ns_p95": global_write_quantiles[95],
            "write_ns_p99": global_write_quantiles[99],
            "ingest_ns_p50": global_ingest_quantiles[50],
            "ingest_ns_p95": global_ingest_quantiles[95],
            "ingest_ns_p99": global_ingest_quantiles[99],
            "backpressure_ns_p50": global_backpressure_quantiles[50],
            "backpressure_ns_p95": global_backpressure_quantiles[95],
            "backpressure_ns_p99": global_backpressure_quantiles[99],
            "coverage_pct": global_coverage_pct,
            "token_ids_seen": len(global_tokens_seen),
            "token_ids_assigned": len(state.pinned_tokens),
            "reconnects": global_reconnects,
            "confirm_failures": global_confirm_failures,
            "confirm_failures_total": global_confirm_failures,
            "shards_unconfirmed_count": global_unconfirmed_count,
            "decode_errors": global_decode_errors,
            "msg_type_counts": global_msg_type_counts,
            "universe_version": state.universe.universe_version,
            "refresh_count": state.universe.refresh_count,
            "refresh_churn_pct_last": state.universe.refresh_churn_pct_last,
            "tokens_added_last": state.universe.tokens_added_last,
            "tokens_removed_last": state.universe.tokens_removed_last,
            "shards_refreshed_last": state.universe.shards_refreshed_last,
            "refresh_failures": state.universe.refresh_failures,
            "refresh_interval_seconds_current": state.universe.effective_refresh_interval_seconds,
            "refresh_skipped_delta_below_min_count": state.universe.refresh_skipped_delta_below_min_count,
            "refresh_last_decision_reason": state.universe.refresh_last_decision_reason,
            "refresh_inflight_skipped_count": state.universe.refresh_inflight_skipped_count,
            "universe_refresh_duration_ms": state.universe.refresh_duration_ms_last,
            "universe_refresh_worker_duration_ms": state.universe.refresh_worker_duration_ms_last,
            "gamma_pages_fetched": state.universe.refresh_gamma_pages_fetched_last,
            "markets_seen": state.universe.refresh_markets_seen_last,
            "tokens_selected": state.universe.refresh_tokens_selected_last,
            "expected_churn_bool": state.universe.expected_churn_last,
            "expected_churn_reason": state.universe.expected_churn_reason_last,
            "rollover_window_hit": state.universe.expected_churn_rollover_hit_last,
            "expiry_window_hit": state.universe.expected_churn_expiry_hit_last,
            "loop_lag_ms_max_last_interval": loop_lag_ms_max_last_interval,
            "ws_rx_to_enqueue_ms_mean": enqueue_stats["mean"],
            "ws_rx_to_enqueue_ms_p50": enqueue_stats["p50"],
            "ws_rx_to_enqueue_ms_p95": enqueue_stats["p95"],
            "ws_rx_to_enqueue_ms_p99": enqueue_stats["p99"],
            "ws_rx_to_enqueue_ms_max": enqueue_max,
            "ws_rx_to_enqueue_ms_count": len(enqueue_samples),
            "ws_rx_to_parsed_ms_mean": parsed_stats["mean"],
            "ws_rx_to_parsed_ms_p50": parsed_stats["p50"],
            "ws_rx_to_parsed_ms_p95": parsed_stats["p95"],
            "ws_rx_to_parsed_ms_p99": parsed_stats["p99"],
            "ws_rx_to_parsed_ms_max": parsed_max,
            "ws_rx_to_parsed_ms_count": len(parsed_samples),
            "ws_rx_to_applied_ms_mean": applied_stats["mean"],
            "ws_rx_to_applied_ms_p50": applied_stats["p50"],
            "ws_rx_to_applied_ms_p95": applied_stats["p95"],
            "ws_rx_to_applied_ms_p99": applied_stats["p99"],
            "ws_rx_to_applied_ms_max": applied_max,
            "ws_rx_to_applied_ms_count": len(applied_samples),
            "write_end_to_apply_ms_mean": write_end_to_apply_stats["mean"],
            "write_end_to_apply_ms_p50": write_end_to_apply_stats["p50"],
            "write_end_to_apply_ms_p95": write_end_to_apply_stats["p95"],
            "write_end_to_apply_ms_p99": write_end_to_apply_stats["p99"],
            "write_end_to_apply_ms_max": write_end_to_apply_max,
            "write_end_to_apply_ms_count": len(write_end_to_apply_samples),
            "apply_update_ms_mean": apply_update_stats["mean"],
            "apply_update_ms_p50": apply_update_stats["p50"],
            "apply_update_ms_p95": apply_update_stats["p95"],
            "apply_update_ms_p99": apply_update_stats["p99"],
            "apply_update_ms_max": apply_update_max,
            "apply_update_ms_count": len(apply_update_samples),
            "ws_in_q_depth_p95": ws_in_q_quantiles[95],
            "ws_in_q_depth_max": ws_in_q_max,
            "parse_q_depth_p95": parse_q_quantiles[95],
            "parse_q_depth_max": parse_q_max,
            "write_q_depth_p95": write_q_quantiles[95],
            "write_q_depth_max": write_q_max,
            "dropped_messages_count": dropped_messages_count,
            "dropped_messages_total": dropped_messages_total,
            "rx_idle_any_shard_s": rx_idle_any_shard_s,
            "rx_idle_all_shards_s": rx_idle_all_shards_s,
            "startup_connect_time_s_p50": startup_connect_time_s_p50,
            "startup_connect_time_s_p95": startup_connect_time_s_p95,
            "startup_connect_time_s_max": startup_connect_time_s_max,
            "startup_connect_pending_shards": startup_pending_shards,
            "midrun_disconnected_time_s_total": midrun_disconnected_time_s_total,
            "midrun_disconnected_incidents": midrun_incidents_total,
            "midrun_disconnected_incident_duration_s_p95": midrun_incident_duration_s_p95,
            "midrun_disconnected_incident_duration_s_max": midrun_incident_duration_s_max,
            "fee_rate_cache_hits": fee_rate_stats.get("cache_hits", 0),
            "fee_rate_cache_misses": fee_rate_stats.get("cache_misses", 0),
            "fee_rate_unknown_count": state.universe.fee_rate_unknown_count_last,
            "fee_regime_state": state.universe.fee_regime_state,
            "circuit_breaker_reason": state.universe.fee_regime_reason,
            "sampled_tokens_count": state.universe.fee_regime_sampled_tokens_count,
            "policy_counts": state.universe.policy_counts_last,
            "loss_total": loss_total,
            "loss_parse_queue_drops": loss_budget.parse_queue_drops.total,
            "loss_parse_results_drops": loss_budget.parse_results_drops.total,
            "loss_metrics_drops": loss_budget.metrics_drops.total,
            "loss_runlog_enqueue_timeouts": loss_budget.runlog_enqueue_timeouts.total,
            "loss_frame_write_queue_pressure_events": (
                loss_budget.frame_write_queue_pressure_events.total
            ),
            "metrics_writer_queue_size": metrics_writer_stats.get("queue_size", 0),
            "metrics_writer_errors": metrics_writer_stats.get("errors", 0),
            "parse_queue_size": parse_stats.get("queue_size", 0),
            "parse_results_queue_size": parse_stats.get("results_size", 0),
            "parse_dropped": loss_budget.parse_queue_drops.total,
            "parse_worker_dropped": parse_dropped,
            "parse_results_dropped": parse_results_dropped,
            "parse_processed": parse_stats.get("processed", 0),
            "frame_write_queue_size": frame_write_stats.get("queue_size", 0),
            "frame_write_results_queue_size": frame_write_stats.get("results_size", 0),
            "frame_write_dropped": loss_budget.frame_write_queue_pressure_events.total,
            "frame_write_worker_dropped": frame_write_dropped,
            "frame_write_results_dropped": frame_write_results_dropped,
            "frame_write_processed": frame_write_stats.get("processed", 0),
        }
        _write_metrics(state, metrics_global, global_record)
        state.heartbeat_samples.append(global_record)

        await _check_backpressure_fatal(
            state,
            now_ns,
            global_record["backpressure_ns_p99"],
        )
        if state.fatal_event.is_set() or state.stop_event.is_set():
            break

        if state.config.min_free_disk_gb is not None:
            usage = shutil.disk_usage(run_dir)
            free_gb = usage.free / (1024**3)
            if free_gb < state.config.min_free_disk_gb:
                await _trigger_fatal(
                    state,
                    FATAL_LOW_DISK,
                    f"free disk below {state.config.min_free_disk_gb} GB",
                )
                break

        next_tick = now_ns + interval_ns



