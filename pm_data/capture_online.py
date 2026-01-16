from __future__ import annotations

import asyncio
import contextlib
import functools
import hashlib
import gc
import os
import platform
import shutil
import signal
import socket
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from math import gcd, ceil
from collections import deque
from statistics import mean
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable

import requests
import websockets

from .capture import RunBootstrap, bootstrap_run, monotonic_ns
from .capture_format import (
    append_record,
    frames_header_len,
    idx_entry_len,
)
from .capture_offline import quantile
from .config import Config
from .fees import (
    FeeRateClient,
    FeeRegimeMonitor,
    FeeRegimeState,
    parse_expected_fee_rate_bps_values,
)
from .gamma import (
    UniverseSnapshot,
    compute_desired_universe,
    fetch_markets,
    parse_clob_token_ids,
    select_active_binary_markets,
)
from .policy import PolicySelector, parse_policy_rules, policy_counts
from .capture_state import (
    CaptureState,
    RefreshPlan,
    ShardState,
    ShardTarget,
    ShardTargetPlan,
    UniverseState,
    _append_sample,
    _append_sample_float,
    _quantile_from_samples,
)
from .fatal_policy import (
    FATAL_CHURN_GUARD,
    FATAL_FRAME_WRITER,
    FATAL_INTERNAL,
    FATAL_MONO_QUANTIZED,
    _check_backpressure_fatal,
    _coverage_pct,
    _eligible_token_ids,
    _trigger_fatal,
    _write_metrics,
    _write_runlog,
    _write_startup_fatal,
    _writer_error_payload,
)
from .metrics_heartbeat import _heartbeat_loop, _loop_lag_monitor
from .parse_pipeline import _ParseWorker, _extract_minimal_fields, _parse_results_loop
from .segments import SegmentTag, build_segment_maps
from .ws_primitives import split_subscribe_groups
from .writers_frames import _FrameWriter
from .writers_ndjson import _NdjsonWriter
from .ws_runner import (
    _apply_shard_refresh,
    _handle_confirm_deadline_miss,
    _handle_payload,
    _mark_reconnecting,
    _refresh_requested,
    _run_shard,
    _wait_for_refresh_or_fatal,
)



def _apply_churn_guard_policy(
    current_interval: float,
    baseline_interval: float,
    max_interval: float,
    *,
    guard_triggered: bool,
    guard_count: int,
    fatal_threshold: int,
) -> tuple[float, int, bool]:
    if guard_triggered:
        base = current_interval if current_interval > 0 else baseline_interval
        next_interval = min(max_interval, base * 2)
        next_guard_count = guard_count + 1
        return next_interval, next_guard_count, next_guard_count >= fatal_threshold
    return baseline_interval, 0, False


@dataclass(frozen=True)
class ExpectedChurnSummary:
    expected_churn: bool
    reason: str
    rollover_window_hit: bool
    expiry_window_hit: bool
    expected_count: int
    total_count: int


def _cadence_window_seconds(config: Config, cadence_bucket: str) -> float:
    if cadence_bucket == "5m":
        return config.capture_expected_churn_window_seconds_5m
    if cadence_bucket == "15m":
        return config.capture_expected_churn_window_seconds_15m
    if cadence_bucket == "30m":
        return config.capture_expected_churn_window_seconds_30m
    if cadence_bucket == "60m":
        return config.capture_expected_churn_window_seconds_60m
    return 0.0


def _is_within_cadence_window(
    now_utc: datetime,
    cadence_bucket: str,
    window_seconds: float,
) -> bool:
    if window_seconds <= 0:
        return False
    interval_seconds = {
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "60m": 3600,
    }.get(cadence_bucket)
    if interval_seconds is None:
        return False
    seconds_since_hour = (
        now_utc.minute * 60
        + now_utc.second
        + (now_utc.microsecond / 1_000_000.0)
    )
    offset = seconds_since_hour % interval_seconds
    distance = min(offset, interval_seconds - offset)
    return distance <= window_seconds


def _classify_expected_churn(
    now_utc: datetime,
    segment: SegmentTag,
    config: Config,
) -> tuple[bool, str, bool, bool]:
    expiry_window_ns = int(config.capture_expected_churn_expiry_window_seconds * 1_000_000_000)
    end_ns = segment.end_wall_ns_utc
    if end_ns is not None and expiry_window_ns > 0:
        now_ns = int(now_utc.timestamp() * 1_000_000_000)
        if abs(now_ns - end_ns) <= expiry_window_ns:
            return True, "EXPECTED_EXPIRY", False, True
    cadence_bucket = segment.cadence_bucket
    window_seconds = _cadence_window_seconds(config, cadence_bucket)
    if cadence_bucket != "unknown" and _is_within_cadence_window(
        now_utc, cadence_bucket, window_seconds
    ):
        reason = "EXPECTED_ROLLOVER_1H" if cadence_bucket == "60m" else "EXPECTED_ROLLOVER_15M"
        return True, reason, True, False
    return False, "UNKNOWN", False, False


def _expected_churn_for_refresh(
    now_wall_ns_utc: int,
    removed_market_ids: set[str],
    added_market_ids: set[str],
    current_segments: dict[str, SegmentTag],
    next_segments: dict[str, SegmentTag],
    config: Config,
) -> ExpectedChurnSummary:
    if not config.capture_expected_churn_enable:
        return ExpectedChurnSummary(
            expected_churn=False,
            reason="UNKNOWN",
            rollover_window_hit=False,
            expiry_window_hit=False,
            expected_count=0,
            total_count=0,
        )
    now_utc = datetime.fromtimestamp(now_wall_ns_utc / 1_000_000_000, tz=timezone.utc)
    expected_count = 0
    total_count = 0
    reasons: dict[str, int] = {}
    rollover_hit = False
    expiry_hit = False
    for market_id in removed_market_ids:
        segment = current_segments.get(str(market_id))
        if segment is None:
            continue
        expected, reason, roll_hit, exp_hit = _classify_expected_churn(
            now_utc, segment, config
        )
        total_count += 1
        if expected:
            expected_count += 1
            reasons[reason] = reasons.get(reason, 0) + 1
        rollover_hit = rollover_hit or roll_hit
        expiry_hit = expiry_hit or exp_hit
    for market_id in added_market_ids:
        segment = next_segments.get(str(market_id))
        if segment is None:
            continue
        expected, reason, roll_hit, exp_hit = _classify_expected_churn(
            now_utc, segment, config
        )
        total_count += 1
        if expected:
            expected_count += 1
            reasons[reason] = reasons.get(reason, 0) + 1
        rollover_hit = rollover_hit or roll_hit
        expiry_hit = expiry_hit or exp_hit
    expected_churn = False
    if total_count > 0:
        expected_churn = (expected_count / total_count) >= config.capture_expected_churn_min_ratio
    reason = "UNKNOWN"
    if expected_churn and reasons:
        reason = max(reasons.items(), key=lambda item: (item[1], item[0]))[0]
    return ExpectedChurnSummary(
        expected_churn=expected_churn,
        reason=reason,
        rollover_window_hit=rollover_hit,
        expiry_window_hit=expiry_hit,
        expected_count=expected_count,
        total_count=total_count,
    )


def _monotonic_precision_stats(sample_count: int) -> dict[str, Any]:
    if sample_count < 2:
        raise ValueError("sample_count must be >= 2")
    stamps = [monotonic_ns() for _ in range(sample_count)]
    deltas = [
        later - earlier
        for earlier, later in zip(stamps, stamps[1:])
        if later > earlier
    ]
    any_non_ms = any(stamp % 1_000_000 != 0 for stamp in stamps)
    stats: dict[str, Any] = {
        "sample_count": sample_count,
        "any_non_ms": any_non_ms,
    }
    if not deltas:
        stats["ok"] = False
        stats["reason"] = "no_positive_deltas"
        return stats
    delta_gcd = deltas[0]
    for delta in deltas[1:]:
        delta_gcd = gcd(delta_gcd, delta)
    stats.update(
        {
            "delta_gcd_ns": delta_gcd,
            "min_delta_ns": min(deltas),
            "max_delta_ns": max(deltas),
        }
    )
    stats["ok"] = any_non_ms and delta_gcd < 1_000_000
    if not stats["ok"]:
        stats["reason"] = "quantized"
    return stats


def _config_snapshot(config: Config) -> dict[str, Any]:
    snapshot = {}
    for field in fields(Config):
        snapshot[field.name] = getattr(config, field.name)
    return snapshot


def _snapshot_for_manifest(snapshot: UniverseSnapshot) -> dict[str, Any]:
    payload = asdict(snapshot)
    payload.pop("selected_markets", None)
    return payload


def _read_git_commit() -> str:
    git_dir = Path.cwd() / ".git"
    head_path = git_dir / "HEAD"
    if not head_path.exists():
        return "unknown"
    head = head_path.read_text(encoding="utf-8").strip()
    if head.startswith("ref:"):
        ref = head.split(" ", 1)[1].strip()
        ref_path = git_dir / ref
        if ref_path.exists():
            return ref_path.read_text(encoding="utf-8").strip()
    return head or "unknown"


def _environment_metadata() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "platform": platform.platform(),
        "hostname": socket.gethostname(),
    }


def _install_signal_handlers(state: CaptureState) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop(sig: signal.Signals) -> None:
        if state.stop_event.is_set():
            return
        state.stop_reason = sig.name
        loop.call_soon_threadsafe(state.stop_event.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop, sig)
        except (NotImplementedError, RuntimeError):
            try:
                signal.signal(sig, lambda *_args, _sig=sig: _request_stop(_sig))
            except (ValueError, AttributeError):
                continue


def _build_fee_rate_client(config: Config) -> FeeRateClient:
    return FeeRateClient(
        base_url=config.fee_rate_base_url,
        timeout_seconds=config.fee_rate_timeout_seconds,
        cache_ttl_seconds=config.fee_rate_cache_ttl_seconds,
        max_in_flight=config.fee_rate_max_in_flight,
    )


def _build_policy_selector(config: Config) -> PolicySelector:
    policy_map = parse_policy_rules(config.policy_rules_json, config.policy_default_id)
    return PolicySelector(policy_map)


def _build_fee_regime_monitor(config: Config) -> FeeRegimeMonitor:
    expected_values = parse_expected_fee_rate_bps_values(
        config.fee_regime_expected_fee_rate_bps_values
    )
    return FeeRegimeMonitor(
        expect_15m_crypto_fee_enabled=config.fee_regime_expect_15m_crypto_fee_enabled,
        expect_other_fee_free=config.fee_regime_expect_other_fee_free,
        expect_unknown_fee_free=config.fee_regime_expect_unknown_fee_free,
        expected_fee_rate_bps_values=expected_values,
    )


def _parse_canary_tokens(raw: str) -> list[str]:
    if not raw:
        return []
    return [token.strip() for token in raw.split(",") if token.strip()]


def _segment_policy_summary(
    selector: PolicySelector,
    segments_by_token_id: dict[str, SegmentTag],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for segment in segments_by_token_id.values():
        segment_key = (
            segment.cadence_bucket,
            segment.is_crypto,
            segment.fee_enabled,
            segment.fee_rate_known,
        )
        policy_id = selector.select(segment)
        group_key = (segment_key, policy_id)
        entry = grouped.get(group_key)
        if entry is None:
            entry = {
                "segment_key": {
                    "cadence_bucket": segment.cadence_bucket,
                    "is_crypto": segment.is_crypto,
                    "fee_enabled": segment.fee_enabled,
                    "fee_rate_known": segment.fee_rate_known,
                },
                "policy_id": policy_id,
                "token_count": 0,
            }
            grouped[group_key] = entry
        entry["token_count"] += 1
    ordered = sorted(
        grouped.values(),
        key=lambda item: (
            item["policy_id"],
            str(item["segment_key"]["cadence_bucket"]),
            str(item["segment_key"]["is_crypto"]),
        ),
    )
    return ordered


def _stable_hash(token_id: str) -> int:
    digest = hashlib.sha256(token_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "little", signed=False)


def _coerce_numeric(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _token_score_map(markets: list[dict[str, Any]]) -> dict[str, float]:
    scores: dict[str, float] = {}
    numeric_keys = ("volume", "volumeNum", "liquidity", "liquidityNum")
    for market in markets:
        score: float | None = None
        for key in numeric_keys:
            score = _coerce_numeric(market.get(key))
            if score is not None:
                break
        if score is None:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_ids:
            current = scores.get(token_id)
            if current is None or score > current:
                scores[token_id] = score
    return scores


def _select_confirm_tokens(
    shard_tokens: list[str],
    scores: dict[str, float],
    max_tokens: int,
) -> list[str]:
    if not shard_tokens:
        return []
    scored: list[tuple[float, str]] = []
    for token_id in shard_tokens:
        score = scores.get(token_id)
        if score is not None:
            scored.append((score, token_id))
    if scored:
        scored.sort(key=lambda item: (-item[0], item[1]))
        ordered = [token_id for _, token_id in scored]
    else:
        ordered = sorted(shard_tokens, key=lambda token_id: (_stable_hash(token_id), token_id))
    return ordered[: min(max_tokens, len(ordered))]


def assign_shards_by_token(
    token_ids: list[str],
    shard_count: int,
) -> dict[int, list[str]]:
    if shard_count <= 0:
        raise ValueError("shard_count must be >= 1")
    shards: dict[int, list[str]] = {idx: [] for idx in range(shard_count)}
    seen: set[str] = set()
    for token_id in token_ids:
        token_key = str(token_id)
        if token_key in seen:
            continue
        seen.add(token_key)
        shard_id = _stable_hash(token_key) % shard_count
        shards[shard_id].append(token_key)
    return shards


def _sorted_unique_tokens(token_ids: Iterable[str]) -> list[str]:
    return sorted({str(token_id) for token_id in token_ids if str(token_id)})


def _compute_refresh_delta(
    current_tokens: set[str],
    desired_tokens: set[str],
) -> tuple[set[str], set[str]]:
    added = desired_tokens - current_tokens
    removed = current_tokens - desired_tokens
    return added, removed


def _should_apply_refresh(
    added: set[str],
    removed: set[str],
    min_delta_tokens: int,
) -> bool:
    return (len(added) + len(removed)) >= min_delta_tokens


def _build_shard_targets(
    token_ids: list[str],
    config: Config,
    *,
    target_version: int,
    scores: dict[str, float] | None = None,
) -> dict[int, ShardTarget]:
    plans = _build_shard_target_plans(
        token_ids,
        config,
        target_version=target_version,
        scores=scores,
    )
    return _materialize_shard_targets(plans)


def _build_shard_target_plans(
    token_ids: list[str],
    config: Config,
    *,
    target_version: int,
    scores: dict[str, float] | None = None,
) -> dict[int, ShardTargetPlan]:
    ordered_tokens = _sorted_unique_tokens(token_ids)
    shard_map = assign_shards_by_token(ordered_tokens, config.ws_shards)
    if scores is None:
        scores = {}
    targets: dict[int, ShardTargetPlan] = {}
    for shard_id, tokens in shard_map.items():
        groups = split_subscribe_groups(
            tokens,
            config.ws_subscribe_max_tokens,
            config.ws_subscribe_max_bytes,
            "A",
        )
        targets[shard_id] = ShardTargetPlan(
            token_ids=tokens,
            groups=groups,
            confirm_token_ids=_select_confirm_tokens(
                tokens,
                scores,
                config.capture_confirm_tokens_per_shard,
            ),
            target_version=target_version,
        )
    return targets


def _materialize_shard_targets(
    targets: dict[int, ShardTargetPlan],
) -> dict[int, ShardTarget]:
    materialized: dict[int, ShardTarget] = {}
    for shard_id, target in targets.items():
        materialized[shard_id] = ShardTarget(
            token_ids=target.token_ids,
            groups=target.groups,
            confirm_token_ids=target.confirm_token_ids,
            target_version=target.target_version,
        )
    return materialized


def _select_changed_shards(
    current_targets: dict[int, ShardTarget],
    next_targets: dict[int, ShardTarget],
) -> dict[int, ShardTarget]:
    changed: dict[int, ShardTarget] = {}
    for shard_id, next_target in next_targets.items():
        current = current_targets.get(shard_id)
        if current is None or set(current.token_ids) != set(next_target.token_ids):
            changed[shard_id] = next_target
    return changed


def _load_pinned_markets(
    config: Config,
) -> tuple[list[dict[str, Any]], list[str], str, str | None, list[dict[str, Any]]]:
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=config.capture_max_markets,
    )
    selected_markets = select_active_binary_markets(
        markets, max_markets=config.capture_max_markets
    )
    universe_mode = "active-binary"
    market_regex_effective: str | None = None
    selected: list[dict[str, Any]] = []
    tokens: set[str] = set()
    for market in selected_markets:
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        selected.append({"id": market.get("id"), "token_ids": [token_a, token_b]})
        tokens.add(token_a)
        tokens.add(token_b)
    return selected, sorted(tokens), universe_mode, market_regex_effective, selected_markets


def _snapshot_from_selected_markets(
    config: Config,
    selected_markets: list[dict[str, Any]],
    *,
    universe_version: int,
) -> UniverseSnapshot:
    market_ids: list[str] = []
    token_ids: list[str] = []
    token_seen: set[str] = set()
    for market in selected_markets:
        market_id = market.get("id")
        if market_id is None:
            continue
        market_ids.append(str(market_id))
        token_list = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_list:
            token_key = str(token_id)
            if token_key in token_seen:
                continue
            token_seen.add(token_key)
            token_ids.append(token_key)
    return UniverseSnapshot(
        universe_version=universe_version,
        market_ids=market_ids,
        token_ids=token_ids,
        created_wall_ns_utc=time.time_ns(),
        created_mono_ns=monotonic_ns(),
        selection={
            "max_markets": config.capture_max_markets,
        },
        selected_markets=selected_markets,
    )


def _load_startup_universe(
    config: Config,
) -> tuple[UniverseSnapshot, list[str], list[dict[str, Any]], list[dict[str, Any]], str]:
    snapshot = compute_desired_universe(config, universe_version=1)
    pinned_tokens = list(snapshot.token_ids)
    universe_mode = "active-binary"
    if snapshot.selected_markets is not None:
        selected_markets = list(snapshot.selected_markets)
    else:
        markets = fetch_markets(
            config.gamma_base_url,
            config.rest_timeout,
            limit=config.gamma_limit,
            max_markets=config.capture_max_markets,
        )
        selected_markets = select_active_binary_markets(
            markets,
            max_markets=config.capture_max_markets,
        )
    market_by_id = {
        str(market["id"]): market
        for market in selected_markets
        if market.get("id") is not None
    }
    pinned_markets: list[dict[str, Any]] = []
    ordered_selected_markets: list[dict[str, Any]] = []
    for market_id in snapshot.market_ids:
        market = market_by_id.get(str(market_id))
        if market is None:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        token_a, token_b = str(token_ids[0]), str(token_ids[1])
        pinned_markets.append({"id": market_id, "token_ids": [token_a, token_b]})
        ordered_selected_markets.append(market)
    return snapshot, pinned_tokens, pinned_markets, ordered_selected_markets, universe_mode


def _all_shards_confirmed(state: CaptureState) -> bool:
    return all(shard.confirmed for shard in state.shards)


async def _write_results_loop(state: CaptureState) -> None:
    writer = state.frame_writer
    if writer is None:
        return
    max_batch = 500
    sleep_seconds = 0.01
    while not state.fatal_event.is_set() and not state.stop_event.is_set():
        error = writer.error()
        if error is not None:
            await _trigger_fatal(
                state,
                FATAL_FRAME_WRITER,
                "frame writer failed",
                first_error={
                    "frame_writer_error": _writer_error_payload(
                        "frame_writer", error
                    )
                },
            )
            return
        processed = 0
        while processed < max_batch:
            result = writer.get_nowait()
            if result is None:
                break
            processed += 1
            t_applied_mono_ns = monotonic_ns()
            applied_latency_ms = max(
                0.0, (t_applied_mono_ns - result.rx_mono_ns) / 1_000_000.0
            )
            _append_sample_float(
                state.ws_rx_to_applied_ms_samples,
                applied_latency_ms,
                state.config.capture_metrics_max_samples,
            )
            write_end_to_apply_ms = max(
                0.0,
                (
                    t_applied_mono_ns
                    - (result.rx_mono_ns + result.ingest_latency_ns)
                )
                / 1_000_000.0,
            )
            _append_sample_float(
                state.write_end_to_apply_ms_samples,
                write_end_to_apply_ms,
                state.config.capture_metrics_max_samples,
            )
            shard = state.shards[result.shard_id]
            shard.stats.record(
                result.payload_len,
                frames_header_len(result.schema_version),
                idx_entry_len(result.schema_version),
                result.write_duration_ns,
                result.ingest_latency_ns,
                result.backpressure_ns,
                [],
                {},
                state.config.capture_metrics_max_samples,
            )
            shard.ring.append((result.header_bytes, result.payload_bytes))
            _append_sample(
                state.write_q_depth_samples,
                writer.results_size(),
                state.config.capture_metrics_max_samples,
            )
            apply_done_mono_ns = monotonic_ns()
            apply_update_ms = max(
                0.0, (apply_done_mono_ns - t_applied_mono_ns) / 1_000_000.0
            )
            _append_sample_float(
                state.apply_update_ms_samples,
                apply_update_ms,
                state.config.capture_metrics_max_samples,
            )
        if processed == 0:
            await asyncio.sleep(sleep_seconds)


def _compute_refresh_plan_sync(
    config: Config,
    *,
    universe_version: int,
    run_id: str,
    canary_tokens: list[str],
    session: requests.Session,
    loop: asyncio.AbstractEventLoop | None,
    fee_rate_client: FeeRateClient | None,
    policy_selector: PolicySelector | None,
    fee_regime_monitor: FeeRegimeMonitor | None,
    refresh_timeout_seconds: float,
) -> RefreshPlan:
    start_ns = time.perf_counter_ns()
    stats: dict[str, int] = {}
    snapshot = compute_desired_universe(
        config,
        universe_version=universe_version,
        session=session,
        stats=stats,
    )
    selected_markets = snapshot.selected_markets or []
    desired_tokens = {str(token_id) for token_id in snapshot.token_ids}
    desired_market_ids = {str(market_id) for market_id in snapshot.market_ids}
    fee_results: dict[str, Any] = {}
    sample_tokens: list[str] = []
    fee_regime_state = FeeRegimeState(
        state="OK",
        reason="DISABLED",
        sampled_tokens_count=0,
        unknown_fee_count=0,
        observed_fee_rates=[],
    )
    if config.fee_rate_enable and fee_rate_client and fee_regime_monitor:
        sample_tokens = fee_regime_monitor.sample_tokens(
            desired_tokens,
            run_id=run_id,
            universe_version=snapshot.universe_version,
            sample_size=config.fee_regime_sample_size_per_cycle,
            canary_token_ids=canary_tokens,
        )
        if loop is not None and not loop.is_closed():
            prefetch_timeout = config.fee_rate_refresh_timeout_seconds
            if refresh_timeout_seconds > 0:
                prefetch_timeout = min(prefetch_timeout, refresh_timeout_seconds)
            if sample_tokens and prefetch_timeout > 0:
                future = asyncio.run_coroutine_threadsafe(
                    fee_rate_client.prefetch_fee_rates(
                        sample_tokens,
                        timeout_seconds=prefetch_timeout,
                        max_tokens=config.fee_rate_prefetch_max_tokens,
                    ),
                    loop,
                )
                try:
                    future.result(timeout=prefetch_timeout)
                except FuturesTimeoutError:
                    with contextlib.suppress(Exception):
                        future.cancel()
                except Exception:
                    with contextlib.suppress(Exception):
                        future.cancel()
        fee_results = fee_rate_client.cached_fee_rates(desired_tokens)

    segment_by_token, segment_by_market = build_segment_maps(
        selected_markets,
        fee_results_by_token=fee_results,
    )
    if config.fee_rate_enable and fee_regime_monitor:
        fee_regime_state = fee_regime_monitor.evaluate(
            segment_by_token,
            token_ids_sampled=sample_tokens,
        )
    if policy_selector is not None:
        next_policy_counts = policy_counts(policy_selector, segment_by_token)
    else:
        next_policy_counts = {}
    scores = _token_score_map(selected_markets)
    ordered_tokens = _sorted_unique_tokens(snapshot.token_ids)
    shard_targets = _build_shard_target_plans(
        ordered_tokens,
        config,
        target_version=snapshot.universe_version,
        scores=scores,
    )
    worker_duration_ms = (time.perf_counter_ns() - start_ns) / 1_000_000.0
    return RefreshPlan(
        snapshot=snapshot,
        desired_token_ids=desired_tokens,
        desired_market_ids=desired_market_ids,
        ordered_tokens=ordered_tokens,
        shard_targets=shard_targets,
        segment_by_token_id=segment_by_token,
        segment_by_market_id=segment_by_market,
        policy_counts=next_policy_counts,
        fee_regime_state=fee_regime_state,
        fee_regime_sampled_tokens_count=fee_regime_state.sampled_tokens_count,
        fee_rate_unknown_count=fee_regime_state.unknown_fee_count,
        gamma_pages_fetched=stats.get("pages_fetched", 0),
        markets_seen=stats.get("markets_seen", len(selected_markets)),
        tokens_selected=len(desired_tokens),
        worker_duration_ms=worker_duration_ms,
    )


class UniverseRefreshWorker:
    def __init__(
        self,
        *,
        config: Config,
        loop: asyncio.AbstractEventLoop,
        fee_rate_client: FeeRateClient | None,
        policy_selector: PolicySelector | None,
        fee_regime_monitor: FeeRegimeMonitor | None,
        canary_tokens: list[str],
    ) -> None:
        self._config = config
        self._loop = loop
        self._fee_rate_client = fee_rate_client
        self._policy_selector = policy_selector
        self._fee_regime_monitor = fee_regime_monitor
        self._canary_tokens = list(canary_tokens)
        self._session: requests.Session | None = None

    def close(self) -> None:
        if self._session is not None:
            with contextlib.suppress(Exception):
                self._session.close()

    def compute_plan(
        self,
        *,
        universe_version: int,
        run_id: str,
        refresh_timeout_seconds: float,
    ) -> RefreshPlan:
        if self._session is None:
            self._session = requests.Session()
        return _compute_refresh_plan_sync(
            self._config,
            universe_version=universe_version,
            run_id=run_id,
            canary_tokens=self._canary_tokens,
            session=self._session,
            loop=self._loop,
            fee_rate_client=self._fee_rate_client,
            policy_selector=self._policy_selector,
            fee_regime_monitor=self._fee_regime_monitor,
            refresh_timeout_seconds=refresh_timeout_seconds,
        )


async def _refresh_loop(state: CaptureState) -> None:
    config = state.config
    baseline_interval = config.capture_universe_refresh_interval_seconds
    if baseline_interval <= 0:
        return
    if state.universe.effective_refresh_interval_seconds <= 0:
        state.universe.effective_refresh_interval_seconds = baseline_interval
    run_dir = state.run.run_dir
    if state.fee_rate_client is None and config.fee_rate_enable:
        state.fee_rate_client = _build_fee_rate_client(config)
    if state.policy_selector is None:
        state.policy_selector = _build_policy_selector(config)
    if state.fee_regime_monitor is None:
        state.fee_regime_monitor = _build_fee_regime_monitor(config)
    canary_tokens = _parse_canary_tokens(config.fee_regime_canary_token_ids)
    if state.refresh_executor is None:
        state.refresh_executor = ThreadPoolExecutor(
            max_workers=1,
            thread_name_prefix="universe-refresh",
        )
    if state.refresh_worker is None:
        state.refresh_worker = UniverseRefreshWorker(
            config=config,
            loop=asyncio.get_running_loop(),
            fee_rate_client=state.fee_rate_client,
            policy_selector=state.policy_selector,
            fee_regime_monitor=state.fee_regime_monitor,
            canary_tokens=canary_tokens,
        )
    refresh_timeout_seconds = config.capture_universe_refresh_timeout_seconds
    if refresh_timeout_seconds <= 0:
        refresh_timeout_seconds = max(baseline_interval, 1.0)
    try:
        while (
            not state.fatal_event.is_set()
            and not state.stop_event.is_set()
            and not state.universe.refresh_cancelled
        ):
            await asyncio.sleep(state.universe.effective_refresh_interval_seconds)
            if (
                state.fatal_event.is_set()
                or state.stop_event.is_set()
                or state.universe.refresh_cancelled
            ):
                break
            inflight = state.universe.refresh_inflight_future
            if inflight is not None:
                if not inflight.done():
                    state.universe.refresh_inflight_skipped_count += 1
                    state.universe.shards_refreshed_last = 0
                    state.universe.refresh_last_decision_reason = "SKIPPED_INFLIGHT"
                    inflight_age_ms = None
                    if state.universe.refresh_inflight_started_mono_ns is not None:
                        inflight_age_ms = int(
                            (monotonic_ns() - state.universe.refresh_inflight_started_mono_ns)
                            / 1_000_000
                        )
                    _write_runlog(
                        state,
                        {
                            "record_type": "universe_refresh",
                            "run_id": state.run.run_id,
                            "universe_version": state.universe.universe_version + 1,
                            "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                            "reason": "SKIPPED_INFLIGHT",
                            "inflight_age_ms": inflight_age_ms,
                        },
                    )
                    continue
                try:
                    inflight.result()
                except Exception:
                    pass
                state.universe.refresh_inflight_future = None
                state.universe.refresh_inflight_timed_out = False
                state.universe.refresh_inflight_started_mono_ns = None
            try:
                started_ns = monotonic_ns()
                refresh_call = functools.partial(
                    state.refresh_worker.compute_plan,
                    universe_version=state.universe.universe_version + 1,
                    run_id=state.run.run_id,
                    refresh_timeout_seconds=refresh_timeout_seconds,
                )
                future = asyncio.get_running_loop().run_in_executor(
                    state.refresh_executor,
                    refresh_call,
                )
                state.universe.refresh_inflight_future = future
                state.universe.refresh_inflight_started_mono_ns = started_ns
                state.universe.refresh_inflight_timed_out = False
                plan = await asyncio.wait_for(
                    asyncio.shield(future),
                    timeout=refresh_timeout_seconds,
                )
                duration_ms = (monotonic_ns() - started_ns) / 1_000_000.0
                state.universe.refresh_duration_ms_last = duration_ms
                state.universe.refresh_worker_duration_ms_last = plan.worker_duration_ms
                state.universe.refresh_gamma_pages_fetched_last = plan.gamma_pages_fetched
                state.universe.refresh_markets_seen_last = plan.markets_seen
                state.universe.refresh_tokens_selected_last = plan.tokens_selected
                state.universe.fee_regime_state = plan.fee_regime_state.state
                state.universe.fee_regime_reason = plan.fee_regime_state.reason
                state.universe.fee_regime_sampled_tokens_count = (
                    plan.fee_regime_sampled_tokens_count
                )
                state.universe.fee_rate_unknown_count_last = plan.fee_rate_unknown_count
                snapshot = plan.snapshot
                desired_tokens = plan.desired_token_ids
                desired_market_ids = plan.desired_market_ids
                added, removed = _compute_refresh_delta(
                    state.universe.current_token_ids, desired_tokens
                )
                added_markets = desired_market_ids - state.universe.current_market_ids
                removed_markets = state.universe.current_market_ids - desired_market_ids
                delta_count = len(added) + len(removed)
                churn_pct = 100.0 * delta_count / max(
                    1, len(state.universe.current_token_ids)
                )
                state.universe.refresh_churn_pct_last = churn_pct
                state.universe.tokens_added_last = len(added)
                state.universe.tokens_removed_last = len(removed)

                expected_summary = _expected_churn_for_refresh(
                    time.time_ns(),
                    removed_markets,
                    added_markets,
                    state.universe.segment_by_market_id,
                    plan.segment_by_market_id,
                    config,
                )
                state.universe.expected_churn_last = expected_summary.expected_churn
                state.universe.expected_churn_reason_last = expected_summary.reason
                state.universe.expected_churn_rollover_hit_last = (
                    expected_summary.rollover_window_hit
                )
                state.universe.expected_churn_expiry_hit_last = (
                    expected_summary.expiry_window_hit
                )

                if not _should_apply_refresh(
                    added,
                    removed,
                    config.capture_universe_refresh_min_delta_tokens,
                ):
                    state.universe.refresh_skipped_delta_below_min_count += 1
                    state.universe.shards_refreshed_last = 0
                    state.universe.refresh_last_decision_reason = "SKIPPED_DELTA_BELOW_MIN"
                    _write_runlog(
                        state,
                        {
                            "record_type": "universe_refresh",
                            "run_id": state.run.run_id,
                            "universe_version": snapshot.universe_version,
                            "added_count": len(added),
                            "removed_count": len(removed),
                            "churn_pct": churn_pct,
                            "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                            "added_sample": sorted(added)[:10],
                            "removed_sample": sorted(removed)[:10],
                            "reason": "SKIPPED_DELTA_BELOW_MIN",
                            "duration_ms": duration_ms,
                            "gamma_pages_fetched": plan.gamma_pages_fetched,
                            "markets_seen": plan.markets_seen,
                            "tokens_selected": plan.tokens_selected,
                        },
                    )
                    state.universe.refresh_inflight_future = None
                    state.universe.refresh_inflight_started_mono_ns = None
                    continue
                guard_triggered = (
                    churn_pct > config.capture_universe_refresh_max_churn_pct
                )
                bypass_guard = guard_triggered and expected_summary.expected_churn
                if bypass_guard:
                    state.universe.refresh_last_decision_reason = "BYPASSED_EXPECTED_CHURN"
                    guard_triggered = False
                (
                    next_interval,
                    next_guard_count,
                    guard_fatal,
                ) = _apply_churn_guard_policy(
                    state.universe.effective_refresh_interval_seconds,
                    baseline_interval,
                    config.capture_universe_refresh_max_interval_seconds,
                    guard_triggered=guard_triggered,
                    guard_count=state.universe.refresh_churn_guard_count,
                    fatal_threshold=config.capture_universe_refresh_churn_guard_consecutive_fatal,
                )
                state.universe.effective_refresh_interval_seconds = next_interval
                state.universe.refresh_churn_guard_count = next_guard_count
                if guard_triggered:
                    state.universe.shards_refreshed_last = 0
                    state.universe.refresh_last_decision_reason = "SKIPPED_CHURN_GUARD"
                    _write_runlog(
                        state,
                        {
                            "record_type": "universe_refresh",
                            "run_id": state.run.run_id,
                            "universe_version": snapshot.universe_version,
                            "added_count": len(added),
                            "removed_count": len(removed),
                            "churn_pct": churn_pct,
                            "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                            "added_sample": sorted(added)[:10],
                            "removed_sample": sorted(removed)[:10],
                            "reason": "CHURN_GUARD",
                            "duration_ms": duration_ms,
                            "gamma_pages_fetched": plan.gamma_pages_fetched,
                            "markets_seen": plan.markets_seen,
                            "tokens_selected": plan.tokens_selected,
                        },
                    )
                    if guard_fatal:
                        await _trigger_fatal(
                            state,
                            FATAL_CHURN_GUARD,
                            "churn guard sustained",
                        )
                        break
                    state.universe.refresh_inflight_future = None
                    state.universe.refresh_inflight_started_mono_ns = None
                    continue
                if not bypass_guard:
                    state.universe.refresh_last_decision_reason = "APPLIED"

                next_targets = _materialize_shard_targets(plan.shard_targets)
                changed_targets = _select_changed_shards(
                    state.universe.shard_targets, next_targets
                )

                _write_runlog(
                    state,
                    {
                        "record_type": "universe_refresh",
                        "run_id": state.run.run_id,
                        "universe_version": snapshot.universe_version,
                        "added_count": len(added),
                        "removed_count": len(removed),
                        "churn_pct": churn_pct,
                        "interval_seconds_used": state.universe.effective_refresh_interval_seconds,
                        "added_sample": sorted(added)[:10],
                        "removed_sample": sorted(removed)[:10],
                        "reason": "APPLIED",
                        "duration_ms": duration_ms,
                        "gamma_pages_fetched": plan.gamma_pages_fetched,
                        "markets_seen": plan.markets_seen,
                        "tokens_selected": plan.tokens_selected,
                    },
                )

                state.universe.universe_version = snapshot.universe_version
                state.universe.refresh_count += 1
                state.universe.shards_refreshed_last = len(changed_targets)
                state.universe.current_token_ids = desired_tokens
                state.universe.current_market_ids = desired_market_ids
                state.universe.segment_by_token_id = plan.segment_by_token_id
                state.universe.segment_by_market_id = plan.segment_by_market_id
                state.universe.policy_counts_last = plan.policy_counts
                now_ns = monotonic_ns()
                for token_id in added:
                    state.universe.token_added_mono_ns[token_id] = now_ns
                for token_id in removed:
                    state.universe.token_added_mono_ns.pop(token_id, None)
                state.pinned_tokens = plan.ordered_tokens
                if removed:
                    for shard in state.shards:
                        for token_id in removed:
                            shard.last_seen.pop(token_id, None)

                for shard_id, next_target in next_targets.items():
                    if (
                        state.universe.refresh_cancelled
                        or state.fatal_event.is_set()
                        or state.stop_event.is_set()
                    ):
                        break
                    current = state.universe.shard_targets.get(shard_id)
                    if current is None:
                        state.universe.shard_targets[shard_id] = next_target
                        current = next_target
                    if shard_id in changed_targets:
                        _write_runlog(
                            state,
                            {
                                "record_type": "shard_refresh_begin",
                                "run_id": state.run.run_id,
                                "shard_id": shard_id,
                                "from_version": current.target_version,
                                "to_version": snapshot.universe_version,
                                "from_token_count": len(current.token_ids),
                                "to_token_count": len(next_target.token_ids),
                            },
                        )
                        current.token_ids = next_target.token_ids
                        current.groups = next_target.groups
                        current.confirm_token_ids = next_target.confirm_token_ids
                        current.target_version = snapshot.universe_version
                        current.refresh_requested.set()
                        await asyncio.sleep(config.capture_universe_refresh_stagger_seconds)
                    else:
                        current.target_version = snapshot.universe_version
                state.universe.refresh_inflight_future = None
                state.universe.refresh_inflight_started_mono_ns = None
            except asyncio.TimeoutError:
                state.universe.refresh_failures += 1
                state.universe.refresh_last_decision_reason = "ERROR"
                if state.universe.refresh_inflight_started_mono_ns is not None:
                    state.universe.refresh_duration_ms_last = (
                        monotonic_ns() - state.universe.refresh_inflight_started_mono_ns
                    ) / 1_000_000.0
                state.universe.refresh_inflight_timed_out = True
                _write_runlog(
                    state,
                    {
                        "record_type": "universe_refresh_error",
                        "run_id": state.run.run_id,
                        "error": "TimeoutError",
                        "message": "refresh timeout",
                        "duration_ms": state.universe.refresh_duration_ms_last,
                    },
                )
            except asyncio.CancelledError:
                state.universe.refresh_cancelled = True
                break
            except Exception as exc:
                state.universe.refresh_failures += 1
                state.universe.refresh_last_decision_reason = "ERROR"
                if state.universe.refresh_inflight_started_mono_ns is not None:
                    state.universe.refresh_duration_ms_last = (
                        monotonic_ns() - state.universe.refresh_inflight_started_mono_ns
                    ) / 1_000_000.0
                _write_runlog(
                    state,
                    {
                        "record_type": "universe_refresh_error",
                        "run_id": state.run.run_id,
                        "error": type(exc).__name__,
                        "message": str(exc),
                        "duration_ms": state.universe.refresh_duration_ms_last,
                    },
                )
                state.universe.refresh_inflight_future = None
                state.universe.refresh_inflight_started_mono_ns = None
    finally:
        inflight = state.universe.refresh_inflight_future
        if state.refresh_worker is not None and (inflight is None or inflight.done()):
            state.refresh_worker.close()
            state.refresh_worker = None
        if state.refresh_executor is not None:
            state.refresh_executor.shutdown(wait=False, cancel_futures=True)
            state.refresh_executor = None


async def _capture_online_async(
    config: Config,
    run_id: str | None = None,
    *,
    duration_seconds: float | None = None,
) -> int:
    if config.ws_shards <= 0:
        raise ValueError("ws_shards must be >= 1")
    (
        initial_snapshot,
        pinned_tokens,
        pinned_markets,
        selected_markets,
        universe_mode,
    ) = _load_startup_universe(config)
    if not pinned_tokens:
        raise RuntimeError("no pinned tokens available for capture")
    scores = _token_score_map(selected_markets)
    shard_targets = _build_shard_targets(
        pinned_tokens,
        config,
        target_version=initial_snapshot.universe_version,
        scores=scores,
    )
    fee_rate_client = _build_fee_rate_client(config) if config.fee_rate_enable else None
    policy_selector = _build_policy_selector(config)
    fee_regime_monitor = _build_fee_regime_monitor(config)
    fee_results = (
        fee_rate_client.cached_fee_rates(pinned_tokens) if fee_rate_client else {}
    )
    segment_by_token, segment_by_market = build_segment_maps(
        selected_markets,
        fee_results_by_token=fee_results,
    )
    policy_counts_initial = policy_counts(policy_selector, segment_by_token)
    policy_summary_initial = _segment_policy_summary(policy_selector, segment_by_token)
    canary_tokens = _parse_canary_tokens(config.fee_regime_canary_token_ids)
    expected_fee_rate_values = sorted(
        parse_expected_fee_rate_bps_values(config.fee_regime_expected_fee_rate_bps_values)
    )

    manifest_extra = {
        "capture_schema_version": config.capture_frames_schema_version,
        "payload_source": "text",
        "payload_encoder": "utf-8",
        "universe_mode": universe_mode,
        "monotonic_ns_source": "perf_counter_ns",
        "git_commit": _read_git_commit(),
        "environment": _environment_metadata(),
        "config": _config_snapshot(config),
        "pinned_tokens": pinned_tokens,
        "pinned_markets": pinned_markets,
        "segment_metadata": {
            "schema_version": 1,
            "by_token_id": {
                token_id: tag.to_dict() for token_id, tag in segment_by_token.items()
            },
            "by_market_id": {
                market_id: tag.to_dict() for market_id, tag in segment_by_market.items()
            },
        },
        "policy_map": policy_selector.policy_map.to_dict(),
        "policy_assignments": {
            "by_policy_id": policy_counts_initial,
            "by_segment": policy_summary_initial,
        },
        "fee_regime": {
            "expect_15m_crypto_fee_enabled": config.fee_regime_expect_15m_crypto_fee_enabled,
            "expect_other_fee_free": config.fee_regime_expect_other_fee_free,
            "expect_unknown_fee_free": config.fee_regime_expect_unknown_fee_free,
            "sample_size_per_cycle": config.fee_regime_sample_size_per_cycle,
            "canary_token_ids": canary_tokens,
            "expected_fee_rate_bps_values": expected_fee_rate_values,
        },
        "initial_universe": _snapshot_for_manifest(initial_snapshot),
        "universe_refresh": {
            "enabled": config.capture_universe_refresh_enable,
            "interval_seconds": config.capture_universe_refresh_interval_seconds,
            "timeout_seconds": config.capture_universe_refresh_timeout_seconds,
            "stagger_seconds": config.capture_universe_refresh_stagger_seconds,
            "grace_seconds": config.capture_universe_refresh_grace_seconds,
            "min_delta_tokens": config.capture_universe_refresh_min_delta_tokens,
            "max_churn_pct": config.capture_universe_refresh_max_churn_pct,
            "max_interval_seconds": config.capture_universe_refresh_max_interval_seconds,
            "churn_guard_consecutive_fatal": config.capture_universe_refresh_churn_guard_consecutive_fatal,
        },
        "shards": {
            "count": config.ws_shards,
            "assignments": {
                str(shard_id): {"token_ids": target.token_ids, "groups": target.groups}
                for shard_id, target in shard_targets.items()
            },
        },
        "subscribe_caps": {
            "max_tokens": config.ws_subscribe_max_tokens,
            "max_bytes": config.ws_subscribe_max_bytes,
        },
    }

    run = bootstrap_run(config, run_id=run_id, manifest_extra=manifest_extra)
    run_dir = run.run_dir
    print(f"capture run dir: {run_dir}")
    runlog_writer = _NdjsonWriter(
        thread_name="runlog-writer",
        fsync_on_close=config.capture_ndjson_fsync_on_close,
        fsync_interval_seconds=config.capture_ndjson_fsync_interval_seconds,
    )
    metrics_writer = _NdjsonWriter(
        thread_name="metrics-writer",
        fsync_on_close=config.capture_ndjson_fsync_on_close,
        fsync_interval_seconds=config.capture_ndjson_fsync_interval_seconds,
    )
    shards: list[ShardState] = []
    for shard_id, target in shard_targets.items():
        frames_path = run_dir / "capture" / f"shard_{shard_id:02d}.frames"
        idx_path = run_dir / "capture" / f"shard_{shard_id:02d}.idx"
        frames_fh = frames_path.open("ab")
        idx_fh = idx_path.open("ab")
        ring = deque(maxlen=config.capture_ring_buffer_frames)
        shards.append(
            ShardState(
                shard_id=shard_id,
                token_ids=target.token_ids,
                groups=target.groups,
                frames_path=frames_path,
                idx_path=idx_path,
                frames_fh=frames_fh,
                idx_fh=idx_fh,
                ring=ring,
                confirm_token_ids=target.confirm_token_ids,
                target=target,
                disconnected_since_mono_ns=run.t0_mono_ns,
            )
        )
    token_added_mono_ns: dict[str, int] = {}
    universe_state = UniverseState(
        universe_version=initial_snapshot.universe_version,
        current_token_ids=set(initial_snapshot.token_ids),
        current_market_ids=set(initial_snapshot.market_ids),
        token_added_mono_ns=token_added_mono_ns,
        shard_targets=shard_targets,
        effective_refresh_interval_seconds=config.capture_universe_refresh_interval_seconds,
        segment_by_token_id=segment_by_token,
        segment_by_market_id=segment_by_market,
        policy_counts_last=policy_counts_initial,
    )
    state = CaptureState(
        run=run,
        config=config,
        shards=shards,
        pinned_tokens=pinned_tokens,
        universe=universe_state,
        fee_rate_client=fee_rate_client,
        policy_selector=policy_selector,
        fee_regime_monitor=fee_regime_monitor,
        runlog_writer=runlog_writer,
        metrics_writer=metrics_writer,
    )
    precision = _monotonic_precision_stats(sample_count=200)
    _write_runlog(
        state,
        {
            "record_type": "monotonic_precision",
            "run_id": run.run_id,
            **precision,
        },
    )
    if not precision.get("ok", False):
        _write_startup_fatal(
            state,
            FATAL_MONO_QUANTIZED,
            "monotonic clock precision insufficient",
            extra={"monotonic_precision": precision},
        )
        runlog_writer.close()
        metrics_writer.close()
        for shard in shards:
            shard.frames_fh.flush()
            shard.idx_fh.flush()
            os.fsync(shard.frames_fh.fileno())
            os.fsync(shard.idx_fh.fileno())
            shard.frames_fh.close()
            shard.idx_fh.close()
        return 1
    state.parse_worker = _ParseWorker()
    state.frame_writer = _FrameWriter(
        shards=shards,
        schema_version=config.capture_frames_schema_version,
    )
    if config.capture_gc_disable and gc.isenabled():
        gc.disable()
        state.gc_was_enabled = True
    _install_signal_handlers(state)
    _write_runlog(
        state,
        {
            "record_type": "capture_start",
            "run_id": run.run_id,
            "shards": config.ws_shards,
            "token_count": len(pinned_tokens),
            "universe_version": initial_snapshot.universe_version,
        },
    )

    tasks = [asyncio.create_task(_run_shard(state, shard)) for shard in shards]
    heartbeat_task = asyncio.create_task(_heartbeat_loop(state))
    tasks.append(heartbeat_task)
    lag_task = asyncio.create_task(_loop_lag_monitor(state))
    tasks.append(lag_task)
    parse_task = asyncio.create_task(_parse_results_loop(state))
    tasks.append(parse_task)
    write_task = asyncio.create_task(_write_results_loop(state))
    tasks.append(write_task)
    if config.capture_universe_refresh_enable:
        refresh_task = asyncio.create_task(_refresh_loop(state))
        state.universe.refresh_task = refresh_task
        tasks.append(refresh_task)
    stop_task = asyncio.create_task(state.stop_event.wait())
    tasks.append(stop_task)
    if duration_seconds is not None and duration_seconds > 0:
        async def _stop_after_duration() -> None:
            await asyncio.sleep(duration_seconds)
            if state.fatal_event.is_set() or state.stop_event.is_set():
                return
            state.stop_reason = "duration"
            state.stop_event.set()

        tasks.append(asyncio.create_task(_stop_after_duration()))

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    if state.stop_event.is_set() and not state.fatal_event.is_set():
        loss_budget = state.loss_budget
        parse_worker_stats: dict[str, int] = {}
        if state.parse_worker is not None:
            parse_worker_stats = state.parse_worker.stats()
        parse_worker_dropped = int(parse_worker_stats.get("dropped", 0))
        parse_results_dropped = int(parse_worker_stats.get("results_dropped", 0))
        quality_reasons: list[str] = []
        if loss_budget.parse_queue_drops.total > 0 or parse_worker_dropped > 0:
            quality_reasons.append("parse_queue_drops")
        if loss_budget.parse_results_drops.total > 0 or parse_results_dropped > 0:
            quality_reasons.append("parse_results_drops")
        if loss_budget.metrics_drops.total > 0:
            quality_reasons.append("metrics_drops")
        if loss_budget.runlog_enqueue_timeouts.total > 0:
            quality_reasons.append("runlog_enqueue_timeouts")
        if loss_budget.frame_write_queue_pressure_events.total > 0:
            quality_reasons.append("frame_write_queue_pressure_events")
        quality_ok = not quality_reasons
        quality_status = "ok" if quality_ok else "degraded"
        loss_total = (
            loss_budget.parse_queue_drops.total
            + loss_budget.parse_results_drops.total
            + loss_budget.metrics_drops.total
            + loss_budget.runlog_enqueue_timeouts.total
            + loss_budget.frame_write_queue_pressure_events.total
        )
        _write_runlog(
            state,
            {
                "record_type": "capture_stop",
                "run_id": state.run.run_id,
                "reason": state.stop_reason or "signal",
                "loss_total": loss_total,
                "loss_parse_queue_drops": loss_budget.parse_queue_drops.total,
                "loss_parse_results_drops": loss_budget.parse_results_drops.total,
                "loss_metrics_drops": loss_budget.metrics_drops.total,
                "loss_runlog_enqueue_timeouts": loss_budget.runlog_enqueue_timeouts.total,
                "loss_frame_write_queue_pressure_events": (
                    loss_budget.frame_write_queue_pressure_events.total
                ),
                "parse_worker_dropped": parse_worker_dropped,
                "parse_results_dropped": parse_results_dropped,
                "quality_status": quality_status,
                "quality_ok": quality_ok,
                "quality_reasons": quality_reasons,
            },
        )
    if not state.fatal_event.is_set():
        for task in done:
            exc = task.exception()
            if exc is not None:
                await _trigger_fatal(
                    state,
                    FATAL_INTERNAL,
                    f"task failed: {type(exc).__name__}",
                )
                break
    state.universe.refresh_cancelled = True
    for task in pending:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    if state.frame_writer is not None:
        state.frame_writer.close()
        state.frame_writer = None

    for shard in shards:
        shard.frames_fh.flush()
        shard.idx_fh.flush()
        os.fsync(shard.frames_fh.fileno())
        os.fsync(shard.idx_fh.fileno())
        shard.frames_fh.close()
        shard.idx_fh.close()

    if state.parse_worker is not None:
        state.parse_worker.close()
        state.parse_worker = None

    if state.runlog_writer is not None:
        state.runlog_writer.close()
        state.runlog_writer = None
    if state.metrics_writer is not None:
        state.metrics_writer.close()
        state.metrics_writer = None

    if state.gc_was_enabled:
        gc.enable()

    return 1 if state.fatal_event.is_set() else 0


def run_capture_online(
    config: Config,
    run_id: str | None = None,
    *,
    duration_seconds: float | None = None,
) -> int:
    try:
        from .windows_timer import windows_high_res_timer

        with windows_high_res_timer(enable=config.capture_windows_high_res_timer_enable):
            return asyncio.run(
                _capture_online_async(
                    config,
                    run_id=run_id,
                    duration_seconds=duration_seconds,
                )
            )
    except KeyboardInterrupt:
        return 0
