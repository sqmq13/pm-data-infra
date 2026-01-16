from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from pm_data.config import Config

from .allocator import Allocator
from .execution_sim import FillEvent, SimExecutionBackend, SimExecutionConfig
from .ledger import Ledger
from .live import LiveDataSource, load_live_tokens
from .intents import CancelIntent, Intent, PlaceOrderIntent
from .normalize import Normalizer
from .orchestrator import Orchestrator
from .orchestrator import EventLatencyTrace
from .state import GlobalState
from .replay import ReplayDataSource
from .strategy import Strategy
from .latency import LiveLatencyCollector


@dataclass(slots=True)
class RunSummary:
    ok: bool
    mode: str
    execution: str
    canonical_events: int
    intents: int
    final_hash: str
    elapsed_ms: float
    error: str | None = None
    reconnects: int | None = None
    decode_errors: int | None = None
    dropped: int | None = None
    callback_stats: Mapping[str, dict[str, float | int]] | None = None
    submitted_intents: int | None = None
    pnl_summary: Mapping[str, object] | None = None


def _canonicalize_stable_json(value: object) -> object:
    if isinstance(value, dict):
        items: list[tuple[str, object]] = []
        for key, item in value.items():
            items.append((str(key), _canonicalize_stable_json(item)))
        items.sort(key=lambda pair: pair[0])
        return OrderedDict(items)
    if isinstance(value, list):
        return [_canonicalize_stable_json(item) for item in value]
    return value


def format_run_summary(summary: RunSummary, *, stable: bool) -> str:
    payload = OrderedDict()
    payload["ok"] = summary.ok
    payload["mode"] = summary.mode
    payload["execution"] = summary.execution
    payload["canonical_events"] = summary.canonical_events
    payload["intents"] = summary.intents
    payload["final_hash"] = summary.final_hash
    payload["elapsed_ms"] = 0 if stable else summary.elapsed_ms
    if summary.reconnects is not None:
        payload["reconnects"] = summary.reconnects
    if summary.decode_errors is not None:
        payload["decode_errors"] = summary.decode_errors
    if summary.dropped is not None:
        payload["dropped"] = summary.dropped
    if summary.callback_stats is not None:
        payload["callback_stats"] = summary.callback_stats
    if summary.submitted_intents is not None:
        payload["submitted_intents"] = summary.submitted_intents
    if summary.pnl_summary is not None:
        payload["pnl"] = summary.pnl_summary
    if summary.error:
        payload["error"] = summary.error
    if stable:
        payload = OrderedDict(
            (key, _canonicalize_stable_json(value)) for key, value in payload.items()
        )
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def format_latency_report(report: Mapping[str, object], *, stable: bool) -> str:
    payload: object = report
    if stable:
        payload = _canonicalize_stable_json(dict(report))
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


def _build_strategies(
    strategy_names: Sequence[str] | None,
    strategy_params: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, Strategy]:
    strategies: dict[str, Strategy] = {}
    params = strategy_params or {}
    for name in strategy_names or []:
        if name == "toy_spread":
            from pm_data.strategies.toy_spread import ToySpreadStrategy

            extra = params.get(name, {})
            strategies[name] = ToySpreadStrategy(**extra)
        else:
            raise ValueError(f"unknown strategy: {name}")
    return strategies


def _hash_intent_line(seq: int, intent: Intent) -> bytes:
    # Hash format (one line per intent) for deterministic audits:
    # seq|strategy_id|intent_type|market_id|side|price_e6|size_e6|tif|urgency|tag\n
    # Execution ack events are excluded from the hash.
    if isinstance(intent, PlaceOrderIntent):
        intent_type = "place"
        market_id = intent.market_id
        side = intent.side
        price_e6 = str(intent.price_e6)
        size_e6 = str(intent.size_e6)
        tif = intent.tif
        urgency = intent.urgency
        tag = intent.tag or ""
        strategy_id = intent.strategy_id or ""
    elif isinstance(intent, CancelIntent):
        intent_type = "cancel"
        market_id = intent.market_id or ""
        side = ""
        price_e6 = ""
        size_e6 = ""
        tif = ""
        urgency = ""
        tag = intent.tag or ""
        strategy_id = intent.strategy_id or ""
    else:
        intent_type = "unknown"
        market_id = ""
        side = ""
        price_e6 = ""
        size_e6 = ""
        tif = ""
        urgency = ""
        tag = ""
        strategy_id = ""
    line = (
        f"{seq}|{strategy_id}|{intent_type}|{market_id}|{side}|{price_e6}|"
        f"{size_e6}|{tif}|{urgency}|{tag}\n"
    )
    return line.encode("utf-8")


def _callback_stats_payload(
    stats: Mapping[str, object],
) -> dict[str, dict[str, float | int]]:
    payload: dict[str, dict[str, float | int]] = {}
    for strategy_id, entry in stats.items():
        count = getattr(entry, "count", 0)
        total_ns = getattr(entry, "total_ns", 0)
        max_ns = getattr(entry, "max_ns", 0)
        overruns = getattr(entry, "overruns", 0)
        mean_ns = 0.0
        if count:
            mean_ns = total_ns / count
        payload[strategy_id] = {
            "count": int(count),
            "mean_ns": mean_ns,
            "max_ns": int(max_ns),
            "overruns": int(overruns),
        }
    return payload


async def _run_replay_sim_async(
    *,
    run_dir: Path,
    max_seconds: float | None,
    max_events: int | None,
    strategy_names: Sequence[str] | None,
    strategy_params: Mapping[str, Mapping[str, object]] | None,
    include_pnl: bool,
) -> RunSummary:
    start = time.perf_counter()
    strategies = _build_strategies(strategy_names, strategy_params)
    state = GlobalState()
    execution = SimExecutionBackend(state=state, config=SimExecutionConfig())
    orchestrator = Orchestrator(
        strategies=strategies,
        allocator=Allocator(),
        execution=execution,
        state=state,
        strategy_params=strategy_params,
    )
    normalizer = Normalizer()
    data_source = ReplayDataSource(run_dir=run_dir, max_seconds=max_seconds)
    ledger = Ledger() if include_pnl else None
    hasher = hashlib.sha256()
    canonical_events = 0
    intents = 0
    stop = False

    async for frame in data_source.stream():
        for event in normalizer.normalize(frame):
            canonical_events += 1
            if ledger is not None:
                ledger.update_market(
                    event.market_id,
                    event.bid_px_e6,
                    event.ask_px_e6,
                )
            merged, _exec_events = orchestrator.process_event(event)
            if merged:
                for intent in merged:
                    hasher.update(_hash_intent_line(event.seq, intent))
                intents += len(merged)
            if ledger is not None and _exec_events:
                for exec_event in _exec_events:
                    if isinstance(exec_event, FillEvent):
                        ledger.apply_fill(
                            market_id=exec_event.market_id,
                            side=exec_event.side,
                            price_e6=exec_event.price_e6,
                            size_e6=exec_event.size_e6,
                            fee_e6=exec_event.fee_e6,
                        )
            if max_events is not None and canonical_events >= max_events:
                stop = True
                break
        if stop:
            break

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    final_hash = hasher.hexdigest()
    return RunSummary(
        ok=True,
        mode="replay",
        execution="sim",
        canonical_events=canonical_events,
        intents=intents,
        final_hash=final_hash,
        elapsed_ms=elapsed_ms,
        error=None,
        pnl_summary=ledger.summary() if ledger is not None else None,
    )


def run_replay_sim(
    *,
    run_dir: Path,
    max_seconds: float | None = None,
    max_events: int | None = None,
    strategy_names: Sequence[str] | None = None,
    strategy_params: Mapping[str, Mapping[str, object]] | None = None,
    include_pnl: bool = False,
) -> RunSummary:
    return asyncio.run(
        _run_replay_sim_async(
            run_dir=run_dir,
            max_seconds=max_seconds,
            max_events=max_events,
            strategy_names=strategy_names,
            strategy_params=strategy_params,
            include_pnl=include_pnl,
        )
    )


async def _run_live_sim_async(
    *,
    config: Config,
    duration_seconds: float,
    max_events: int | None,
    strategy_names: Sequence[str] | None,
    strategy_params: Mapping[str, Mapping[str, object]] | None,
    token_ids: Sequence[str],
    latency_report: bool,
) -> tuple[RunSummary, dict[str, object] | None]:
    start = time.perf_counter()
    strategies = _build_strategies(strategy_names, strategy_params)
    state = GlobalState()
    execution = SimExecutionBackend(state=state, config=SimExecutionConfig())
    orchestrator = Orchestrator(
        strategies=strategies,
        allocator=Allocator(),
        execution=execution,
        state=state,
        strategy_params=strategy_params,
    )
    normalizer = Normalizer()
    data_source = LiveDataSource(
        config=config,
        duration_seconds=duration_seconds,
        token_ids=list(token_ids),
    )
    hasher = hashlib.sha256()
    canonical_events = 0
    intents = 0
    submitted_intents = 0
    stop = False
    collector: LiveLatencyCollector | None = LiveLatencyCollector() if latency_report else None
    if collector is not None:
        collector.start(time.perf_counter_ns())

    async for frame in data_source.stream():
        if collector is not None:
            collector.frames += 1
            recv_ns = frame.rx_mono_ns
            for event, decode_end_ns, normalize_end_ns in normalizer.iter_normalize_timed(
                frame
            ):
                trace = EventLatencyTrace()
                canonical_events += 1
                merged, _exec_events = orchestrator.process_event(event, trace=trace)
                collector.observe_event(
                    recv_ns=recv_ns,
                    decode_end_ns=decode_end_ns,
                    normalize_end_ns=normalize_end_ns,
                    state_end_ns=trace.state_end_ns,
                    strategy_end_ns=trace.strategy_end_ns,
                    allocator_end_ns=trace.allocator_end_ns,
                    execution_end_ns=trace.execution_end_ns,
                    emit_end_ns=trace.execution_end_ns,
                )
                if merged:
                    for intent in merged:
                        hasher.update(_hash_intent_line(event.seq, intent))
                    intents += len(merged)
                    submitted_intents += len(merged)
                    collector.intents += len(merged)
                if max_events is not None and canonical_events >= max_events:
                    stop = True
                    break
        else:
            for event in normalizer.iter_normalize(frame):
                canonical_events += 1
                merged, _exec_events = orchestrator.process_event(event)
                if merged:
                    for intent in merged:
                        hasher.update(_hash_intent_line(event.seq, intent))
                    intents += len(merged)
                    submitted_intents += len(merged)
                if max_events is not None and canonical_events >= max_events:
                    stop = True
                    break
        if stop:
            break

    elapsed_ms = (time.perf_counter() - start) * 1000.0
    final_hash = hasher.hexdigest()
    summary = RunSummary(
        ok=True,
        mode="live",
        execution="sim",
        canonical_events=canonical_events,
        intents=intents,
        final_hash=final_hash,
        elapsed_ms=elapsed_ms,
        error=None,
        reconnects=data_source.stats.reconnects,
        decode_errors=normalizer.decode_errors,
        dropped=data_source.stats.dropped.total,
        callback_stats=_callback_stats_payload(orchestrator.callback_stats()),
        submitted_intents=submitted_intents,
    )
    report: dict[str, object] | None = None
    if collector is not None:
        collector.finish(time.perf_counter_ns())
        report = collector.report(
            reconnects=data_source.stats.reconnects,
            dropped=data_source.stats.dropped.total,
            decode_errors=normalizer.decode_errors,
            schema_errors=normalizer.schema_errors,
            bid_scan_count=normalizer.bid_scan_count,
            bid_scan_levels_total=normalizer.bid_scan_levels_total,
            bid_scan_levels_max=normalizer.bid_scan_levels_max,
            ask_scan_count=normalizer.ask_scan_count,
            ask_scan_levels_total=normalizer.ask_scan_levels_total,
            ask_scan_levels_max=normalizer.ask_scan_levels_max,
        )
    return summary, report


def run_live_sim(
    *,
    config: Config,
    duration_seconds: float,
    max_events: int | None = None,
    strategy_names: Sequence[str] | None = None,
    strategy_params: Mapping[str, Mapping[str, object]] | None = None,
    latency_report: bool = False,
) -> tuple[RunSummary, dict[str, object] | None]:
    token_ids = load_live_tokens(config)
    from pm_data.windows_timer import windows_high_res_timer

    with windows_high_res_timer(enable=config.runtime_windows_high_res_timer_enable):
        return asyncio.run(
            _run_live_sim_async(
                config=config,
                duration_seconds=duration_seconds,
                max_events=max_events,
                strategy_names=strategy_names,
                strategy_params=strategy_params,
                token_ids=token_ids,
                latency_report=latency_report,
            )
        )
