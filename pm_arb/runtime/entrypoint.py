from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Mapping, Sequence

from pm_arb.config import Config

from .allocator import Allocator
from .execution_sim import FillEvent, SimExecutionBackend, SimExecutionConfig
from .ledger import Ledger
from .live import LiveDataSource
from .intents import CancelIntent, Intent, PlaceOrderIntent
from .normalize import Normalizer
from .orchestrator import Orchestrator
from .state import GlobalState
from .replay import ReplayDataSource
from .strategy import Strategy


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
    callback_stats: Mapping[str, dict[str, float | int]] | None = None
    submitted_intents: int | None = None
    pnl_summary: Mapping[str, object] | None = None


def _build_strategies(
    strategy_names: Sequence[str] | None,
    strategy_params: Mapping[str, Mapping[str, object]] | None,
) -> dict[str, Strategy]:
    strategies: dict[str, Strategy] = {}
    params = strategy_params or {}
    for name in strategy_names or []:
        if name == "toy_spread":
            from pm_arb.strategies.toy_spread import ToySpreadStrategy

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
        strategy_id = intent.tag or ""
    elif isinstance(intent, CancelIntent):
        intent_type = "cancel"
        market_id = intent.market_id or ""
        side = ""
        price_e6 = ""
        size_e6 = ""
        tif = ""
        urgency = ""
        tag = intent.tag or ""
        strategy_id = intent.tag or ""
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
    data_source = LiveDataSource(config=config, duration_seconds=duration_seconds)
    hasher = hashlib.sha256()
    canonical_events = 0
    intents = 0
    submitted_intents = 0
    stop = False

    async for frame in data_source.stream():
        for event in normalizer.normalize(frame):
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
    return RunSummary(
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
        callback_stats=_callback_stats_payload(orchestrator.callback_stats()),
        submitted_intents=submitted_intents,
    )


def run_live_sim(
    *,
    config: Config,
    duration_seconds: float,
    max_events: int | None = None,
    strategy_names: Sequence[str] | None = None,
    strategy_params: Mapping[str, Mapping[str, object]] | None = None,
) -> RunSummary:
    return asyncio.run(
        _run_live_sim_async(
            config=config,
            duration_seconds=duration_seconds,
            max_events=max_events,
            strategy_names=strategy_names,
            strategy_params=strategy_params,
        )
    )
