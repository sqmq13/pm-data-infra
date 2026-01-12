from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Mapping

from .allocator import Allocator
from .events import TopOfBookUpdate
from .execution_sim import ExecutionEvent, SimExecutionBackend
from .intents import Intent
from .state import GlobalState
from .strategy import PortfolioView, Strategy, StrategyContext


@dataclass(slots=True)
class CallbackStats:
    count: int = 0
    total_ns: int = 0
    max_ns: int = 0
    overruns: int = 0

    def record(
        self,
        duration_ns: int,
        max_callback_ns: int | None,
        overrun_hook: Callable[[str, int], None] | None,
        strategy_id: str,
    ) -> None:
        self.count += 1
        self.total_ns += duration_ns
        if duration_ns > self.max_ns:
            self.max_ns = duration_ns
        if max_callback_ns is not None and duration_ns > max_callback_ns:
            self.overruns += 1
            if overrun_hook is not None:
                overrun_hook(strategy_id, duration_ns)


@dataclass(slots=True)
class OrchestratorConfig:
    max_callback_us: int | None = None


@dataclass(slots=True)
class OrchestratorResult:
    events_processed: int
    intents: list[Intent]
    execution_events: list[ExecutionEvent]
    callback_stats: Mapping[str, CallbackStats]


class Orchestrator:
    def __init__(
        self,
        *,
        strategies: Mapping[str, Strategy],
        allocator: Allocator,
        execution: SimExecutionBackend,
        state: GlobalState | None = None,
        strategy_params: Mapping[str, Mapping[str, object]] | None = None,
        config: OrchestratorConfig | None = None,
        now_ns: Callable[[], int] | None = None,
        clock_ns: Callable[[], int] | None = None,
        overrun_hook: Callable[[str, int], None] | None = None,
    ) -> None:
        self._strategies = dict(strategies)
        self._strategy_order = sorted(self._strategies)
        self._allocator = allocator
        self._execution = execution
        self._state = state or GlobalState()
        params = strategy_params or {}
        self._contexts = {
            strategy_id: StrategyContext(
                strategy_id=strategy_id,
                params=params.get(strategy_id, {}),
            )
            for strategy_id in self._strategy_order
        }
        self._config = config or OrchestratorConfig()
        self._max_callback_ns = (
            self._config.max_callback_us * 1_000 if self._config.max_callback_us else None
        )
        self._now_ns = now_ns or time.perf_counter_ns
        self._clock_ns = clock_ns or time.perf_counter_ns
        self._overrun_hook = overrun_hook
        self._callback_stats = {
            strategy_id: CallbackStats() for strategy_id in self._strategy_order
        }
        self._seq = 0
        self._portfolio = PortfolioView(
            positions=self._state.positions,
            balances=self._state.balances,
        )

    def process_event(
        self, event: TopOfBookUpdate
    ) -> tuple[list[Intent], list[ExecutionEvent]]:
        self._seq += 1
        event.seq = self._seq
        market_state = self._state.get_market(event.market_id)
        market_state.apply_top_of_book(event)
        intents_by_strategy: dict[str, list[Intent]] = {}
        for strategy_id in self._strategy_order:
            strategy = self._strategies[strategy_id]
            ctx = self._contexts[strategy_id]
            start_ns = self._clock_ns()
            intents = strategy.on_top_of_book(
                ctx,
                event,
                market_state,
                self._portfolio,
            )
            duration_ns = self._clock_ns() - start_ns
            self._callback_stats[strategy_id].record(
                duration_ns,
                self._max_callback_ns,
                self._overrun_hook,
                strategy_id,
            )
            if intents:
                intents_by_strategy[strategy_id] = intents
        merged = self._allocator.merge_intents(intents_by_strategy, self._state)
        execution_events: list[ExecutionEvent] = []
        if merged:
            execution_events = self._execution.submit(merged, self._now_ns())
        return merged, execution_events

    def run_events(self, events: Iterable[TopOfBookUpdate]) -> OrchestratorResult:
        intents_out: list[Intent] = []
        execution_events: list[ExecutionEvent] = []
        events_processed = 0

        for event in events:
            events_processed += 1
            merged, exec_events = self.process_event(event)
            if merged:
                intents_out.extend(merged)
            if exec_events:
                execution_events.extend(exec_events)

        return OrchestratorResult(
            events_processed=events_processed,
            intents=intents_out,
            execution_events=execution_events,
            callback_stats=self._callback_stats,
        )

    def callback_stats(self) -> Mapping[str, CallbackStats]:
        return self._callback_stats
