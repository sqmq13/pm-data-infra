from pm_data.runtime.allocator import Allocator
from pm_data.runtime.events import TopOfBookUpdate
from pm_data.runtime.execution_sim import SimExecutionBackend
from pm_data.runtime.intents import Intent, PlaceOrderIntent
from pm_data.runtime.orchestrator import Orchestrator
from pm_data.runtime.state import GlobalState, MarketState
from pm_data.runtime.strategy import PortfolioView, StrategyContext


class StepClock:
    def __init__(self) -> None:
        self._ns = 0

    def __call__(self) -> int:
        self._ns += 1
        return self._ns


class EchoStrategy:
    def __init__(self, strategy_id: str) -> None:
        self.strategy_id = strategy_id

    def on_top_of_book(
        self,
        ctx: StrategyContext,
        event: TopOfBookUpdate,
        market_state: MarketState,
        portfolio: PortfolioView,
    ) -> list[Intent]:
        if event.bid_px_e6 is None:
            return []
        return [
            PlaceOrderIntent(
                market_id=event.market_id,
                side="buy",
                price_e6=event.bid_px_e6,
                size_e6=1_000_000,
                order_type="limit",
                tif="gtc",
                urgency="maker",
                max_slippage_bps=None,
                tag=self.strategy_id,
                expires_at=None,
            )
        ]

    def on_timer(self, ctx: StrategyContext, ts_ns: int) -> list[Intent]:
        return []


def _build_events() -> list[TopOfBookUpdate]:
    return [
        TopOfBookUpdate(
            market_id="m1",
            bid_px_e6=900_000,
            bid_sz_e6=1_000_000,
            ask_px_e6=1_100_000,
            ask_sz_e6=1_000_000,
            ts_event=10,
            ts_recv=20,
            seq=0,
        ),
        TopOfBookUpdate(
            market_id="m1",
            bid_px_e6=950_000,
            bid_sz_e6=1_000_000,
            ask_px_e6=1_050_000,
            ask_sz_e6=1_000_000,
            ts_event=30,
            ts_recv=40,
            seq=0,
        ),
    ]


def _run_once():
    clock = StepClock()
    state = GlobalState()
    orchestrator = Orchestrator(
        strategies={"b": EchoStrategy("b"), "a": EchoStrategy("a")},
        allocator=Allocator(),
        execution=SimExecutionBackend(state=state),
        state=state,
        now_ns=clock,
        clock_ns=clock,
    )
    return orchestrator.run_events(_build_events())


def test_orchestrator_deterministic_intent_stream():
    first = _run_once()
    second = _run_once()
    assert first.events_processed == 2
    assert second.events_processed == 2
    assert first.intents == second.intents
    assert [intent.tag for intent in first.intents] == ["a", "b", "a", "b"]
