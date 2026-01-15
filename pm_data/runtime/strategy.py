from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Protocol

from .events import TopOfBookUpdate
from .intents import Intent
from .state import MarketState


@dataclass(slots=True)
class PortfolioView:
    positions: Mapping[str, int] = field(default_factory=dict)
    balances: Mapping[str, int] = field(default_factory=dict)


@dataclass(slots=True)
class StrategyContext:
    strategy_id: str
    params: Mapping[str, object] = field(default_factory=dict)


class Strategy(Protocol):
    strategy_id: str

    def on_top_of_book(
        self,
        ctx: StrategyContext,
        event: TopOfBookUpdate,
        market_state: MarketState,
        portfolio: PortfolioView,
    ) -> list[Intent]:
        ...

    def on_timer(self, ctx: StrategyContext, ts_ns: int) -> list[Intent]:
        ...
