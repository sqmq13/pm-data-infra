from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pm_data.runtime.events import TopOfBookUpdate
from pm_data.runtime.intents import Intent, PlaceOrderIntent
from pm_data.runtime.state import MarketState
from pm_data.runtime.strategy import PortfolioView, StrategyContext


@dataclass(slots=True)
class ToySpreadStrategy:
    strategy_id: str = "toy_spread"
    size_e6: int = 1_000_000
    min_spread_e6: int = 50_000
    order_type: Literal["limit", "market"] = "limit"
    tif: Literal["gtc", "ioc", "gtd"] = "gtc"
    urgency: Literal["maker", "taker"] = "maker"
    max_slippage_bps: int | None = None

    def on_top_of_book(
        self,
        ctx: StrategyContext,
        event: TopOfBookUpdate,
        market_state: MarketState,
        portfolio: PortfolioView,
    ) -> list[Intent]:
        if event.bid_px_e6 is None or event.ask_px_e6 is None:
            return []
        if event.ask_px_e6 - event.bid_px_e6 < self.min_spread_e6:
            return []
        buy = PlaceOrderIntent(
            market_id=event.market_id,
            side="buy",
            price_e6=event.bid_px_e6,
            size_e6=self.size_e6,
            order_type=self.order_type,
            tif=self.tif,
            urgency=self.urgency,
            max_slippage_bps=self.max_slippage_bps,
            tag=self.strategy_id,
            expires_at=None,
        )
        sell = PlaceOrderIntent(
            market_id=event.market_id,
            side="sell",
            price_e6=event.ask_px_e6,
            size_e6=self.size_e6,
            order_type=self.order_type,
            tif=self.tif,
            urgency=self.urgency,
            max_slippage_bps=self.max_slippage_bps,
            tag=self.strategy_id,
            expires_at=None,
        )
        return [buy, sell]

    def on_timer(self, ctx: StrategyContext, ts_ns: int) -> list[Intent]:
        return []
