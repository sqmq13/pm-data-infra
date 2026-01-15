from __future__ import annotations

from dataclasses import dataclass, field

from .events import TopOfBookUpdate


@dataclass(slots=True)
class MarketState:
    market_id: str
    best_bid_px_e6: int | None = None
    best_bid_sz_e6: int | None = None
    best_ask_px_e6: int | None = None
    best_ask_sz_e6: int | None = None
    status: str | None = None
    last_update_seq: int = 0

    def apply_top_of_book(self, event: TopOfBookUpdate) -> None:
        if event.seq <= self.last_update_seq:
            return
        self.best_bid_px_e6 = event.bid_px_e6
        self.best_bid_sz_e6 = event.bid_sz_e6
        self.best_ask_px_e6 = event.ask_px_e6
        self.best_ask_sz_e6 = event.ask_sz_e6
        self.last_update_seq = event.seq


@dataclass(slots=True)
class GlobalState:
    markets: dict[str, MarketState] = field(default_factory=dict)
    universe_tokens: set[str] = field(default_factory=set)
    open_orders: dict[str, object] = field(default_factory=dict)
    positions: dict[str, int] = field(default_factory=dict)
    balances: dict[str, int] = field(default_factory=dict)

    def get_market(self, market_id: str) -> MarketState:
        market = self.markets.get(market_id)
        if market is None:
            market = MarketState(market_id=market_id)
            self.markets[market_id] = market
        return market
