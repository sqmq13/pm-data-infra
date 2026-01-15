from __future__ import annotations

from dataclasses import dataclass, field


def _notional_e6(price_e6: int, size_e6: int, *, round_up: bool) -> int:
    raw = price_e6 * size_e6
    if round_up:
        return (raw + 999_999) // 1_000_000
    return raw // 1_000_000


def _avg_price_e6(position_e6: int, cost_e6: int) -> int:
    if position_e6 == 0:
        return 0
    if position_e6 > 0:
        return (cost_e6 * 1_000_000 + position_e6 - 1) // position_e6
    abs_pos = abs(position_e6)
    return (abs(cost_e6) * 1_000_000) // abs_pos


@dataclass(slots=True)
class MarketLedger:
    position_e6: int = 0
    cost_e6: int = 0
    last_bid_e6: int | None = None
    last_ask_e6: int | None = None


@dataclass(slots=True)
class Ledger:
    cash_e6: int = 0
    realized_pnl_e6: int = 0
    fees_paid_e6: int = 0
    markets: dict[str, MarketLedger] = field(default_factory=dict)

    def _market(self, market_id: str) -> MarketLedger:
        market = self.markets.get(market_id)
        if market is None:
            market = MarketLedger()
            self.markets[market_id] = market
        return market

    def update_market(
        self,
        market_id: str,
        bid_px_e6: int | None,
        ask_px_e6: int | None,
    ) -> None:
        market = self._market(market_id)
        if bid_px_e6 is not None:
            market.last_bid_e6 = bid_px_e6
        if ask_px_e6 is not None:
            market.last_ask_e6 = ask_px_e6

    def apply_fill(
        self,
        *,
        market_id: str,
        side: str,
        price_e6: int,
        size_e6: int,
        fee_e6: int,
    ) -> None:
        if size_e6 <= 0 or price_e6 <= 0:
            return
        market = self._market(market_id)
        notional_e6 = _notional_e6(price_e6, size_e6, round_up=side == "buy")
        if side == "buy":
            self.cash_e6 -= notional_e6 + fee_e6
        else:
            self.cash_e6 += notional_e6 - fee_e6
        self.fees_paid_e6 += fee_e6

        pos = market.position_e6
        cost = market.cost_e6
        if side == "buy":
            if pos >= 0:
                pos += size_e6
                cost += notional_e6
            else:
                cover = min(size_e6, -pos)
                avg_price_e6 = _avg_price_e6(pos, cost)
                pnl_e6 = (avg_price_e6 - price_e6) * cover // 1_000_000
                self.realized_pnl_e6 += pnl_e6
                cost += _notional_e6(avg_price_e6, cover, round_up=False)
                pos += cover
                remaining = size_e6 - cover
                if remaining > 0:
                    pos += remaining
                    cost += _notional_e6(price_e6, remaining, round_up=True)
        else:
            if pos <= 0:
                pos -= size_e6
                cost -= notional_e6
            else:
                sell = min(size_e6, pos)
                avg_price_e6 = _avg_price_e6(pos, cost)
                pnl_e6 = (price_e6 - avg_price_e6) * sell // 1_000_000
                self.realized_pnl_e6 += pnl_e6
                cost -= _notional_e6(avg_price_e6, sell, round_up=True)
                pos -= sell
                remaining = size_e6 - sell
                if remaining > 0:
                    pos -= remaining
                    cost -= _notional_e6(price_e6, remaining, round_up=False)

        market.position_e6 = pos
        market.cost_e6 = cost

    def unrealized_pnl_e6(self) -> int:
        total = 0
        for market in self.markets.values():
            if market.position_e6 == 0:
                continue
            if market.position_e6 > 0:
                mark = market.last_bid_e6
            else:
                mark = market.last_ask_e6
            if mark is None:
                continue
            # Conservative mark: bid for longs, ask for shorts.
            mark_notional = (mark * market.position_e6) // 1_000_000
            total += mark_notional - market.cost_e6
        return total

    def summary(self) -> dict[str, object]:
        unrealized = self.unrealized_pnl_e6()
        positions = {
            market_id: market.position_e6
            for market_id, market in self.markets.items()
            if market.position_e6 != 0
        }
        return {
            "cash_e6": self.cash_e6,
            "realized_pnl_e6": self.realized_pnl_e6,
            "unrealized_pnl_e6": unrealized,
            "fees_paid_e6": self.fees_paid_e6,
            "total_pnl_e6": self.realized_pnl_e6 + unrealized - self.fees_paid_e6,
            "positions_e6": positions,
        }
