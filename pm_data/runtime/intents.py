from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(slots=True)
class PlaceOrderIntent:
    market_id: str
    side: Literal["buy", "sell"]
    price_e6: int
    size_e6: int
    order_type: Literal["limit", "market"]
    tif: Literal["gtc", "ioc", "gtd"]
    urgency: Literal["maker", "taker"]
    max_slippage_bps: int | None
    tag: str
    expires_at: int | None
    strategy_id: str | None = None


@dataclass(slots=True)
class CancelIntent:
    order_id: str | None
    market_id: str | None
    tag: str | None
    reason: str
    strategy_id: str | None = None


Intent = PlaceOrderIntent | CancelIntent
