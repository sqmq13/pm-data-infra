from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Protocol, Sequence

from .intents import CancelIntent, Intent, PlaceOrderIntent
from .state import GlobalState


@dataclass(slots=True)
class SimExecutionConfig:
    taker_slippage_bps: int = 10
    max_fill_size_e6: int | None = None
    default_fee_bps: int = 100


class FeeModel(Protocol):
    def fee_bps(
        self,
        *,
        market_id: str,
        side: str,
        price_e6: int,
        size_e6: int,
        is_taker: bool,
    ) -> int:
        ...


@dataclass(slots=True)
class FlatFeeModel:
    fee_bps_value: int = 0

    def fee_bps(
        self,
        *,
        market_id: str,
        side: str,
        price_e6: int,
        size_e6: int,
        is_taker: bool,
    ) -> int:
        del market_id, side, price_e6, size_e6, is_taker
        return self.fee_bps_value


@dataclass(slots=True)
class AckEvent:
    kind: Literal["ack"]
    order_id: str
    market_id: str | None
    intent_tag: str | None
    ts_ns: int


@dataclass(slots=True)
class FillEvent:
    kind: Literal["fill"]
    order_id: str
    market_id: str
    side: Literal["buy", "sell"]
    price_e6: int
    size_e6: int
    fee_e6: int
    ts_ns: int
    intent_tag: str | None


ExecutionEvent = AckEvent | FillEvent


def _notional_e6(price_e6: int, size_e6: int, *, round_up: bool) -> int:
    raw = price_e6 * size_e6
    if round_up:
        return (raw + 999_999) // 1_000_000
    return raw // 1_000_000


def _slippage_e6(price_e6: int, slippage_bps: int, *, round_up: bool) -> int:
    raw = price_e6 * slippage_bps
    if round_up:
        return (raw + 9_999) // 10_000
    return raw // 10_000


class SimExecutionBackend:
    def __init__(
        self,
        *,
        state: GlobalState,
        config: SimExecutionConfig | None = None,
        fee_model: FeeModel | None = None,
    ) -> None:
        self._state = state
        self._config = config or SimExecutionConfig()
        self._fee_model = fee_model or FlatFeeModel(self._config.default_fee_bps)
        self._next_order_id = 1

    def submit(self, intents: Sequence[Intent], ts_ns: int) -> list[ExecutionEvent]:
        events: list[ExecutionEvent] = []
        for intent in intents:
            order_id = f"sim-{self._next_order_id:08d}"
            self._next_order_id += 1
            market_id: str | None
            intent_tag: str | None
            if isinstance(intent, PlaceOrderIntent):
                market_id = intent.market_id
                intent_tag = intent.tag
            elif isinstance(intent, CancelIntent):
                market_id = intent.market_id
                intent_tag = intent.tag
                if intent.order_id is not None:
                    order_id = intent.order_id
            else:
                market_id = None
                intent_tag = None
            events.append(
                AckEvent(
                    kind="ack",
                    order_id=order_id,
                    market_id=market_id,
                    intent_tag=intent_tag,
                    ts_ns=ts_ns,
                )
            )
            if not isinstance(intent, PlaceOrderIntent):
                continue
            if intent.urgency != "taker":
                continue
            market_state = self._state.get_market(intent.market_id)
            max_slip_bps = self._config.taker_slippage_bps
            if intent.max_slippage_bps is not None:
                max_slip_bps = min(max_slip_bps, intent.max_slippage_bps)
            if intent.side == "buy":
                book_px = market_state.best_ask_px_e6
                book_sz = market_state.best_ask_sz_e6
                if book_px is None or book_sz is None:
                    continue
                if intent.order_type == "limit" and intent.price_e6 < book_px:
                    continue
                slip = _slippage_e6(book_px, max_slip_bps, round_up=True)
                fill_px_e6 = book_px + slip
            else:
                book_px = market_state.best_bid_px_e6
                book_sz = market_state.best_bid_sz_e6
                if book_px is None or book_sz is None:
                    continue
                if intent.order_type == "limit" and intent.price_e6 > book_px:
                    continue
                slip = _slippage_e6(book_px, max_slip_bps, round_up=False)
                fill_px_e6 = max(0, book_px - slip)

            fill_sz_e6 = min(intent.size_e6, book_sz)
            if self._config.max_fill_size_e6 is not None:
                fill_sz_e6 = min(fill_sz_e6, self._config.max_fill_size_e6)
            if fill_sz_e6 <= 0:
                continue
            fee_bps = self._fee_model.fee_bps(
                market_id=intent.market_id,
                side=intent.side,
                price_e6=fill_px_e6,
                size_e6=fill_sz_e6,
                is_taker=True,
            )
            notional_e6 = _notional_e6(
                fill_px_e6,
                fill_sz_e6,
                round_up=intent.side == "buy",
            )
            fee_e6 = (notional_e6 * fee_bps + 9_999) // 10_000
            events.append(
                FillEvent(
                    kind="fill",
                    order_id=order_id,
                    market_id=intent.market_id,
                    side=intent.side,
                    price_e6=fill_px_e6,
                    size_e6=fill_sz_e6,
                    fee_e6=fee_e6,
                    ts_ns=ts_ns,
                    intent_tag=intent.tag,
                )
            )
        return events
