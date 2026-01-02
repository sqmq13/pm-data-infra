from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .fixed import PRICE_SCALE, parse_price_to_micro, parse_size_to_units


class BookParseError(ValueError):
    pass


def _parse_level(level: Any, price_scale: int) -> tuple[int, int]:
    if isinstance(level, dict):
        price = level.get("price")
        size = level.get("size") if "size" in level else level.get("amount")
    elif isinstance(level, (list, tuple)) and len(level) >= 2:
        price, size = level[0], level[1]
    else:
        raise BookParseError(f"invalid level: {level}")
    try:
        price_micro = parse_price_to_micro(str(price), scale=price_scale)
        size_units = parse_size_to_units(str(size))
    except ValueError as exc:
        raise BookParseError(str(exc)) from exc
    return price_micro, size_units


def normalize_asks(asks: list[tuple[int, int]], top_k: int) -> list[tuple[int, int]]:
    levels: dict[int, int] = {}
    for price_micro, size_units in asks:
        if price_micro <= 0 or size_units <= 0:
            continue
        levels[price_micro] = levels.get(price_micro, 0) + size_units
    merged = sorted(levels.items(), key=lambda x: x[0])
    if top_k > 0:
        merged = merged[:top_k]
    return merged


def parse_ws_message(
    message: Any, price_scale: int = PRICE_SCALE
) -> tuple[str, list[tuple[int, int]], int | None]:
    if isinstance(message, list):
        last_error: Exception | None = None
        for item in message:
            try:
                return parse_ws_message(item, price_scale=price_scale)
            except BookParseError as exc:
                last_error = exc
        raise BookParseError("no decodable book in list") from last_error
    if not isinstance(message, dict):
        raise BookParseError("invalid message type")
    asset_id = message.get("asset_id") or message.get("assetId") or message.get("token_id")
    if not asset_id:
        raise BookParseError("missing asset id")
    asks_raw = message.get("asks")
    if asks_raw is None:
        raise BookParseError("missing asks")
    asks = [_parse_level(level, price_scale) for level in asks_raw]
    ts_raw = message.get("ts") or message.get("timestamp")
    ts = None
    if ts_raw is not None:
        try:
            ts = int(ts_raw)
        except (TypeError, ValueError):
            ts = None
    return str(asset_id), asks, ts


def parse_rest_book(book: dict[str, Any], price_scale: int = PRICE_SCALE) -> list[tuple[int, int]]:
    asks_raw = book.get("asks")
    if asks_raw is None:
        raise BookParseError("missing asks")
    asks = [_parse_level(level, price_scale) for level in asks_raw]
    return asks


@dataclass
class OrderBook:
    token_id: str
    asks: list[tuple[int, int]] = field(default_factory=list)
    last_ts: int | None = None

    def update_from_asks(self, asks: list[tuple[int, int]], top_k: int) -> None:
        self.asks = normalize_asks(asks, top_k)

    def update_from_ws(self, message: dict[str, Any], top_k: int, price_scale: int = PRICE_SCALE) -> int | None:
        token_id, asks, ts = parse_ws_message(message, price_scale=price_scale)
        if token_id != self.token_id:
            raise BookParseError(f"unexpected token id: {token_id}")
        self.update_from_asks(asks, top_k)
        self.last_ts = ts if ts is not None else self.last_ts
        return ts

    def update_from_rest(self, book: dict[str, Any], top_k: int, price_scale: int = PRICE_SCALE) -> None:
        asks = parse_rest_book(book, price_scale=price_scale)
        self.update_from_asks(asks, top_k)

    def max_fillable_units(self) -> int:
        return sum(size for _, size in self.asks)

    def best_ask_micro(self) -> int | None:
        if not self.asks:
            return None
        return self.asks[0][0]

    def worst_price_for_units(self, units: int) -> int | None:
        remaining = units
        worst = None
        for price, size in self.asks:
            if remaining <= 0:
                break
            take = min(size, remaining)
            if take > 0:
                worst = price
            remaining -= take
        return worst
