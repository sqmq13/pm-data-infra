from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable

import orjson

from .events import TopOfBookUpdate
from .replay import RawFrame

_E6 = Decimal("1000000")


def to_e6(value: str | float | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError("invalid numeric type")
    if isinstance(value, int):
        return value * 1_000_000
    if isinstance(value, float):
        raw = Decimal(str(value))
    elif isinstance(value, str):
        raw = Decimal(value.strip())
    else:
        raw = Decimal(str(value))
    scaled = raw * _E6
    quantized = scaled.to_integral_value(rounding=ROUND_HALF_UP)
    return int(quantized)


def _iter_items(payload: object) -> Iterable[dict]:
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item
    elif isinstance(payload, dict):
        yield payload


def _get_token_id(item: dict) -> str | None:
    token_id = item.get("asset_id")
    if token_id is None:
        token_id = item.get("token_id") or item.get("tokenId") or item.get("assetId")
    if token_id is None:
        return None
    return str(token_id)


def _to_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_level(level: object) -> tuple[int, int] | None:
    if not isinstance(level, dict):
        return None
    price = level.get("price")
    size = level.get("size")
    try:
        price_e6 = to_e6(price)
        size_e6 = to_e6(size)
    except (InvalidOperation, ValueError):
        return None
    if price_e6 is None or size_e6 is None:
        return None
    if price_e6 <= 0 or size_e6 <= 0:
        return None
    return price_e6, size_e6


def _scan_best(levels: list, *, best_is_max: bool) -> tuple[int | None, int | None]:
    best: tuple[int, int] | None = None
    for level in levels:
        parsed = _parse_level(level)
        if parsed is None:
            continue
        if best is None:
            best = parsed
            continue
        if best_is_max:
            if parsed[0] > best[0]:
                best = parsed
        else:
            if parsed[0] < best[0]:
                best = parsed
    if best is None:
        return None, None
    return best


def _best_bid(levels: list) -> tuple[int | None, int | None]:
    if not levels:
        return None, None
    first = _parse_level(levels[0])
    if len(levels) >= 2:
        second = _parse_level(levels[1])
        if first is not None and second is not None and first[0] < second[0]:
            return _scan_best(levels, best_is_max=True)
    if first is not None:
        return first
    return _scan_best(levels, best_is_max=True)


def _best_ask(levels: list) -> tuple[int | None, int | None]:
    if not levels:
        return None, None
    first = _parse_level(levels[0])
    if len(levels) >= 2:
        second = _parse_level(levels[1])
        if first is not None and second is not None and first[0] > second[0]:
            return _scan_best(levels, best_is_max=False)
    if first is not None:
        return first
    return _scan_best(levels, best_is_max=False)


@dataclass(slots=True)
class Normalizer:
    decode_errors: int = 0

    def normalize(self, frame: RawFrame) -> list[TopOfBookUpdate]:
        payload_bytes = frame.payload
        if payload_bytes in (b"PONG", b"PING"):
            return []
        try:
            payload = orjson.loads(payload_bytes)
        except orjson.JSONDecodeError:
            self.decode_errors += 1
            return []

        events: list[TopOfBookUpdate] = []
        for item in _iter_items(payload):
            if "bids" not in item and "asks" not in item:
                continue
            market_id = _get_token_id(item)
            if market_id is None:
                continue
            bids = item.get("bids") or []
            asks = item.get("asks") or []
            bid_px_e6, bid_sz_e6 = _best_bid(bids)
            ask_px_e6, ask_sz_e6 = _best_ask(asks)
            if bid_px_e6 is None and ask_px_e6 is None:
                continue
            ts_event = _to_int(item.get("timestamp"))
            events.append(
                TopOfBookUpdate(
                    market_id=market_id,
                    bid_px_e6=bid_px_e6,
                    bid_sz_e6=bid_sz_e6,
                    ask_px_e6=ask_px_e6,
                    ask_sz_e6=ask_sz_e6,
                    ts_event=ts_event,
                    ts_recv=frame.rx_mono_ns,
                    seq=0,
                )
            )
        return events
