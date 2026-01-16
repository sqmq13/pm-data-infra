from __future__ import annotations

from dataclasses import dataclass
import time
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Iterable, Iterator

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
    if isinstance(value, str):
        text = value.strip()
        if text:
            if "e" not in text and "E" not in text:
                sign = 1
                pos = 0
                if text[0] == "-":
                    sign = -1
                    pos = 1
                elif text[0] == "+":
                    pos = 1
                if pos >= len(text):
                    raw = Decimal(text)
                    scaled_dec = raw * _E6
                    quantized = scaled_dec.to_integral_value(rounding=ROUND_HALF_UP)
                    return int(quantized)

                whole_val = 0
                frac_val = 0
                frac_digits = 0
                round_digit = -1
                saw_dot = False
                saw_digit = False
                for char in text[pos:]:
                    if "0" <= char <= "9":
                        saw_digit = True
                        digit = ord(char) - 48
                        if not saw_dot:
                            whole_val = whole_val * 10 + digit
                            continue
                        if frac_digits < 6:
                            frac_val = frac_val * 10 + digit
                            frac_digits += 1
                            continue
                        if frac_digits == 6:
                            round_digit = digit
                            frac_digits += 1
                            continue
                        frac_digits += 1
                        continue
                    if char == "." and not saw_dot:
                        saw_dot = True
                        continue
                    raw = Decimal(text)
                    scaled_dec = raw * _E6
                    quantized = scaled_dec.to_integral_value(rounding=ROUND_HALF_UP)
                    return int(quantized)
                if not saw_digit:
                    raw = Decimal(text)
                    scaled_dec = raw * _E6
                    quantized = scaled_dec.to_integral_value(rounding=ROUND_HALF_UP)
                    return int(quantized)
                while frac_digits < 6:
                    frac_val *= 10
                    frac_digits += 1
                scaled = whole_val * 1_000_000 + frac_val
                if round_digit >= 5:
                    scaled += 1
                return sign * scaled
        raw = Decimal(text)
    elif isinstance(value, float):
        raw = Decimal(str(value))
    else:
        raw = Decimal(str(value))
    scaled_dec = raw * _E6
    quantized = scaled_dec.to_integral_value(rounding=ROUND_HALF_UP)
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


def _parse_price_e6(level: object) -> int | None:
    if not isinstance(level, dict):
        return None
    price = level.get("price")
    try:
        price_e6 = to_e6(price)
    except (InvalidOperation, ValueError):
        return None
    if price_e6 is None or price_e6 <= 0:
        return None
    return price_e6


def _parse_size_e6(level: object) -> int | None:
    if not isinstance(level, dict):
        return None
    size = level.get("size")
    try:
        size_e6 = to_e6(size)
    except (InvalidOperation, ValueError):
        return None
    if size_e6 is None or size_e6 <= 0:
        return None
    return size_e6


def _parse_level(level: object) -> tuple[int, int] | None:
    if not isinstance(level, dict):
        return None
    price_e6 = _parse_price_e6(level)
    if price_e6 is None:
        return None
    size_e6 = _parse_size_e6(level)
    if size_e6 is None:
        return None
    return price_e6, size_e6


def _scan_best(levels: list, *, best_is_max: bool) -> tuple[int | None, int | None]:
    best_price: int | None = None
    best_size: int | None = None
    for level in levels:
        price_e6 = _parse_price_e6(level)
        if price_e6 is None:
            continue
        if best_price is not None:
            if best_is_max:
                if price_e6 <= best_price:
                    continue
            else:
                if price_e6 >= best_price:
                    continue
        size_e6 = _parse_size_e6(level)
        if size_e6 is None:
            continue
        best_price = price_e6
        best_size = size_e6
    if best_price is None or best_size is None:
        return None, None
    return best_price, best_size


def _best_bid(levels: list, *, stats: "Normalizer | None" = None) -> tuple[int | None, int | None]:
    if not levels:
        return None, None
    first_price = _parse_price_e6(levels[0])
    if len(levels) >= 2:
        second_price = _parse_price_e6(levels[1])
        if first_price is not None and second_price is not None and first_price < second_price:
            last_price = _parse_price_e6(levels[-1])
            second_last_price = _parse_price_e6(levels[-2])
            mid_price = _parse_price_e6(levels[len(levels) // 2])
            if (
                last_price is not None
                and second_last_price is not None
                and mid_price is not None
                and first_price <= second_price
                and second_price <= mid_price
                and second_last_price <= last_price
                and mid_price <= last_price
            ):
                last_size = _parse_size_e6(levels[-1])
                if last_size is not None:
                    return last_price, last_size
            if stats is not None:
                stats.bid_scan_count += 1
                scan_len = len(levels)
                stats.bid_scan_levels_total += scan_len
                if scan_len > stats.bid_scan_levels_max:
                    stats.bid_scan_levels_max = scan_len
            return _scan_best(levels, best_is_max=True)
    if first_price is not None:
        first_size = _parse_size_e6(levels[0])
        if first_size is not None:
            return first_price, first_size
    if stats is not None:
        stats.bid_scan_count += 1
        scan_len = len(levels)
        stats.bid_scan_levels_total += scan_len
        if scan_len > stats.bid_scan_levels_max:
            stats.bid_scan_levels_max = scan_len
    return _scan_best(levels, best_is_max=True)


def _best_ask(levels: list, *, stats: "Normalizer | None" = None) -> tuple[int | None, int | None]:
    if not levels:
        return None, None
    first_price = _parse_price_e6(levels[0])
    if len(levels) >= 2:
        second_price = _parse_price_e6(levels[1])
        if first_price is not None and second_price is not None and first_price > second_price:
            last_price = _parse_price_e6(levels[-1])
            second_last_price = _parse_price_e6(levels[-2])
            mid_price = _parse_price_e6(levels[len(levels) // 2])
            if (
                last_price is not None
                and second_last_price is not None
                and mid_price is not None
                and first_price >= second_price
                and second_price >= mid_price
                and second_last_price >= last_price
                and mid_price >= last_price
            ):
                last_size = _parse_size_e6(levels[-1])
                if last_size is not None:
                    return last_price, last_size
            if stats is not None:
                stats.ask_scan_count += 1
                scan_len = len(levels)
                stats.ask_scan_levels_total += scan_len
                if scan_len > stats.ask_scan_levels_max:
                    stats.ask_scan_levels_max = scan_len
            return _scan_best(levels, best_is_max=False)
    if first_price is not None:
        first_size = _parse_size_e6(levels[0])
        if first_size is not None:
            return first_price, first_size
    if stats is not None:
        stats.ask_scan_count += 1
        scan_len = len(levels)
        stats.ask_scan_levels_total += scan_len
        if scan_len > stats.ask_scan_levels_max:
            stats.ask_scan_levels_max = scan_len
    return _scan_best(levels, best_is_max=False)


@dataclass(slots=True)
class Normalizer:
    decode_errors: int = 0
    schema_errors: int = 0
    bid_scan_count: int = 0
    bid_scan_levels_total: int = 0
    bid_scan_levels_max: int = 0
    ask_scan_count: int = 0
    ask_scan_levels_total: int = 0
    ask_scan_levels_max: int = 0

    def iter_normalize(self, frame: RawFrame) -> Iterator[TopOfBookUpdate]:
        payload_bytes = frame.payload
        if payload_bytes in (b"PONG", b"PING"):
            return
            yield  # pragma: no cover
        try:
            payload = orjson.loads(payload_bytes)
        except orjson.JSONDecodeError:
            self.decode_errors += 1
            return
            yield  # pragma: no cover

        for item in _iter_items(payload):
            try:
                if "bids" not in item and "asks" not in item:
                    continue
                market_id = _get_token_id(item)
                if market_id is None:
                    continue
                bids: list = []
                asks: list = []
                bids_raw = item.get("bids")
                asks_raw = item.get("asks")
                if isinstance(bids_raw, list):
                    bids = bids_raw
                elif bids_raw is not None:
                    self.schema_errors += 1
                if isinstance(asks_raw, list):
                    asks = asks_raw
                elif asks_raw is not None:
                    self.schema_errors += 1
                bid_px_e6, bid_sz_e6 = _best_bid(bids, stats=self)
                ask_px_e6, ask_sz_e6 = _best_ask(asks, stats=self)
                if bid_px_e6 is None and ask_px_e6 is None:
                    continue
                ts_event = _to_int(item.get("timestamp"))
                yield TopOfBookUpdate(
                    market_id=market_id,
                    bid_px_e6=bid_px_e6,
                    bid_sz_e6=bid_sz_e6,
                    ask_px_e6=ask_px_e6,
                    ask_sz_e6=ask_sz_e6,
                    ts_event=ts_event,
                    ts_recv=frame.rx_mono_ns,
                    seq=0,
                )
            except Exception:
                self.schema_errors += 1
                continue

    def iter_normalize_timed(
        self,
        frame: RawFrame,
        *,
        clock_ns=time.perf_counter_ns,
    ) -> Iterator[tuple[TopOfBookUpdate, int, int]]:
        payload_bytes = frame.payload
        if payload_bytes in (b"PONG", b"PING"):
            return
            yield  # pragma: no cover
        try:
            payload = orjson.loads(payload_bytes)
        except orjson.JSONDecodeError:
            self.decode_errors += 1
            return
            yield  # pragma: no cover
        decode_end_ns = int(clock_ns())

        for item in _iter_items(payload):
            try:
                if "bids" not in item and "asks" not in item:
                    continue
                market_id = _get_token_id(item)
                if market_id is None:
                    continue
                bids: list = []
                asks: list = []
                bids_raw = item.get("bids")
                asks_raw = item.get("asks")
                if isinstance(bids_raw, list):
                    bids = bids_raw
                elif bids_raw is not None:
                    self.schema_errors += 1
                if isinstance(asks_raw, list):
                    asks = asks_raw
                elif asks_raw is not None:
                    self.schema_errors += 1
                bid_px_e6, bid_sz_e6 = _best_bid(bids, stats=self)
                ask_px_e6, ask_sz_e6 = _best_ask(asks, stats=self)
                if bid_px_e6 is None and ask_px_e6 is None:
                    continue
                ts_event = _to_int(item.get("timestamp"))
                event = TopOfBookUpdate(
                    market_id=market_id,
                    bid_px_e6=bid_px_e6,
                    bid_sz_e6=bid_sz_e6,
                    ask_px_e6=ask_px_e6,
                    ask_sz_e6=ask_sz_e6,
                    ts_event=ts_event,
                    ts_recv=frame.rx_mono_ns,
                    seq=0,
                )
                normalize_end_ns = int(clock_ns())
                yield event, decode_end_ns, normalize_end_ns
            except Exception:
                self.schema_errors += 1
                continue

    def normalize_timed(
        self,
        frame: RawFrame,
        *,
        clock_ns=time.perf_counter_ns,
    ) -> tuple[list[TopOfBookUpdate], int, int]:
        events: list[TopOfBookUpdate] = []
        decode_end_ns = 0
        normalize_end_ns = 0
        for event, decode_end_ns, normalize_end_ns in self.iter_normalize_timed(
            frame, clock_ns=clock_ns
        ):
            events.append(event)
        return events, decode_end_ns, normalize_end_ns

    def normalize(self, frame: RawFrame) -> list[TopOfBookUpdate]:
        return list(self.iter_normalize(frame))
