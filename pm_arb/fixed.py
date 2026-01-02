from __future__ import annotations

from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, getcontext

PRICE_SCALE = 1_000_000  # micro-dollars per dollar
SIZE_SCALE = 1_000_000  # micro-shares per share


def _to_decimal(value: str) -> Decimal:
    if value is None:
        raise ValueError("value is None")
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError) as exc:
        raise ValueError(f"invalid decimal: {value}") from exc


def parse_price_to_micro(value: str, scale: int = PRICE_SCALE) -> int:
    """Parse decimal price string to integer micro-dollars, rounding up."""
    getcontext().prec = 28
    dec = _to_decimal(value)
    if dec <= 0:
        raise ValueError(f"price must be > 0: {value}")
    micro = (dec * scale).to_integral_value(rounding=ROUND_CEILING)
    return int(micro)


def parse_size_to_units(value: str, scale: int = SIZE_SCALE) -> int:
    """Parse decimal size to integer micro-shares, rounding down (conservative)."""
    getcontext().prec = 28
    dec = _to_decimal(value)
    if dec <= 0:
        raise ValueError(f"size must be > 0: {value}")
    units = (dec * scale).to_integral_value(rounding=ROUND_FLOOR)
    if units <= 0:
        raise ValueError(f"size too small after scaling: {value}")
    return int(units)


def parse_sizes_list(value: str) -> list[int]:
    """Parse comma-separated sizes in whole shares."""
    if value is None:
        raise ValueError("sizes must be provided")
    sizes: list[int] = []
    for item in str(value).split(","):
        item = item.strip()
        if not item:
            continue
        try:
            dec = _to_decimal(item)
        except ValueError as exc:
            raise ValueError(f"invalid size: {item}") from exc
        if dec <= 0:
            raise ValueError(f"size must be > 0: {item}")
        if dec != dec.to_integral_value():
            raise ValueError(f"size must be whole shares: {item}")
        sizes.append(int(dec))
    if not sizes:
        raise ValueError("no sizes parsed")
    return sizes


def units_to_shares(units: int, scale: int = SIZE_SCALE) -> int:
    return int(units // scale)


def micro_to_cents(micro_dollars: int) -> float:
    return micro_dollars / 10_000.0
