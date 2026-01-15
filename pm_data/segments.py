from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from .gamma import parse_clob_token_ids

CADENCE_BUCKETS = ("5m", "15m", "30m", "60m", "unknown")

CADENCE_MINUTES_FIELDS = (
    "cadence_minutes",
    "cadenceMinutes",
    "intervalMinutes",
    "resolutionMinutes",
    "durationMinutes",
    "windowMinutes",
)

CADENCE_STRING_FIELDS = ("cadence", "interval", "frequency")

CADENCE_PARSE_FIELDS = ("slug", "question", "title", "subtitle", "ticker", "event_ticker")

CADENCE_REGEXES = {
    "5m": [
        re.compile(r"\b5\s*(?:min|minute)s?\b", re.IGNORECASE),
        re.compile(r"\b5m\b", re.IGNORECASE),
    ],
    "15m": [
        re.compile(r"\b15\s*(?:min|minute)s?\b", re.IGNORECASE),
        re.compile(r"\b15m\b", re.IGNORECASE),
    ],
    "30m": [
        re.compile(r"\b30\s*(?:min|minute)s?\b", re.IGNORECASE),
        re.compile(r"\b30m\b", re.IGNORECASE),
    ],
    "60m": [
        re.compile(r"\b60\s*(?:min|minute)s?\b", re.IGNORECASE),
        re.compile(r"\b1\s*hour\b", re.IGNORECASE),
        re.compile(r"\b1h\b", re.IGNORECASE),
    ],
}

CRYPTO_CATEGORY_FIELDS = (
    "category",
    "category_slug",
    "parentCategory",
    "subcategory",
    "tag",
    "tags",
)

NON_CRYPTO_CATEGORY_KEYWORDS = (
    "politic",
    "sport",
    "entertainment",
    "science",
    "culture",
    "health",
    "weather",
    "election",
)

CRYPTO_TICKERS = (
    "BTC",
    "ETH",
    "SOL",
    "XRP",
    "DOGE",
    "ADA",
    "DOT",
    "LTC",
    "BCH",
    "AVAX",
    "MATIC",
    "LINK",
    "UNI",
    "AAVE",
    "OP",
    "ARB",
)

INSTRUMENT_FIELDS = ("ticker", "symbol", "baseAsset", "underlying", "asset")

START_TIME_FIELDS = ("startDate", "startTime", "startTimestamp", "start_date")
END_TIME_FIELDS = (
    "endDate",
    "endTime",
    "endTimestamp",
    "closeTime",
    "closeDate",
    "end_date",
)
RESOLVE_TIME_FIELDS = (
    "resolveTime",
    "resolutionTime",
    "resolvedTime",
    "resolveDate",
    "resolutionDate",
)


@dataclass(frozen=True)
class SegmentTag:
    cadence_bucket: str
    is_crypto: bool | None
    fee_rate_bps: int | None
    fee_enabled: bool | None
    fee_rate_known: bool
    start_wall_ns_utc: int | None
    end_wall_ns_utc: int | None
    resolve_wall_ns_utc: int | None
    rollover_group_key: str | None
    rollover_instrument: str | None
    rollover_boundary_key: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "cadence_bucket": self.cadence_bucket,
            "is_crypto": self.is_crypto,
            "fee_rate_bps": self.fee_rate_bps,
            "fee_enabled": self.fee_enabled,
            "fee_rate_known": self.fee_rate_known,
            "start_wall_ns_utc": self.start_wall_ns_utc,
            "end_wall_ns_utc": self.end_wall_ns_utc,
            "resolve_wall_ns_utc": self.resolve_wall_ns_utc,
            "rollover_group_key": self.rollover_group_key,
            "rollover_instrument": self.rollover_instrument,
            "rollover_boundary_key": self.rollover_boundary_key,
        }


def _coerce_minutes(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            return int(float(text))
        except ValueError:
            return None
    return None


def _parse_timestamp_ns(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if numeric <= 0:
            return None
        if numeric >= 1e16:
            return int(numeric)
        if numeric >= 1e12:
            return int(numeric * 1_000_000)
        if numeric >= 1e9:
            return int(numeric * 1_000_000_000)
        return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp() * 1_000_000_000)
    return None


def _first_timestamp_ns(market: dict[str, Any], fields: Iterable[str]) -> int | None:
    for field in fields:
        if field not in market:
            continue
        ts = _parse_timestamp_ns(market.get(field))
        if ts is not None:
            return ts
    return None


def extract_expiry_times(market: dict[str, Any]) -> tuple[int | None, int | None, int | None]:
    start_ns = _first_timestamp_ns(market, START_TIME_FIELDS)
    end_ns = _first_timestamp_ns(market, END_TIME_FIELDS)
    resolve_ns = _first_timestamp_ns(market, RESOLVE_TIME_FIELDS)
    return start_ns, end_ns, resolve_ns


def _extract_text_fields(market: dict[str, Any], fields: Iterable[str]) -> Iterable[str]:
    for field in fields:
        value = market.get(field)
        if value is None:
            continue
        if isinstance(value, str):
            text = value.strip()
            if text:
                yield text
        elif isinstance(value, (list, tuple)):
            for item in value:
                if isinstance(item, str):
                    text = item.strip()
                    if text:
                        yield text


def _match_buckets(text: str) -> set[str]:
    matched: set[str] = set()
    for bucket, patterns in CADENCE_REGEXES.items():
        if any(pattern.search(text) for pattern in patterns):
            matched.add(bucket)
    return matched


def infer_cadence_bucket(market: dict[str, Any]) -> str:
    for field in CADENCE_MINUTES_FIELDS:
        if field not in market:
            continue
        minutes = _coerce_minutes(market.get(field))
        if minutes in (5, 15, 30, 60):
            return f"{minutes}m"
        if minutes is not None:
            return "unknown"
    for field in CADENCE_STRING_FIELDS:
        value = market.get(field)
        if not isinstance(value, str):
            continue
        matches = _match_buckets(value)
        if len(matches) == 1:
            return next(iter(matches))
        if len(matches) > 1:
            return "unknown"
    matched: set[str] = set()
    for text in _extract_text_fields(market, CADENCE_PARSE_FIELDS):
        matched |= _match_buckets(text)
        if len(matched) > 1:
            return "unknown"
    if len(matched) == 1:
        return next(iter(matched))
    return "unknown"


def infer_is_crypto(market: dict[str, Any]) -> bool | None:
    explicit = market.get("is_crypto")
    if isinstance(explicit, bool):
        return explicit
    category_text = " ".join(_extract_text_fields(market, CRYPTO_CATEGORY_FIELDS)).lower()
    if "crypto" in category_text:
        return True
    if category_text and any(word in category_text for word in NON_CRYPTO_CATEGORY_KEYWORDS):
        return False
    ticker_matches = _infer_crypto_ticker(market)
    if ticker_matches:
        return True
    return None


def _infer_crypto_ticker(market: dict[str, Any]) -> set[str]:
    matches: set[str] = set()
    fields = list(INSTRUMENT_FIELDS) + list(CADENCE_PARSE_FIELDS)
    for text in _extract_text_fields(market, fields):
        for ticker in CRYPTO_TICKERS:
            pattern = re.compile(rf"\b{re.escape(ticker)}\b", re.IGNORECASE)
            if pattern.search(text):
                matches.add(ticker)
    return matches


def infer_instrument(market: dict[str, Any]) -> str | None:
    for field in INSTRUMENT_FIELDS:
        value = market.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()
    matches = _infer_crypto_ticker(market)
    if len(matches) == 1:
        return next(iter(matches))
    return None


def cadence_boundary_key(cadence_bucket: str) -> str | None:
    if cadence_bucket == "unknown":
        return None
    if cadence_bucket == "60m":
        return "1h"
    return cadence_bucket


def _fee_fields_from_result(result: Any) -> tuple[int | None, bool, bool | None]:
    if result is None:
        return None, False, None
    if isinstance(result, dict):
        fee_rate_bps = result.get("fee_rate_bps")
        fee_rate_known = bool(result.get("fee_rate_known"))
        fee_enabled = result.get("fee_enabled")
        return fee_rate_bps, fee_rate_known, fee_enabled
    fee_rate_bps = getattr(result, "fee_rate_bps", None)
    fee_rate_known = bool(getattr(result, "fee_rate_known", False))
    fee_enabled = getattr(result, "fee_enabled", None)
    return fee_rate_bps, fee_rate_known, fee_enabled


def build_segment_maps(
    markets: list[dict[str, Any]],
    fee_results_by_token: dict[str, Any] | None = None,
) -> tuple[dict[str, SegmentTag], dict[str, SegmentTag]]:
    fee_results_by_token = fee_results_by_token or {}
    token_map: dict[str, SegmentTag] = {}
    market_map: dict[str, SegmentTag] = {}
    for market in markets:
        market_id = market.get("id")
        if market_id is None:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        cadence_bucket = infer_cadence_bucket(market)
        is_crypto = infer_is_crypto(market)
        start_ns, end_ns, resolve_ns = extract_expiry_times(market)
        instrument = infer_instrument(market)
        boundary_key = cadence_boundary_key(cadence_bucket)
        rollover_group_key = None
        if boundary_key is not None:
            instrument_key = instrument or "unknown"
            rollover_group_key = f"{cadence_bucket}:{instrument_key}:{boundary_key}"

        token_tags: list[SegmentTag] = []
        for token_id in token_ids:
            fee_rate_bps, fee_rate_known, fee_enabled = _fee_fields_from_result(
                fee_results_by_token.get(str(token_id))
            )
            tag = SegmentTag(
                cadence_bucket=cadence_bucket,
                is_crypto=is_crypto,
                fee_rate_bps=fee_rate_bps,
                fee_enabled=fee_enabled,
                fee_rate_known=fee_rate_known,
                start_wall_ns_utc=start_ns,
                end_wall_ns_utc=end_ns,
                resolve_wall_ns_utc=resolve_ns,
                rollover_group_key=rollover_group_key,
                rollover_instrument=instrument,
                rollover_boundary_key=boundary_key,
            )
            token_map[str(token_id)] = tag
            token_tags.append(tag)

        market_fee_rate_bps = None
        market_fee_rate_known = False
        market_fee_enabled = None
        if len(token_tags) == 2 and all(tag.fee_rate_known for tag in token_tags):
            if token_tags[0].fee_rate_bps == token_tags[1].fee_rate_bps:
                market_fee_rate_bps = token_tags[0].fee_rate_bps
                market_fee_rate_known = True
                market_fee_enabled = token_tags[0].fee_enabled

        market_map[str(market_id)] = SegmentTag(
            cadence_bucket=cadence_bucket,
            is_crypto=is_crypto,
            fee_rate_bps=market_fee_rate_bps,
            fee_enabled=market_fee_enabled,
            fee_rate_known=market_fee_rate_known,
            start_wall_ns_utc=start_ns,
            end_wall_ns_utc=end_ns,
            resolve_wall_ns_utc=resolve_ns,
            rollover_group_key=rollover_group_key,
            rollover_instrument=instrument,
            rollover_boundary_key=boundary_key,
        )

    return token_map, market_map

