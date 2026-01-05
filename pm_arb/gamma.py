from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests

DEFAULT_USER_AGENT = "pm-arb-py/phase1"


@dataclass(frozen=True)
class UniverseSnapshot:
    universe_version: int
    market_ids: list[str]
    token_ids: list[str]
    created_wall_ns_utc: int
    created_mono_ns: int
    selection: dict[str, Any]


def parse_clob_token_ids(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item) for item in value if str(item)]
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        try:
            parsed = json.loads(text)
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item)]
            if isinstance(parsed, str):
                text = parsed
        except json.JSONDecodeError:
            pass
        tokens = [item.strip().strip('"').strip("'") for item in text.split(",")]
        return [token for token in tokens if token]
    return []


def select_active_binary_markets(
    markets: list[dict[str, Any]],
    *,
    max_markets: int,
) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    enable_key = "enable" + "Order" + "Book"
    for market in markets:
        active = market.get("active")
        if active is not None and not active:
            continue
        if market.get(enable_key) is False:
            continue
        token_ids = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        if len(token_ids) != 2:
            continue
        selected.append(market)
        if len(selected) >= max_markets:
            break
    return selected


def _sort_markets_deterministic(markets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    parsed: list[tuple[int, dict[str, Any]]] = []
    for idx, market in enumerate(markets):
        market_id = market.get("id")
        try:
            market_num = int(market_id)
        except (TypeError, ValueError):
            return list(markets)
        parsed.append((idx, market_num, market))
    parsed.sort(key=lambda item: (-item[1], item[0]))
    return [item[2] for item in parsed]


def compute_desired_universe(
    config,
    *,
    universe_version: int = 0,
) -> UniverseSnapshot:
    markets = fetch_markets(
        config.gamma_base_url,
        config.rest_timeout,
        limit=config.gamma_limit,
        max_markets=config.capture_max_markets,
    )
    ordered_markets = _sort_markets_deterministic(markets)
    selected_markets = select_active_binary_markets(
        ordered_markets,
        max_markets=config.capture_max_markets,
    )
    market_ids: list[str] = []
    token_ids: list[str] = []
    token_seen: set[str] = set()
    for market in selected_markets:
        market_id = market.get("id")
        if market_id is None:
            continue
        market_ids.append(str(market_id))
        token_list = parse_clob_token_ids(
            market.get("clobTokenIds") or market.get("clob_token_ids")
        )
        for token_id in token_list:
            token_key = str(token_id)
            if token_key in token_seen:
                continue
            token_seen.add(token_key)
            token_ids.append(token_key)
    selection = {
        "max_markets": config.capture_max_markets,
        "filters_enabled": bool(getattr(config, "capture_use_market_filters", False)),
    }
    return UniverseSnapshot(
        universe_version=universe_version,
        market_ids=market_ids,
        token_ids=token_ids,
        created_wall_ns_utc=time.time_ns(),
        created_mono_ns=time.perf_counter_ns(),
        selection=selection,
    )


def fetch_markets(
    base_url: str,
    timeout: float,
    limit: int = 100,
    max_markets: int | None = None,
    params_override: dict[str, Any] | None = None,
    session: requests.Session | None = None,
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/markets"
    offset = 0
    markets: list[dict[str, Any]] = []
    created_session = False
    if session is None:
        session = requests.Session()
        created_session = True
    try:
        while True:
            params = {
                "order": "id",
                "ascending": "false",
                "limit": limit,
                "offset": offset,
            }
            if params_override:
                params.update(params_override)
            resp = session.get(
                url,
                params=params,
                timeout=timeout,
                headers={"User-Agent": DEFAULT_USER_AGENT},
            )
            resp.raise_for_status()
            data = resp.json()
            if isinstance(data, dict) and "markets" in data:
                page = list(data["markets"])
            elif isinstance(data, list):
                page = data
            else:
                raise ValueError("unexpected Gamma response shape")
            if not page:
                break
            markets.extend(page)
            if max_markets is not None and len(markets) >= max_markets:
                return markets[:max_markets]
            offset += limit
        return markets
    finally:
        if created_session:
            session.close()


def load_markets_fixture(path: str | Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict) and "markets" in data:
        return list(data["markets"])
    if isinstance(data, list):
        return data
    raise ValueError("unexpected Gamma fixture shape")
