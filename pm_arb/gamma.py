from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests


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


def fetch_markets(
    base_url: str, timeout: float, limit: int = 100, max_markets: int | None = None
) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/markets"
    offset = 0
    markets: list[dict[str, Any]] = []
    while True:
        params = {"closed": "false", "limit": limit, "offset": offset}
        resp = requests.get(url, params=params, timeout=timeout)
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


def load_markets_fixture(path: str | Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict) and "markets" in data:
        return list(data["markets"])
    if isinstance(data, list):
        return data
    raise ValueError("unexpected Gamma fixture shape")
