from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import requests


def fetch_markets(base_url: str, timeout: float) -> list[dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/markets"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict) and "markets" in data:
        return list(data["markets"])
    if isinstance(data, list):
        return data
    raise ValueError("unexpected Gamma response shape")


def load_markets_fixture(path: str | Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict) and "markets" in data:
        return list(data["markets"])
    if isinstance(data, list):
        return data
    raise ValueError("unexpected Gamma fixture shape")
