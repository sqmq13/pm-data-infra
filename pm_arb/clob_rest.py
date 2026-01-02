from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests


class RestError(RuntimeError):
    pass


@dataclass
class RestRateLimiter:
    rate_per_sec: int
    burst: int
    tokens: float = 0.0
    last_ts: float = 0.0

    def allow(self) -> None:
        now = time.time()
        if self.last_ts == 0.0:
            self.last_ts = now
        elapsed = max(0.0, now - self.last_ts)
        self.tokens = min(self.burst, self.tokens + elapsed * self.rate_per_sec)
        self.last_ts = now
        if self.tokens < 1.0:
            sleep_for = (1.0 - self.tokens) / max(self.rate_per_sec, 1)
            time.sleep(sleep_for)
            self.tokens = 0.0
            self.last_ts = time.time()
        else:
            self.tokens -= 1.0


class RestClient:
    def __init__(self, base_url: str, timeout: float, rate_per_sec: int, burst: int):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.limiter = RestRateLimiter(rate_per_sec=rate_per_sec, burst=burst)

    def fetch_book(self, token_id: str) -> dict[str, Any]:
        self.limiter.allow()
        url = f"{self.base_url}/book"
        resp = requests.get(url, params={"token_id": token_id}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()


def fetch_book_with_retries(
    client: RestClient, token_id: str, retry_max: int
) -> dict[str, Any]:
    last_err = None
    for _ in range(retry_max):
        try:
            return client.fetch_book(token_id)
        except (requests.RequestException, ValueError) as exc:
            last_err = exc
            time.sleep(0.2)
    raise RestError(f"REST book failed for {token_id}: {last_err}")


def load_rest_book_fixture(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        data = json.load(handle)
    if not isinstance(data, dict):
        raise ValueError("expected dict mapping token_id to book")
    return data
