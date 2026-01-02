from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import websockets


def build_subscribe_payload(variant: str, asset_ids: list[str]) -> dict[str, Any]:
    if variant == "A":
        return {"type": "market", "assets_ids": asset_ids, "operation": "subscribe"}
    if variant == "B":
        return {"type": "MARKET", "assets_ids": asset_ids, "operation": "subscribe"}
    if variant == "C":
        return {"type": "market", "assetsIds": asset_ids, "operation": "subscribe"}
    raise ValueError(f"unknown subscribe variant: {variant}")


async def wait_for_decodable_book(
    url: str,
    asset_ids: list[str],
    variant: str,
    timeout: float,
    decode_fn: Callable[[dict[str, Any]], Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    payload = build_subscribe_payload(variant, asset_ids)
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(payload))
        end = asyncio.get_event_loop().time() + timeout
        while True:
            remaining = end - asyncio.get_event_loop().time()
            if remaining <= 0:
                raise TimeoutError(f"no decodable book within {timeout}s")
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            if isinstance(raw, bytes):
                raw = raw.decode("utf-8", errors="ignore")
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            try:
                decode_fn(msg)
                return payload, msg
            except Exception:
                continue
