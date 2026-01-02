from __future__ import annotations

import asyncio
import json
from typing import Any, Callable

import websockets


def build_handshake_payload(asset_ids: list[str]) -> dict[str, Any]:
    return {"assets_ids": asset_ids, "type": "market"}


def build_subscribe_payload(variant: str, asset_ids: list[str]) -> dict[str, Any]:
    if variant == "A":
        return {"type": "market", "assets_ids": asset_ids, "operation": "subscribe"}
    if variant == "B":
        return {"type": "MARKET", "assets_ids": asset_ids, "operation": "subscribe"}
    if variant == "C":
        return {"type": "market", "assetsIds": asset_ids, "operation": "subscribe"}
    raise ValueError(f"unknown subscribe variant: {variant}")


async def _read_until_decodable(
    ws, timeout: float, decode_fn: Callable[[Any], Any]
) -> Any:
    end = asyncio.get_event_loop().time() + timeout
    while True:
        remaining = end - asyncio.get_event_loop().time()
        if remaining <= 0:
            raise TimeoutError("timeout waiting for book after handshake")
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="ignore")
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        try:
            decode_fn(msg)
            return msg
        except Exception:
            continue


async def _try_payload(
    url: str,
    payload: dict[str, Any],
    timeout: float,
    decode_fn: Callable[[Any], Any],
) -> Any:
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps(payload))
        return await _read_until_decodable(ws, timeout, decode_fn)


async def wait_for_decodable_book(
    url: str,
    asset_ids: list[str],
    timeout: float,
    decode_fn: Callable[[Any], Any],
    fallback_variants: tuple[str, ...] = ("A", "B", "C"),
) -> tuple[dict[str, Any], Any, list[dict[str, Any]]]:
    attempted: list[dict[str, Any]] = []
    handshake_payload = build_handshake_payload(asset_ids)
    attempted.append(handshake_payload)
    try:
        msg = await _try_payload(url, handshake_payload, timeout, decode_fn)
        return handshake_payload, msg, attempted
    except TimeoutError:
        pass
    last_error: Exception | None = None
    for variant in fallback_variants:
        payload = build_subscribe_payload(variant, asset_ids)
        attempted.append(payload)
        try:
            msg = await _try_payload(url, payload, timeout, decode_fn)
            return payload, msg, attempted
        except Exception as exc:
            last_error = exc
    message = f"timeout waiting for book after handshake; ws_url={url}; payloads={attempted}"
    raise TimeoutError(message) from last_error
