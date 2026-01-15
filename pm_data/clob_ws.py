from __future__ import annotations

from typing import Any


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
