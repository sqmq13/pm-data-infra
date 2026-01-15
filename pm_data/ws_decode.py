from __future__ import annotations

import orjson
from dataclasses import dataclass
from typing import Any


@dataclass
class WsDecodeResult:
    raw_text: str
    payload: Any | None
    is_pong: bool
    json_error: bool
    empty_array: bool
    book_items: list[dict[str, Any]]
    json_error_sample: str | None = None


def _is_book_item(item: Any) -> bool:
    if not isinstance(item, dict):
        return False
    return "bids" in item and "asks" in item


def decode_ws_raw(raw: Any) -> WsDecodeResult:
    """Decode WS payloads which are often list-of-events, plus PONG/[] keepalives."""
    if isinstance(raw, bytes):
        raw_text = raw.decode("utf-8", errors="ignore")
        raw_payload: str | bytes = raw
    else:
        raw_text = str(raw)
        raw_payload = raw_text
    text = raw_text.strip()
    if text.upper() in {"PONG", "PING"}:
        return WsDecodeResult(
            raw_text=raw_text,
            payload=None,
            is_pong=True,
            json_error=False,
            empty_array=False,
            book_items=[],
            json_error_sample=None,
        )
    try:
        payload = orjson.loads(raw_payload)
    except orjson.JSONDecodeError:
        return WsDecodeResult(
            raw_text=raw_text,
            payload=None,
            is_pong=False,
            json_error=True,
            empty_array=False,
            book_items=[],
            json_error_sample=raw_text[:50],
        )
    book_items: list[dict[str, Any]] = []
    empty_array = False
    if isinstance(payload, list):
        if not payload:
            empty_array = True
        else:
            for item in payload:
                if _is_book_item(item):
                    book_items.append(item)
    elif isinstance(payload, dict):
        msg_type = payload.get("type")
        if isinstance(msg_type, str) and msg_type.lower() in {"ping", "pong"}:
            return WsDecodeResult(
                raw_text=raw_text,
                payload=payload,
                is_pong=True,
                json_error=False,
                empty_array=False,
                book_items=[],
                json_error_sample=None,
            )
        if _is_book_item(payload):
            book_items.append(payload)
    return WsDecodeResult(
        raw_text=raw_text,
        payload=payload,
        is_pong=False,
        json_error=False,
        empty_array=empty_array,
        book_items=book_items,
        json_error_sample=None,
    )
