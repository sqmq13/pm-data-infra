from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class TopOfBookUpdate:
    market_id: str
    bid_px_e6: int | None
    bid_sz_e6: int | None
    ask_px_e6: int | None
    ask_sz_e6: int | None
    ts_event: int | None
    ts_recv: int
    seq: int


CanonicalEvent = TopOfBookUpdate
