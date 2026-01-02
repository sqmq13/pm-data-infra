from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .book import OrderBook
from .fixed import SIZE_SCALE
from .sweep import sweep_cost

ABS_TOL_MICRO_PER_SHARE = 2_000  # 0.002 dollars


def infer_tick_micro(asks: list[tuple[int, int]]) -> int | None:
    prices = sorted({price for price, _ in asks})
    if len(prices) < 2:
        return None
    diffs = [b - a for a, b in zip(prices, prices[1:]) if b - a > 0]
    if not diffs:
        return None
    return min(diffs)


@dataclass
class ReconState:
    mismatch_count: int = 0
    desynced: bool = False
    rest_unavailable: bool = False


@dataclass
class ReconResult:
    token_id: str
    ok: bool
    mismatch: bool
    mismatch_count: int
    desynced: bool
    immediate: bool
    details: list[dict[str, Any]] = field(default_factory=list)


class Reconciler:
    def __init__(self, persist_n: int, tick_tolerance: int, rel_tol: float):
        self.persist_n = persist_n
        self.tick_tolerance = tick_tolerance
        self.rel_tol = rel_tol
        self.state: dict[str, ReconState] = {}

    def _get_state(self, token_id: str) -> ReconState:
        if token_id not in self.state:
            self.state[token_id] = ReconState()
        return self.state[token_id]

    def compare(
        self,
        token_id: str,
        ws_book: OrderBook,
        rest_book: OrderBook,
        sizes_shares: list[int],
    ) -> ReconResult:
        state = self._get_state(token_id)
        tick = infer_tick_micro(ws_book.asks)
        mismatch = False
        immediate = False
        details: list[dict[str, Any]] = []
        ws_best = ws_book.best_ask_micro() or 0
        rest_best = rest_book.best_ask_micro() or 0
        if tick is None:
            best_abs_tol = ABS_TOL_MICRO_PER_SHARE
        else:
            best_abs_tol = tick * self.tick_tolerance
        best_diff = abs(ws_best - rest_best)
        if ws_best > 0 and rest_best > 0 and best_diff > best_abs_tol:
            mismatch = True
        details.append(
            {
                "metric": "best_ask",
                "ws_best": ws_best,
                "rest_best": rest_best,
                "diff": best_diff,
                "allowed": best_abs_tol,
            }
        )
        for size_shares in sizes_shares:
            size_units = size_shares * SIZE_SCALE
            ws_cost, _, ws_ok = sweep_cost(ws_book.asks, size_units)
            rest_cost, _, rest_ok = sweep_cost(rest_book.asks, size_units)
            if not ws_ok or not rest_ok:
                continue
            diff = abs(ws_cost - rest_cost)
            if ws_cost > 0 and (rest_cost >= ws_cost * 2 or rest_cost <= ws_cost // 2):
                immediate = True
            if tick is None:
                abs_tol = ABS_TOL_MICRO_PER_SHARE * size_shares
            else:
                abs_tol = tick * self.tick_tolerance * size_shares
            rel_tol = int(ws_cost * self.rel_tol)
            allowed = max(abs_tol, rel_tol)
            if diff > allowed:
                mismatch = True
            details.append(
                {
                    "size": size_shares,
                    "ws_cost": ws_cost,
                    "rest_cost": rest_cost,
                    "diff": diff,
                    "allowed": allowed,
                }
            )
        if mismatch:
            state.mismatch_count += 1
        else:
            state.mismatch_count = 0
        if immediate or state.mismatch_count >= self.persist_n:
            state.desynced = True
        return ReconResult(
            token_id=token_id,
            ok=not mismatch,
            mismatch=mismatch,
            mismatch_count=state.mismatch_count,
            desynced=state.desynced,
            immediate=immediate,
            details=details,
        )
