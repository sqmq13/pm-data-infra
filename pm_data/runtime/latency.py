from __future__ import annotations

from dataclasses import dataclass, field
import math
import os
import time
from typing import Any


@dataclass(slots=True)
class LatencySummary:
    count: int
    p50_us: int
    p90_us: int
    p99_us: int
    max_us: int

    def to_dict(self) -> dict[str, int]:
        return {
            "count": int(self.count),
            "p50_us": int(self.p50_us),
            "p90_us": int(self.p90_us),
            "p99_us": int(self.p99_us),
            "max_us": int(self.max_us),
        }


@dataclass(slots=True)
class LatencyHistogram:
    _counts: list[int] = field(default_factory=lambda: [0] * 561)
    _total: int = 0
    _max_us: int = 0

    def observe_us(self, value_us: int) -> None:
        if value_us <= 0:
            idx = 0
            upper_us = 50
        elif value_us < 20_000:
            idx = value_us // 50
            upper_us = (idx + 1) * 50
        elif value_us < 100_000:
            idx = 400 + (value_us - 20_000) // 1_000
            upper_us = 20_000 + (idx - 400 + 1) * 1_000
        elif value_us < 500_000:
            idx = 480 + (value_us - 100_000) // 5_000
            upper_us = 100_000 + (idx - 480 + 1) * 5_000
        else:
            idx = 560
            upper_us = value_us

        self._counts[int(idx)] += 1
        self._total += 1
        if upper_us > self._max_us:
            self._max_us = upper_us

    @property
    def total(self) -> int:
        return self._total

    @property
    def max_us(self) -> int:
        return self._max_us

    def _quantile_upper_us(self, q: float) -> int:
        if self._total <= 0:
            return 0
        q = min(1.0, max(0.0, float(q)))
        rank = max(0, int(math.ceil(q * self._total) - 1))
        cumulative = 0
        for idx, count in enumerate(self._counts):
            if count <= 0:
                continue
            cumulative += count
            if cumulative > rank:
                if idx == 560:
                    return self._max_us
                if idx < 400:
                    return (idx + 1) * 50
                if idx < 480:
                    return 20_000 + (idx - 400 + 1) * 1_000
                return 100_000 + (idx - 480 + 1) * 5_000
        return self._max_us

    def summary(self) -> LatencySummary:
        return LatencySummary(
            count=self._total,
            p50_us=self._quantile_upper_us(0.50),
            p90_us=self._quantile_upper_us(0.90),
            p99_us=self._quantile_upper_us(0.99),
            max_us=self._max_us,
        )


@dataclass(slots=True)
class LiveLatencyCollector:
    start_mono_ns: int = 0
    end_mono_ns: int = 0
    start_cpu_ns: int = 0
    end_cpu_ns: int = 0
    frames: int = 0
    canonical_events: int = 0
    intents: int = 0
    stages: dict[str, LatencyHistogram] = field(
        default_factory=lambda: {
            "recv_to_decode": LatencyHistogram(),
            "decode_to_normalize": LatencyHistogram(),
            "normalize_to_state": LatencyHistogram(),
            "state_to_strategy": LatencyHistogram(),
            "strategy_to_allocator": LatencyHistogram(),
            "allocator_to_execution": LatencyHistogram(),
            "execution_to_emit": LatencyHistogram(),
            "e2e_total": LatencyHistogram(),
            "e2e_intent_ready": LatencyHistogram(),
        }
    )

    def start(self, mono_ns: int) -> None:
        self.start_mono_ns = int(mono_ns)
        self.start_cpu_ns = int(time.process_time_ns())

    def finish(self, mono_ns: int) -> None:
        self.end_mono_ns = int(mono_ns)
        self.end_cpu_ns = int(time.process_time_ns())

    def observe_event(
        self,
        *,
        recv_ns: int,
        decode_end_ns: int,
        normalize_end_ns: int,
        state_end_ns: int,
        strategy_end_ns: int,
        allocator_end_ns: int,
        execution_end_ns: int,
        emit_end_ns: int,
    ) -> None:
        def _us(delta_ns: int) -> int:
            if delta_ns <= 0:
                return 0
            return int(delta_ns // 1_000)

        self.canonical_events += 1
        self.stages["recv_to_decode"].observe_us(_us(decode_end_ns - recv_ns))
        self.stages["decode_to_normalize"].observe_us(
            _us(normalize_end_ns - decode_end_ns)
        )
        self.stages["normalize_to_state"].observe_us(_us(state_end_ns - normalize_end_ns))
        self.stages["state_to_strategy"].observe_us(_us(strategy_end_ns - state_end_ns))
        self.stages["strategy_to_allocator"].observe_us(
            _us(allocator_end_ns - strategy_end_ns)
        )
        self.stages["allocator_to_execution"].observe_us(
            _us(execution_end_ns - allocator_end_ns)
        )
        self.stages["execution_to_emit"].observe_us(_us(emit_end_ns - execution_end_ns))
        self.stages["e2e_total"].observe_us(_us(emit_end_ns - recv_ns))
        self.stages["e2e_intent_ready"].observe_us(_us(allocator_end_ns - recv_ns))

    def report(
        self,
        *,
        reconnects: int,
        dropped: int,
        decode_errors: int,
        schema_errors: int,
        bid_scan_count: int,
        bid_scan_levels_total: int,
        bid_scan_levels_max: int,
        ask_scan_count: int,
        ask_scan_levels_total: int,
        ask_scan_levels_max: int,
    ) -> dict[str, Any]:
        elapsed_ns = max(0, self.end_mono_ns - self.start_mono_ns)
        cpu_ns = max(0, self.end_cpu_ns - self.start_cpu_ns)
        cpu_count = os.cpu_count() or 1
        cpu_util_bp = 0
        events_per_sec_e3 = 0
        frames_per_sec_e3 = 0
        if elapsed_ns > 0:
            cpu_util_bp = int((cpu_ns * 10_000) // (elapsed_ns * cpu_count))
            events_per_sec_e3 = int((self.canonical_events * 1_000_000_000_000) // elapsed_ns)
            frames_per_sec_e3 = int((self.frames * 1_000_000_000_000) // elapsed_ns)

        stages_payload: dict[str, dict[str, int]] = {}
        for name, hist in self.stages.items():
            stages_payload[name] = hist.summary().to_dict()

        return {
            "record_type": "live_latency_report",
            "mode": "live",
            "execution": "sim",
            "t0_mono_ns": int(self.start_mono_ns),
            "t1_mono_ns": int(self.end_mono_ns),
            "elapsed_ns": int(elapsed_ns),
            "frames": int(self.frames),
            "canonical_events": int(self.canonical_events),
            "intents": int(self.intents),
            "reconnects": int(reconnects),
            "dropped": int(dropped),
            "decode_errors": int(decode_errors),
            "schema_errors": int(schema_errors),
            "best_bid_scans": int(bid_scan_count),
            "best_bid_scan_levels_total": int(bid_scan_levels_total),
            "best_bid_scan_levels_max": int(bid_scan_levels_max),
            "best_ask_scans": int(ask_scan_count),
            "best_ask_scan_levels_total": int(ask_scan_levels_total),
            "best_ask_scan_levels_max": int(ask_scan_levels_max),
            "events_per_sec_e3": int(events_per_sec_e3),
            "frames_per_sec_e3": int(frames_per_sec_e3),
            "cpu_time_ns": int(cpu_ns),
            "cpu_util_bp": int(cpu_util_bp),
            "stages": stages_payload,
        }
