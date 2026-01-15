from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, Sequence

from .intents import Intent
from .state import GlobalState


@dataclass(slots=True)
class AllocatorConfig:
    max_intents_global: int | None = None
    max_intents_per_strategy: int | None = None


@dataclass(slots=True)
class Allocator:
    config: AllocatorConfig = field(default_factory=AllocatorConfig)

    def merge_intents(
        self,
        intents_by_strategy: Mapping[str, Sequence[Intent]],
        state: GlobalState,
    ) -> list[Intent]:
        del state
        merged: list[Intent] = []
        max_global = self.config.max_intents_global
        max_per = self.config.max_intents_per_strategy
        for strategy_id in sorted(intents_by_strategy):
            intents = intents_by_strategy[strategy_id]
            count_for_strategy = 0
            for intent in intents:
                if max_per is not None and count_for_strategy >= max_per:
                    break
                merged.append(intent)
                count_for_strategy += 1
                if max_global is not None and len(merged) >= max_global:
                    return merged
        return merged
