from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PolicyRule:
    policy_id: str
    cadence_bucket: str | None = None
    is_crypto: bool | None = None
    fee_enabled: bool | None = None
    fee_rate_known: bool | None = None

    def matches(self, segment: Any) -> bool:
        if self.cadence_bucket is not None and getattr(segment, "cadence_bucket", None) != self.cadence_bucket:
            return False
        if self.is_crypto is not None and getattr(segment, "is_crypto", None) != self.is_crypto:
            return False
        if self.fee_enabled is not None and getattr(segment, "fee_enabled", None) != self.fee_enabled:
            return False
        if self.fee_rate_known is not None and bool(getattr(segment, "fee_rate_known", False)) != self.fee_rate_known:
            return False
        return True


@dataclass(frozen=True)
class PolicyMap:
    default_policy_id: str
    rules: list[PolicyRule]

    def to_dict(self) -> dict[str, Any]:
        return {
            "default_policy_id": self.default_policy_id,
            "rules": [
                {
                    "policy_id": rule.policy_id,
                    "cadence_bucket": rule.cadence_bucket,
                    "is_crypto": rule.is_crypto,
                    "fee_enabled": rule.fee_enabled,
                    "fee_rate_known": rule.fee_rate_known,
                }
                for rule in self.rules
            ],
        }


class PolicySelector:
    def __init__(self, policy_map: PolicyMap) -> None:
        self._policy_map = policy_map

    @property
    def policy_map(self) -> PolicyMap:
        return self._policy_map

    def select(self, segment: Any) -> str:
        for rule in self._policy_map.rules:
            if rule.matches(segment):
                return rule.policy_id
        return self._policy_map.default_policy_id


def _parse_tri_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        text = value.strip().lower()
        if text in {"unknown", "none", "null"}:
            return None
        if text in {"true", "1", "yes", "y", "on"}:
            return True
        if text in {"false", "0", "no", "n", "off"}:
            return False
    return None


def parse_policy_rules(raw: str, default_policy_id: str) -> PolicyMap:
    rules: list[PolicyRule] = []
    if raw:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = []
        if isinstance(payload, dict):
            if "default_policy_id" in payload and isinstance(payload["default_policy_id"], str):
                default_policy_id = payload["default_policy_id"]
            payload = payload.get("rules", [])
        if isinstance(payload, list):
            for entry in payload:
                if not isinstance(entry, dict):
                    continue
                policy_id = entry.get("policy_id")
                if not isinstance(policy_id, str) or not policy_id:
                    continue
                rules.append(
                    PolicyRule(
                        policy_id=policy_id,
                        cadence_bucket=entry.get("cadence_bucket"),
                        is_crypto=_parse_tri_bool(entry.get("is_crypto")),
                        fee_enabled=_parse_tri_bool(entry.get("fee_enabled")),
                        fee_rate_known=_parse_tri_bool(entry.get("fee_rate_known")),
                    )
                )
    return PolicyMap(default_policy_id=default_policy_id, rules=rules)


def policy_counts(selector: PolicySelector, segments_by_token_id: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for segment in segments_by_token_id.values():
        policy_id = selector.select(segment)
        counts[policy_id] = counts.get(policy_id, 0) + 1
    return counts

