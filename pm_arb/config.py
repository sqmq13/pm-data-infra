from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

ENV_PREFIX = "PM_ARB_"


def _parse_bool(value: str) -> bool:
    val = str(value).strip().lower()
    if val in {"1", "true", "yes", "y", "on"}:
        return True
    if val in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid bool: {value}")


def _parse_number(value: str, target_type: type) -> Any:
    if target_type is int:
        return int(value)
    if target_type is float:
        return float(value)
    return value


def _is_field_type(field_type, expected: type, expected_name: str) -> bool:
    if field_type is expected:
        return True
    if isinstance(field_type, str) and field_type == expected_name:
        return True
    return False


@dataclass
class Config:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    gamma_limit: int = 100
    rest_timeout: float = 10.0
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    ws_shards: int = 8
    ws_subscribe_max_tokens: int = 500
    ws_subscribe_max_bytes: int = 32768
    ws_reconnect_max: int = 10
    ws_reconnect_backoff_seconds: float = 1.0
    capture_frames_schema_version: int = 2
    capture_max_markets: int = 5000
    capture_confirm_tokens_per_shard: int = 25
    capture_confirm_timeout_seconds: float = 45.0
    capture_confirm_min_events: int = 1
    capture_confirm_max_failures: int = 3
    capture_backpressure_fatal_ms: float = 250.0
    capture_ring_buffer_frames: int = 4096
    capture_metrics_max_samples: int = 5000
    capture_heartbeat_interval_seconds: float = 1.0
    capture_universe_refresh_enable: bool = False
    capture_universe_refresh_interval_seconds: float = 60.0
    capture_universe_refresh_stagger_seconds: float = 0.25
    capture_universe_refresh_grace_seconds: float = 30.0
    capture_universe_refresh_min_delta_tokens: int = 2
    capture_universe_refresh_max_churn_pct: float = 10.0
    capture_universe_refresh_max_interval_seconds: float = 600.0
    capture_universe_refresh_churn_guard_consecutive_fatal: int = 5
    data_dir: str = "./data"
    min_free_disk_gb: int = 5
    offline: bool = False

    def apply_overrides(self, overrides: dict[str, Any]) -> "Config":
        for field in fields(self):
            name = field.name
            if name in overrides and overrides[name] is not None:
                setattr(self, name, overrides[name])
        return self

    @classmethod
    def from_env_and_cli(cls, cli_overrides: dict[str, Any], env: dict[str, str]) -> "Config":
        cfg = cls().apply_overrides(cli_overrides)
        for field in fields(cfg):
            env_key = ENV_PREFIX + field.name.upper()
            if env_key not in env:
                continue
            raw = env[env_key]
            if _is_field_type(field.type, bool, "bool"):
                value = _parse_bool(raw)
            elif _is_field_type(field.type, int, "int"):
                value = _parse_number(raw, int)
            elif _is_field_type(field.type, float, "float"):
                value = _parse_number(raw, float)
            else:
                value = raw
            setattr(cfg, field.name, value)
        return cfg
