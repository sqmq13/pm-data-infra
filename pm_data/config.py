from __future__ import annotations

from dataclasses import dataclass, fields
import types
import typing
from typing import Any, get_args, get_origin

ENV_PREFIX = "PM_DATA_"


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


def _unwrap_optional(field_type: Any) -> tuple[Any, bool]:
    origin = get_origin(field_type)
    union_type = getattr(types, "UnionType", None)
    if origin not in (typing.Union, union_type):
        return field_type, False
    args = get_args(field_type)
    if args and type(None) in args and len(args) == 2:
        base = args[0] if args[1] is type(None) else args[1]
        return base, True
    return field_type, False


def _is_field_type(field_type: Any, expected: type, expected_name: str) -> bool:
    base_type, _is_optional = _unwrap_optional(field_type)
    if base_type is expected:
        return True
    if isinstance(base_type, str) and base_type == expected_name:
        return True
    return False


def _parse_optional(raw: str, target_type: type) -> Any:
    text = str(raw).strip()
    if text == "":
        return None
    lower = text.lower()
    if lower in {"none", "null"}:
        return None
    if target_type is bool:
        return _parse_bool(text)
    return _parse_number(text, target_type)


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
    ws_ping_interval_seconds: float = 10.0
    ws_ping_timeout_seconds: float = 10.0
    ws_data_idle_reconnect_seconds: float = 120.0
    ws_user_agent: str = "pm_data"
    capture_frames_schema_version: int = 2
    capture_max_markets: int | None = 2000
    capture_confirm_tokens_per_shard: int = 25
    capture_confirm_timeout_seconds: float = 45.0
    capture_confirm_min_events: int = 1
    capture_confirm_max_failures: int = 3
    capture_backpressure_fatal_ms: float = 250.0
    capture_ring_buffer_frames: int = 4096
    capture_metrics_max_samples: int = 5000
    capture_runlog_enqueue_timeout_seconds: float = 1.0
    capture_ndjson_fsync_on_close: bool = False
    capture_ndjson_fsync_interval_seconds: float | None = None
    capture_heartbeat_interval_seconds: float = 1.0
    capture_gc_disable: bool = True
    capture_universe_refresh_enable: bool = True
    capture_universe_refresh_interval_seconds: float = 60.0
    capture_universe_refresh_timeout_seconds: float = 30.0
    capture_universe_refresh_stagger_seconds: float = 0.25
    capture_universe_refresh_grace_seconds: float = 30.0
    capture_universe_refresh_min_delta_tokens: int = 2
    capture_universe_refresh_max_churn_pct: float = 10.0
    capture_universe_refresh_max_interval_seconds: float = 600.0
    capture_universe_refresh_churn_guard_consecutive_fatal: int = 5
    capture_expected_churn_enable: bool = True
    capture_expected_churn_window_seconds_5m: float = 30.0
    capture_expected_churn_window_seconds_15m: float = 60.0
    capture_expected_churn_window_seconds_30m: float = 90.0
    capture_expected_churn_window_seconds_60m: float = 120.0
    capture_expected_churn_expiry_window_seconds: float = 300.0
    capture_expected_churn_min_ratio: float = 0.5
    fee_rate_enable: bool = True
    fee_rate_base_url: str = "https://clob.polymarket.com"
    fee_rate_timeout_seconds: float = 5.0
    fee_rate_cache_ttl_seconds: float = 300.0
    fee_rate_max_in_flight: int = 25
    fee_rate_prefetch_max_tokens: int = 200
    fee_rate_refresh_timeout_seconds: float = 2.0
    fee_regime_sample_size_per_cycle: int = 50
    fee_regime_canary_token_ids: str = ""
    fee_regime_expect_15m_crypto_fee_enabled: bool = True
    fee_regime_expect_other_fee_free: bool = True
    fee_regime_expect_unknown_fee_free: bool = True
    fee_regime_expected_fee_rate_bps_values: str = ""
    policy_default_id: str = "default"
    policy_rules_json: str = "[]"
    data_dir: str = "./data"
    min_free_disk_gb: int | None = 5
    offline: bool = False

    def apply_overrides(self, overrides: dict[str, Any]) -> "Config":
        for field in fields(self):
            name = field.name
            if name in overrides:
                value = overrides[name]
                if value is None:
                    _base_type, is_optional = _unwrap_optional(field.type)
                    if is_optional:
                        setattr(self, name, None)
                    continue
                setattr(self, name, value)
        return self

    @classmethod
    def from_env_and_cli(cls, cli_overrides: dict[str, Any], env: dict[str, str]) -> "Config":
        cfg = cls().apply_overrides(cli_overrides)
        for field in fields(cfg):
            env_key = ENV_PREFIX + field.name.upper()
            if env_key not in env:
                continue
            raw = env[env_key]
            base_type, is_optional = _unwrap_optional(field.type)
            if is_optional:
                value = _parse_optional(raw, base_type)
            elif _is_field_type(field.type, bool, "bool"):
                value = _parse_bool(raw)
            elif _is_field_type(field.type, int, "int"):
                value = _parse_number(raw, int)
            elif _is_field_type(field.type, float, "float"):
                value = _parse_number(raw, float)
            else:
                value = raw
            setattr(cfg, field.name, value)
        return cfg
