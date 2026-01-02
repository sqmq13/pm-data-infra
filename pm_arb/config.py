from __future__ import annotations

from dataclasses import dataclass, fields
from typing import Any

from .fixed import PRICE_SCALE, parse_sizes_list

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


@dataclass
class Config:
    gamma_base_url: str = "https://gamma-api.polymarket.com"
    gamma_limit: int = 100
    clob_rest_base_url: str = "https://clob.polymarket.com"
    clob_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/"
    geoblock_url: str = "https://polymarket.com/api/geoblock"
    markets_poll_interval: float = 20.0
    market_regex: str = (
        r"(?i)\b15\b.*(min|minute).*(BTC|ETH|SOL)|\b(BTC|ETH|SOL)\b.*\b15\b.*(min|minute)"
    )
    secondary_filter_enable: bool = True
    max_markets: int = 5000
    top_k: int = 50
    sizes: str = "10,50,100,250,500"
    sizes_auto_warmup_minutes: int = 20
    buffer_per_share: float = 0.005
    price_scale: int = PRICE_SCALE
    stale_seconds: float = 2.0
    ws_disconnect_alarm_seconds: float = 5.0
    resync_interval: float = 60.0
    rest_timeout: float = 10.0
    rest_snapshot_sample_min: int = 25
    rest_snapshot_sample_pct: float = 0.10
    rest_rate_per_sec: int = 10
    rest_burst: int = 20
    rest_retry_max: int = 6
    contract_timeout: float = 10.0
    ws_sample_capture_n: int = 20
    ws_initial_parse_fail_threshold_pct: float = 0.10
    reconcile_mismatch_persist_n: int = 3
    reconcile_tick_tolerance: int = 2
    reconcile_rel_tol: float = 0.20
    data_dir: str = "./data"
    heartbeat_interval: float = 5.0
    log_rotate_mb: int = 256
    log_rotate_keep: int = 20
    min_free_disk_gb: int = 5
    offline: bool = False

    def sizes_list(self) -> list[int] | None:
        if str(self.sizes).strip().lower() == "auto":
            return None
        return parse_sizes_list(self.sizes)

    def buffer_per_share_micro(self) -> int:
        from decimal import Decimal, ROUND_CEILING

        dec = Decimal(str(self.buffer_per_share))
        micro = (dec * Decimal(self.price_scale)).to_integral_value(rounding=ROUND_CEILING)
        return int(micro)

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
            if field.type is bool:
                value = _parse_bool(raw)
            elif field.type in {int, float}:
                value = _parse_number(raw, field.type)
            else:
                value = raw
            setattr(cfg, field.name, value)
        return cfg
