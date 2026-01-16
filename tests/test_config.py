from pm_data.config import Config


def test_default_ws_url():
    assert Config().clob_ws_url == "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def test_env_optional_int_parses() -> None:
    cfg = Config.from_env_and_cli({}, {"PM_DATA_CAPTURE_MAX_MARKETS": "4000"})
    assert cfg.capture_max_markets == 4000


def test_env_optional_float_parses() -> None:
    cfg = Config.from_env_and_cli({}, {"PM_DATA_CAPTURE_NDJSON_FSYNC_INTERVAL_SECONDS": "1.5"})
    assert cfg.capture_ndjson_fsync_interval_seconds == 1.5
