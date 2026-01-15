from decimal import Decimal

import pytest

from pm_data.fees import (
    UnknownFeeRate,
    estimate_taker_fee,
    fee_denomination_for_side,
    required_fee_rate_bps,
)


def _meta(bps: int | None, known: bool = True) -> dict[str, object]:
    return {
        "fee_rate_bps": bps,
        "fee_rate_known": known,
        "fee_enabled": None if bps is None else bps > 0,
    }


def test_required_fee_rate_bps_known():
    assert required_fee_rate_bps("t1", _meta(25)) == 25


def test_required_fee_rate_bps_unknown():
    with pytest.raises(UnknownFeeRate):
        required_fee_rate_bps("t1", _meta(None, known=False))


def test_fee_estimator_table_points():
    fee = estimate_taker_fee(1, Decimal("0.05"), "buy", "t1", _meta(10))
    assert fee == Decimal("0.0006")
    fee = estimate_taker_fee(100, Decimal("0.70"), "sell", "t1", _meta(10))
    assert fee == Decimal("1.1025")


def test_fee_estimator_zero_rate_returns_zero():
    fee = estimate_taker_fee(1, Decimal("0.42"), "buy", "t1", _meta(0))
    assert fee == Decimal("0")


def test_fee_estimator_unknown_returns_none():
    fee = estimate_taker_fee(1, Decimal("0.05"), "buy", "t1", _meta(None, known=False))
    assert fee is None


def test_fee_estimator_unsupported_shares_returns_none():
    fee = estimate_taker_fee(2, Decimal("0.05"), "buy", "t1", _meta(10))
    assert fee is None


def test_fee_denomination_for_side():
    assert fee_denomination_for_side("buy") == "token"
    assert fee_denomination_for_side("sell") == "usdc"
