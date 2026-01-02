import pytest

from pm_arb.fixed import parse_price_to_micro, parse_size_to_units, parse_sizes_list


def test_parse_price_to_micro_ceiling():
    assert parse_price_to_micro("0.000001") == 1
    assert parse_price_to_micro("1.0000001") == 1_000_001
    assert parse_price_to_micro("1.0") == 1_000_000


def test_parse_size_to_units_decimals():
    assert parse_size_to_units("1.234") == 1_234_000
    with pytest.raises(ValueError):
        parse_size_to_units("0.0000001")
    with pytest.raises(ValueError):
        parse_size_to_units("-1")


def test_parse_sizes_list():
    assert parse_sizes_list("10,50,100") == [10, 50, 100]
    with pytest.raises(ValueError):
        parse_sizes_list("10,5.5")
