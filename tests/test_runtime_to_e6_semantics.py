import pytest

from pm_data.runtime.normalize import to_e6


def test_to_e6_round_half_up_string() -> None:
    assert to_e6("0.0000004") == 0
    assert to_e6("0.0000005") == 1
    assert to_e6("1.2345674") == 1_234_567
    assert to_e6("1.2345675") == 1_234_568


def test_to_e6_accepts_scientific_notation_string() -> None:
    assert to_e6("1e-6") == 1
    assert to_e6("2E-6") == 2
    assert to_e6("1e0") == 1_000_000


def test_to_e6_int_and_float_inputs() -> None:
    assert to_e6(1) == 1_000_000
    assert to_e6(0.1) == 100_000


def test_to_e6_rejects_bool() -> None:
    with pytest.raises(ValueError, match="invalid numeric type"):
        to_e6(True)

