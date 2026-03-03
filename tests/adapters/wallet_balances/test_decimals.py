"""Wallet balances decimals scaling tests."""

from decimal import Decimal

from adapters.wallet_balances.adapter import normalize_raw_amount


def test_normalize_raw_amount_scales_by_decimals() -> None:
    assert normalize_raw_amount(123456789, 6) == Decimal("123.456789")
    assert normalize_raw_amount(10**18, 18) == Decimal("1")
