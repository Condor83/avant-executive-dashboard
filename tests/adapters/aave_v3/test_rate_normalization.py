"""Rate normalization tests for Aave v3 fixed-point units."""

from __future__ import annotations

from decimal import Decimal

from adapters.aave_v3.adapter import apr_to_apy, normalize_aave_ray_rate


def test_normalize_aave_ray_rate_to_decimal_units() -> None:
    five_percent_ray = int(Decimal("0.05") * Decimal("1e27"))
    assert normalize_aave_ray_rate(five_percent_ray) == Decimal("0.05")


def test_apr_to_apy_is_in_0_to_1_units() -> None:
    apr = Decimal("0.10")
    apy = apr_to_apy(apr)
    assert apy > apr
    assert apy < Decimal("0.11")


def test_apr_to_apy_handles_zero() -> None:
    assert apr_to_apy(Decimal("0")) == Decimal("0")
