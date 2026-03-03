"""Health-factor sanitization tests for Aave v3 snapshots."""

from __future__ import annotations

from decimal import Decimal

from adapters.aave_v3.adapter import AaveV3Adapter


def test_health_factor_is_none_without_borrow() -> None:
    assert AaveV3Adapter._sanitize_health_factor(Decimal("9999999999"), has_borrow=False) is None


def test_health_factor_is_none_when_over_numeric_limit() -> None:
    assert AaveV3Adapter._sanitize_health_factor(Decimal("10000000000"), has_borrow=True) is None


def test_health_factor_is_retained_when_valid() -> None:
    value = Decimal("1.2345")
    assert AaveV3Adapter._sanitize_health_factor(value, has_borrow=True) == value
