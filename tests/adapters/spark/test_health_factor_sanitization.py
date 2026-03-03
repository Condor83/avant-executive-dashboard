"""Health-factor sanitization tests for Spark snapshots."""

from __future__ import annotations

from decimal import Decimal

from adapters.spark.adapter import SparkAdapter


def test_health_factor_is_none_without_borrow() -> None:
    assert SparkAdapter._sanitize_health_factor(Decimal("9999999999"), has_borrow=False) is None


def test_health_factor_is_none_when_over_numeric_limit() -> None:
    assert SparkAdapter._sanitize_health_factor(Decimal("10000000000"), has_borrow=True) is None


def test_health_factor_is_retained_when_valid() -> None:
    value = Decimal("1.2345")
    assert SparkAdapter._sanitize_health_factor(value, has_borrow=True) == value
