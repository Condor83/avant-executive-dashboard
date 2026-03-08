"""Deterministic tests for fixed fee waterfall identities."""

from __future__ import annotations

from decimal import Decimal

from analytics.fee_engine import apply_fee_waterfall


def test_fee_waterfall_positive_gross_yield() -> None:
    gross = Decimal("100")
    fees = apply_fee_waterfall(gross)

    assert fees.strategy_fee_usd == Decimal("15.00")
    assert fees.avant_gop_usd == Decimal("8.5000")
    assert fees.net_yield_usd == Decimal("76.5000")


def test_fee_waterfall_zero_gross_yield() -> None:
    gross = Decimal("0")
    fees = apply_fee_waterfall(gross)

    assert fees.strategy_fee_usd == Decimal("0")
    assert fees.avant_gop_usd == Decimal("0")
    assert fees.net_yield_usd == Decimal("0")


def test_fee_waterfall_negative_gross_yield() -> None:
    gross = Decimal("-20")
    fees = apply_fee_waterfall(gross)

    assert fees.strategy_fee_usd == Decimal("0")
    assert fees.avant_gop_usd == Decimal("0")
    assert fees.net_yield_usd == Decimal("-20")
