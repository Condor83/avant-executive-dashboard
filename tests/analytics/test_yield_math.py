"""Deterministic unit tests for daily APY-pro-rated yield math."""

from __future__ import annotations

from decimal import Decimal

from analytics.yield_engine import compute_daily_gross_yield


def test_daily_gross_yield_formula_matches_expected() -> None:
    gross = compute_daily_gross_yield(
        supply_usd_sod=Decimal("100"),
        supply_usd_eod=Decimal("120"),
        borrow_usd_sod=Decimal("40"),
        borrow_usd_eod=Decimal("20"),
        supply_apy_sod=Decimal("0.10"),
        supply_apy_eod=Decimal("0.12"),
        reward_apy_sod=Decimal("0.02"),
        reward_apy_eod=Decimal("0.01"),
        borrow_apy_sod=Decimal("0.05"),
        borrow_apy_eod=Decimal("0.07"),
    )

    expected = ((Decimal("110") * Decimal("0.125")) / Decimal("365")) - (
        (Decimal("30") * Decimal("0.06")) / Decimal("365")
    )
    assert gross == expected


def test_daily_gross_yield_uses_zero_when_missing_boundary_values() -> None:
    gross = compute_daily_gross_yield(
        supply_usd_sod=Decimal("100"),
        supply_usd_eod=Decimal("0"),
        borrow_usd_sod=Decimal("30"),
        borrow_usd_eod=Decimal("0"),
        supply_apy_sod=Decimal("0.10"),
        supply_apy_eod=Decimal("0"),
        reward_apy_sod=Decimal("0.02"),
        reward_apy_eod=Decimal("0"),
        borrow_apy_sod=Decimal("0.06"),
        borrow_apy_eod=Decimal("0"),
    )

    expected = (
        ((Decimal("50") * Decimal("0.05")) / Decimal("365"))
        + ((Decimal("50") * Decimal("0.01")) / Decimal("365"))
        - ((Decimal("15") * Decimal("0.03")) / Decimal("365"))
    )
    assert gross == expected
