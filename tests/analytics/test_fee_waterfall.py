"""Deterministic tests for fixed fee waterfall identities."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from analytics.fee_engine import apply_fee_waterfall
from analytics.yield_engine import YieldDailyRow, build_rollup_rows


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


def test_rollup_rows_apply_fee_waterfall_to_aggregate_gross() -> None:
    business_date = date(2026, 3, 8)
    rollups = build_rollup_rows(
        business_date=business_date,
        method="apy_prorated_sod_eod",
        position_rows=[
            YieldDailyRow(
                business_date=business_date,
                wallet_id=1,
                product_id=1,
                protocol_id=1,
                market_id=1,
                row_key="position:gain",
                position_key="gain",
                gross_yield_usd=Decimal("100"),
                strategy_fee_usd=Decimal("15"),
                avant_gop_usd=Decimal("8.5"),
                net_yield_usd=Decimal("76.5"),
                avg_equity_usd=Decimal("1_000"),
                gross_roe=Decimal("0.1"),
                post_strategy_fee_roe=Decimal("0.085"),
                net_roe=Decimal("0.0765"),
                avant_gop_roe=Decimal("0.0085"),
                method="apy_prorated_sod_eod",
                confidence_score=Decimal("1"),
            ),
            YieldDailyRow(
                business_date=business_date,
                wallet_id=1,
                product_id=1,
                protocol_id=1,
                market_id=2,
                row_key="position:loss",
                position_key="loss",
                gross_yield_usd=Decimal("-90"),
                strategy_fee_usd=Decimal("0"),
                avant_gop_usd=Decimal("0"),
                net_yield_usd=Decimal("-90"),
                avg_equity_usd=Decimal("900"),
                gross_roe=Decimal("-0.1"),
                post_strategy_fee_roe=Decimal("-0.1"),
                net_roe=Decimal("-0.1"),
                avant_gop_roe=Decimal("0"),
                method="apy_prorated_sod_eod",
                confidence_score=Decimal("1"),
            ),
        ],
    )

    total = next(row for row in rollups if row.row_key == "total")

    assert total.gross_yield_usd == Decimal("10")
    assert total.strategy_fee_usd == Decimal("1.50")
    assert total.avant_gop_usd == Decimal("0.850")
    assert total.net_yield_usd == Decimal("7.650")
