"""Deterministic kink-proximity risk scoring tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from analytics.risk_engine import (
    MarketRiskRow,
    RiskComputationResult,
    compute_kink_risk_score,
    extract_kink_target_from_irm,
    top_markets_by_kink_risk,
)
from core.config import load_risk_thresholds_config


def test_kink_score_normalizes_utilization_by_target() -> None:
    score = compute_kink_risk_score(
        utilization=Decimal("0.90"),
        kink_target_utilization=Decimal("0.80"),
    )

    assert score == Decimal("1.125")


def test_kink_target_extraction_accepts_decimal_or_percent_values() -> None:
    assert extract_kink_target_from_irm({"optimal_usage_ratio": "0.85"}) == Decimal("0.85")
    assert extract_kink_target_from_irm({"kink": "90"}) == Decimal("0.9")
    assert extract_kink_target_from_irm({"kink": "not-a-number"}) is None


def test_markets_rank_by_highest_kink_risk_score() -> None:
    as_of = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    result = RiskComputationResult(
        as_of_ts_utc=as_of,
        position_rows=[],
        market_rows=[
            MarketRiskRow(
                as_of_ts_utc=as_of,
                market_id=1,
                protocol_code="aave_v3",
                chain_code="ethereum",
                market_address="0xmarket1",
                utilization=Decimal("0.82"),
                kink_target_utilization=Decimal("0.80"),
                kink_score=Decimal("1.025"),
                borrow_apy=Decimal("0.05"),
                borrow_apy_delta=Decimal("0.01"),
                available_liquidity_usd=Decimal("18"),
                total_supply_usd=Decimal("100"),
                available_liquidity_ratio=Decimal("0.18"),
            ),
            MarketRiskRow(
                as_of_ts_utc=as_of,
                market_id=2,
                protocol_code="morpho",
                chain_code="ethereum",
                market_address="0xmarket2",
                utilization=Decimal("0.91"),
                kink_target_utilization=Decimal("0.85"),
                kink_score=Decimal("1.070588235294117647058823529"),
                borrow_apy=Decimal("0.07"),
                borrow_apy_delta=Decimal("0.02"),
                available_liquidity_usd=Decimal("9"),
                total_supply_usd=Decimal("100"),
                available_liquidity_ratio=Decimal("0.09"),
            ),
            MarketRiskRow(
                as_of_ts_utc=as_of,
                market_id=3,
                protocol_code="spark",
                chain_code="ethereum",
                market_address="0xmarket3",
                utilization=Decimal("0.88"),
                kink_target_utilization=Decimal("0.80"),
                kink_score=Decimal("1.1"),
                borrow_apy=Decimal("0.08"),
                borrow_apy_delta=Decimal("0.03"),
                available_liquidity_usd=Decimal("12"),
                total_supply_usd=Decimal("100"),
                available_liquidity_ratio=Decimal("0.12"),
            ),
        ],
    )

    ranked = top_markets_by_kink_risk(result, limit=3)

    assert [row.market_id for row in ranked] == [3, 2, 1]


def test_kamino_uses_explicit_kink_target_override() -> None:
    thresholds = load_risk_thresholds_config(Path("config/risk_thresholds.yaml"))

    assert thresholds.kink.protocol_target_overrides["kamino"] == Decimal("0.90")
