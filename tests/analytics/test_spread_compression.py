"""Deterministic spread-compression scoring tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from analytics.alerts import classify_decreasing_risk
from analytics.risk_engine import (
    PositionRiskRow,
    RiskComputationResult,
    compute_net_spread_apy,
    top_positions_by_worst_net_spread,
)
from core.config import DecreasingRiskThresholds


def test_net_spread_formula_matches_supply_plus_rewards_minus_borrow() -> None:
    net_spread = compute_net_spread_apy(
        supply_apy=Decimal("0.065"),
        reward_apy=Decimal("0.010"),
        borrow_apy=Decimal("0.055"),
    )

    assert net_spread == Decimal("0.020")


def test_spread_threshold_classification_is_deterministic() -> None:
    thresholds = DecreasingRiskThresholds(
        low=Decimal("0.015"),
        med=Decimal("0.010"),
        high=Decimal("0.005"),
    )

    assert classify_decreasing_risk(Decimal("0.020"), thresholds=thresholds) is None
    assert classify_decreasing_risk(Decimal("0.012"), thresholds=thresholds) == "low"
    assert classify_decreasing_risk(Decimal("0.009"), thresholds=thresholds) == "med"
    assert classify_decreasing_risk(Decimal("0.004"), thresholds=thresholds) == "high"


def test_positions_rank_by_worst_net_spread() -> None:
    as_of = datetime(2026, 3, 4, 12, 0, tzinfo=UTC)
    result = RiskComputationResult(
        as_of_ts_utc=as_of,
        market_rows=[],
        position_rows=[
            PositionRiskRow(
                as_of_ts_utc=as_of,
                position_key="pos-a",
                wallet_id=1,
                market_id=101,
                supply_apy=Decimal("0.05"),
                reward_apy=Decimal("0.00"),
                borrow_apy=Decimal("0.04"),
                net_spread_apy=Decimal("0.01"),
            ),
            PositionRiskRow(
                as_of_ts_utc=as_of,
                position_key="pos-b",
                wallet_id=2,
                market_id=102,
                supply_apy=Decimal("0.03"),
                reward_apy=Decimal("0.00"),
                borrow_apy=Decimal("0.05"),
                net_spread_apy=Decimal("-0.02"),
            ),
            PositionRiskRow(
                as_of_ts_utc=as_of,
                position_key="pos-c",
                wallet_id=3,
                market_id=103,
                supply_apy=Decimal("0.06"),
                reward_apy=Decimal("0.00"),
                borrow_apy=Decimal("0.06"),
                net_spread_apy=Decimal("0"),
            ),
        ],
    )

    ranked = top_positions_by_worst_net_spread(result, limit=3)

    assert [row.position_key for row in ranked] == ["pos-b", "pos-c", "pos-a"]
