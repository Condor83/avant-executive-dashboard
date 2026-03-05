"""Spark market risk-parameter extraction tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.spark.adapter import SparkAdapter
from core.config import MarketsConfig
from tests.adapters.spark.conftest import FixtureSparkRpcClient, build_full_price_map


def test_market_risk_params_are_normalized(
    markets_config: MarketsConfig,
    fixture_rpc_client: FixtureSparkRpcClient,
) -> None:
    adapter = SparkAdapter(markets_config=markets_config, rpc_client=fixture_rpc_client)

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=build_full_price_map(markets_config, price_usd=Decimal("1")),
    )

    assert not issues
    assert snapshots

    for snapshot in snapshots:
        assert snapshot.max_ltv == Decimal("0.75")
        assert snapshot.liquidation_threshold == Decimal("0.8")
        assert snapshot.liquidation_penalty == Decimal("0.04")
