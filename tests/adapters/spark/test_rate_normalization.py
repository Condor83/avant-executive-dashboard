"""Spark APY normalization tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.spark.adapter import SparkAdapter
from core.config import MarketsConfig
from tests.adapters.spark.conftest import FixtureSparkRpcClient, build_full_price_map


def test_rates_are_normalized_to_unit_interval(
    markets_config: MarketsConfig,
    fixture_rpc_client: FixtureSparkRpcClient,
) -> None:
    adapter = SparkAdapter(markets_config=markets_config, rpc_client=fixture_rpc_client)
    prices_by_token = build_full_price_map(markets_config, price_usd=Decimal("1"))

    positions, position_issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )
    market_snapshots, market_issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )

    assert not position_issues
    assert not market_issues
    assert positions
    assert market_snapshots

    for position in positions:
        assert Decimal("0") <= position.supply_apy <= Decimal("1")
        assert Decimal("0") <= position.borrow_apy <= Decimal("1")
        assert Decimal("0") <= position.reward_apy <= Decimal("1")

    for snapshot in market_snapshots:
        assert Decimal("0") <= snapshot.supply_apy <= Decimal("1")
        assert Decimal("0") <= snapshot.borrow_apy <= Decimal("1")
