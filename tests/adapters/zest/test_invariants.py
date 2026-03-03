"""Invariant tests for Zest adapter snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.zest.adapter import ZestAdapter
from core.config import MarketsConfig
from tests.adapters.zest.conftest import FixtureZestClient, build_full_price_map


def test_zest_position_and_market_invariants(
    markets_config: MarketsConfig,
    fixture_zest_client: FixtureZestClient,
) -> None:
    adapter = ZestAdapter(markets_config=markets_config, client=fixture_zest_client)
    prices_by_token = build_full_price_map(markets_config)

    positions, position_issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )
    snapshots, market_issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )

    assert not position_issues
    assert not market_issues
    assert positions
    assert snapshots

    epsilon = Decimal("0.0000000001")
    for position in positions:
        assert position.supplied_usd >= Decimal("0")
        assert position.borrowed_usd >= Decimal("0")
        assert abs(position.equity_usd - (position.supplied_usd - position.borrowed_usd)) <= epsilon

    for snapshot in snapshots:
        expected_utilization = (
            Decimal("0")
            if snapshot.total_supply_usd == 0
            else snapshot.total_borrow_usd / snapshot.total_supply_usd
        )
        assert abs(snapshot.utilization - expected_utilization) <= epsilon
        assert Decimal("0") <= snapshot.utilization <= Decimal("1.5")
