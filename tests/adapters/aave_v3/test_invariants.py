"""Invariant tests for Aave v3 adapter snapshots."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.aave_v3.adapter import AaveV3Adapter
from core.config import MarketsConfig
from tests.adapters.aave_v3.conftest import FixtureAaveRpcClient, build_full_price_map


def test_position_and_market_invariants(
    markets_config: MarketsConfig,
    fixture_rpc_client: FixtureAaveRpcClient,
) -> None:
    adapter = AaveV3Adapter(markets_config=markets_config, rpc_client=fixture_rpc_client)
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

    epsilon = Decimal("0.0000000001")
    for position in positions:
        assert position.supplied_usd >= Decimal("0")
        assert position.borrowed_usd >= Decimal("0")
        assert abs(position.equity_usd - (position.supplied_usd - position.borrowed_usd)) <= epsilon

    for snapshot in market_snapshots:
        assert Decimal("0") <= snapshot.utilization <= Decimal("1.5")
