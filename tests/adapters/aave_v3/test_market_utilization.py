"""Aave v3 market snapshot utilization tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.aave_v3.adapter import AaveV3Adapter
from core.config import MarketsConfig
from tests.adapters.aave_v3.conftest import FixtureAaveRpcClient, build_full_price_map


def test_market_utilization_identity(
    markets_config: MarketsConfig,
    fixture_rpc_client: FixtureAaveRpcClient,
) -> None:
    adapter = AaveV3Adapter(markets_config=markets_config, rpc_client=fixture_rpc_client)
    prices_by_token = build_full_price_map(markets_config, price_usd=Decimal("2"))

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )

    assert not issues
    assert snapshots

    epsilon = Decimal("0.0000000001")
    for snapshot in snapshots:
        expected = (
            Decimal("0")
            if snapshot.total_supply_usd == 0
            else snapshot.total_borrow_usd / snapshot.total_supply_usd
        )
        assert abs(snapshot.utilization - expected) <= epsilon
        assert Decimal("0") <= snapshot.utilization <= Decimal("1.5")
        assert snapshot.supply_apy >= Decimal("0")
        assert snapshot.borrow_apy >= Decimal("0")
        assert snapshot.irm_params_json is not None
        assert "supply_rate" in snapshot.irm_params_json
        assert snapshot.irm_params_json["optimal_usage_ratio"] == "0.92"
