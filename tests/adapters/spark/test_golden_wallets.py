"""Deterministic golden-wallet tests for Spark adapter output."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.spark.adapter import SparkAdapter
from core.config import MarketsConfig, canonical_address
from core.types import PositionSnapshotInput
from tests.adapters.spark.conftest import FixtureSparkRpcClient, build_full_price_map


def _has_position_for_wallet(
    chain_code: str,
    wallet_address: str,
    positions: list[PositionSnapshotInput],
) -> bool:
    wallet_norm = canonical_address(wallet_address)
    return any(
        position.chain_code == chain_code
        and canonical_address(position.wallet_address) == wallet_norm
        for position in positions
    )


def test_golden_wallet_positions_are_emitted_for_fixture_wallets(
    markets_config: MarketsConfig,
    fixture_rpc_client: FixtureSparkRpcClient,
) -> None:
    adapter = SparkAdapter(markets_config=markets_config, rpc_client=fixture_rpc_client)
    prices_by_token = build_full_price_map(markets_config, price_usd=Decimal("1"))

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )

    assert not issues
    assert positions

    for required_chain in ("ethereum",):
        if required_chain not in markets_config.spark:
            continue
        wallet = markets_config.spark[required_chain].wallets[0]
        assert _has_position_for_wallet(required_chain, wallet, positions)

    for position in positions:
        assert position.supplied_amount == Decimal("125")
        assert position.borrowed_amount == Decimal("25")
        assert position.supplied_usd >= 0
        assert position.borrowed_usd >= 0
        assert position.equity_usd == position.supplied_usd - position.borrowed_usd
        assert position.supply_apy > Decimal("0")
        assert position.reward_apy == Decimal("0")
        assert position.block_number_or_slot is not None
        assert position.position_key.startswith("spark:")
