"""Deterministic golden-wallet tests for Zest adapter output."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.zest.adapter import ZestAdapter
from core.config import MarketsConfig, canonical_address
from tests.adapters.zest.conftest import FixtureZestClient, build_full_price_map


def test_zest_golden_wallet_positions_are_emitted(
    markets_config: MarketsConfig,
    fixture_zest_client: FixtureZestClient,
) -> None:
    adapter = ZestAdapter(markets_config=markets_config, client=fixture_zest_client)
    prices_by_token = build_full_price_map(markets_config)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=prices_by_token,
    )

    assert not issues
    assert positions

    chain_config = markets_config.zest["stacks"]
    fixture_wallet = chain_config.wallets[0]
    fixture_wallet_norm = canonical_address(fixture_wallet)
    emitted_for_wallet = [
        pos for pos in positions if canonical_address(pos.wallet_address) == fixture_wallet_norm
    ]
    assert len(emitted_for_wallet) == len(chain_config.markets)

    market_to_decimals = {
        canonical_address(market.asset_contract): market.decimals for market in chain_config.markets
    }
    for position in emitted_for_wallet:
        decimals = market_to_decimals[position.market_ref]
        if decimals == 8:  # sBTC
            assert position.supplied_amount == Decimal("1.23")
            assert position.borrowed_amount == Decimal("0.25")
        elif decimals == 6:  # aeUSDC
            assert position.supplied_amount == Decimal("2500")
            assert position.borrowed_amount == Decimal("100")
        else:
            raise AssertionError(f"unexpected decimals fixture path: {decimals}")

        assert position.supplied_usd >= Decimal("0")
        assert position.borrowed_usd >= Decimal("0")
        assert position.equity_usd == position.supplied_usd - position.borrowed_usd
        assert Decimal("0") <= position.supply_apy <= Decimal("1")
        assert Decimal("0") <= position.borrow_apy <= Decimal("1")
        assert position.reward_apy == Decimal("0")
