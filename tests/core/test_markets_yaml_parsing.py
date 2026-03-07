"""Markets config parsing and required field assertions."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pytest

from core.config import MarketsConfig, load_markets_config


def test_markets_yaml_has_expected_protocol_keys() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    assert set(markets.model_dump().keys()) == {
        "aave_v3",
        "spark",
        "morpho",
        "euler_v2",
        "dolomite",
        "kamino",
        "zest",
        "wallet_balances",
        "traderjoe_lp",
        "stakedao",
        "etherex",
    }


def test_markets_and_tokens_have_required_fields() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    for aave_chain in markets.aave_v3.values():
        for aave_market in aave_chain.markets:
            assert aave_market.symbol
            assert aave_market.asset
            assert aave_market.decimals >= 0

    for spark_chain in markets.spark.values():
        for spark_market in spark_chain.markets:
            assert spark_market.symbol
            assert spark_market.asset
            assert spark_market.decimals >= 0

    for morpho_chain in markets.morpho.values():
        assert morpho_chain.morpho
        for morpho_market in morpho_chain.markets:
            assert morpho_market.id
            assert morpho_market.loan_token
            assert morpho_market.loan_token_address
            assert morpho_market.collateral_token
            assert morpho_market.collateral_token_address
            assert morpho_market.loan_decimals >= 0
            assert morpho_market.collateral_decimals is not None
            assert morpho_market.collateral_decimals >= 0
        for morpho_vault in morpho_chain.vaults:
            assert morpho_vault.address
            if morpho_vault.asset_address is not None:
                assert morpho_vault.asset_symbol
                assert morpho_vault.asset_decimals is not None
                assert morpho_vault.asset_decimals >= 0
            if morpho_vault.chain_id is not None:
                assert morpho_vault.chain_id > 0
            assert morpho_vault.apy_source == "morpho_api"
            assert morpho_vault.apy_lookback

    for euler_chain in markets.euler_v2.values():
        for euler_vault in euler_chain.vaults:
            assert euler_vault.address
            assert euler_vault.symbol
            assert euler_vault.asset_address
            assert euler_vault.asset_symbol
            assert euler_vault.asset_decimals >= 0

    for dolomite_chain in markets.dolomite.values():
        assert dolomite_chain.margin
        assert dolomite_chain.account_numbers
        assert all(account_number >= 0 for account_number in dolomite_chain.account_numbers)
        for dolomite_market in dolomite_chain.markets:
            assert dolomite_market.id >= 0
            assert dolomite_market.symbol
            assert dolomite_market.token_address
            assert dolomite_market.decimals >= 0


def test_dolomite_weeth_market_uses_defillama_carry_fallback() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    target = next(market for market in markets.dolomite["ethereum"].markets if market.id == 6)

    assert target.symbol == "weETH"
    assert target.token_address == "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"
    assert target.defillama_pool_id == "46bd2bdf-6d92-4066-b482-e885ee172264"

    for kamino_chain in markets.kamino.values():
        for kamino_market in kamino_chain.markets:
            assert kamino_market.market_pubkey
            assert kamino_market.name
            if kamino_market.supply_token is not None:
                assert kamino_market.supply_token.symbol
                assert kamino_market.supply_token.mint
                assert kamino_market.supply_token.decimals >= 0
            if kamino_market.borrow_token is not None:
                assert kamino_market.borrow_token.symbol
                assert kamino_market.borrow_token.mint
                assert kamino_market.borrow_token.decimals >= 0

    for wallet_balance_chain in markets.wallet_balances.values():
        for token in wallet_balance_chain.tokens:
            assert token.symbol
            assert token.address
            assert token.decimals >= 0

    for traderjoe_chain in markets.traderjoe_lp.values():
        for traderjoe_pool in traderjoe_chain.pools:
            assert traderjoe_pool.pool_address
            assert traderjoe_pool.pool_type
            assert traderjoe_pool.token_x_address
            assert traderjoe_pool.token_x_symbol
            assert traderjoe_pool.token_x_decimals >= 0
            assert traderjoe_pool.token_y_address
            assert traderjoe_pool.token_y_symbol
            assert traderjoe_pool.token_y_decimals >= 0
            assert traderjoe_pool.bin_ids

    for stakedao_chain in markets.stakedao.values():
        for stakedao_vault in stakedao_chain.vaults:
            assert stakedao_vault.vault_address
            assert stakedao_vault.asset_address
            assert stakedao_vault.asset_decimals >= 0
            assert stakedao_vault.apy_source == "fixed_apy_override"
            assert stakedao_vault.fixed_apy == Decimal("0.10")
            assert stakedao_vault.review_after is not None
            assert stakedao_vault.include_in_yield is True
            assert stakedao_vault.underlyings
            for underlying in stakedao_vault.underlyings:
                assert underlying.symbol
                assert underlying.address
                assert underlying.decimals >= 0
                assert underlying.pool_index >= 0

    for etherex_chain in markets.etherex.values():
        for etherex_pool in etherex_chain.pools:
            assert etherex_pool.pool_address
            assert etherex_pool.position_manager_address
            assert etherex_pool.token0_address
            assert etherex_pool.token0_symbol
            assert etherex_pool.token0_decimals >= 0
            assert etherex_pool.token1_address
            assert etherex_pool.token1_symbol
            assert etherex_pool.token1_decimals >= 0
            assert etherex_pool.fee >= 0


def test_morpho_susde_pyusd_market_uses_susde_carry_fallback() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    target = next(
        market
        for market in markets.morpho["ethereum"].markets
        if market.id.lower() == "0x90ef0c5a0dc7c4de4ad4585002d44e9d411d212d2f6258e94948beecf8b4c0d5"
    )

    assert target.collateral_token == "sUSDe"
    assert target.collateral_token_address == "0x9d39a5de30e57443bff2a8307a4256c8797a3497"
    assert target.loan_token == "PYUSD"
    assert target.loan_token_address == "0x6c3ea9036406852006290770bedfcaba0e23a0e8"
    assert target.collateral_decimals == 18
    assert target.defillama_pool_id == "66985a81-9c51-46ca-9977-42b4fe7bc6df"


def test_spark_weeth_market_uses_defillama_supply_fallback() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    target = next(
        market for market in markets.spark["ethereum"].markets if market.symbol == "weETH"
    )

    assert target.asset == "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"
    assert target.supply_apy_fallback_pool_id == "46bd2bdf-6d92-4066-b482-e885ee172264"


def test_stakedao_fixed_apy_override_requires_matching_source() -> None:
    with pytest.raises(ValueError, match="fixed_apy requires apy_source"):
        MarketsConfig.model_validate(
            {
                "aave_v3": {},
                "spark": {},
                "morpho": {},
                "euler_v2": {},
                "dolomite": {},
                "kamino": {},
                "zest": {},
                "wallet_balances": {},
                "traderjoe_lp": {},
                "etherex": {},
                "stakedao": {
                    "ethereum": {
                        "wallets": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "vaults": [
                            {
                                "vault_address": "0xad1dcc6ca212673d2dfd403a905a1ec57666d910",
                                "asset_address": "0x2e1776968ec75bfd13dbc5b94ae57034d7e85fb9",
                                "asset_decimals": 18,
                                "fixed_apy": "0.10",
                                "underlyings": [
                                    {
                                        "symbol": "USDC",
                                        "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                                        "decimals": 6,
                                        "pool_index": 0,
                                    }
                                ],
                            }
                        ],
                    }
                },
            }
        )
