"""Markets config parsing and required field assertions."""

from __future__ import annotations

from pathlib import Path

from core.config import load_markets_config


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
            assert morpho_market.collateral_token
            assert morpho_market.loan_decimals >= 0

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
            assert dolomite_market.decimals >= 0

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
