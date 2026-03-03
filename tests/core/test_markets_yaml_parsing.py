"""Markets config parsing and required field assertions."""

from __future__ import annotations

from pathlib import Path

from core.config import load_markets_config


def test_markets_yaml_has_expected_protocol_keys() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    assert set(markets.model_dump().keys()) == {
        "aave_v3",
        "morpho",
        "euler_v2",
        "dolomite",
        "kamino",
        "zest",
        "wallet_balances",
    }


def test_markets_and_tokens_have_required_fields() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))

    for aave_chain in markets.aave_v3.values():
        for aave_market in aave_chain.markets:
            assert aave_market.symbol
            assert aave_market.asset
            assert aave_market.decimals >= 0

    for morpho_chain in markets.morpho.values():
        assert morpho_chain.morpho
        for morpho_market in morpho_chain.markets:
            assert morpho_market.id
            assert morpho_market.loan_token
            assert morpho_market.collateral_token
            assert morpho_market.loan_decimals >= 0

    for euler_chain in markets.euler_v2.values():
        for vault in euler_chain.vaults:
            assert vault.address
            assert vault.symbol

    for dolomite_chain in markets.dolomite.values():
        assert dolomite_chain.margin
        assert dolomite_chain.account_numbers
        assert all(account_number >= 0 for account_number in dolomite_chain.account_numbers)
        for dolomite_market in dolomite_chain.markets:
            assert dolomite_market.id >= 0
            assert dolomite_market.symbol
            assert dolomite_market.decimals >= 0

    for wallet_balance_chain in markets.wallet_balances.values():
        for token in wallet_balance_chain.tokens:
            assert token.symbol
            assert token.address
            assert token.decimals >= 0
