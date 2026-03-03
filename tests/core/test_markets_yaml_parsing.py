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

    for chain_config in markets.aave_v3.values():
        for market in chain_config.markets:
            assert market.symbol
            assert market.asset
            assert market.decimals >= 0

    for chain_config in markets.morpho.values():
        assert chain_config.morpho
        for market in chain_config.markets:
            assert market.id
            assert market.loan_token
            assert market.collateral_token
            assert market.loan_decimals >= 0

    for chain_config in markets.euler_v2.values():
        for vault in chain_config.vaults:
            assert vault.address
            assert vault.symbol

    for chain_config in markets.dolomite.values():
        assert chain_config.margin
        for market in chain_config.markets:
            assert market.id >= 0
            assert market.symbol
            assert market.decimals >= 0

    for wallet_balance_chain in markets.wallet_balances.values():
        for token in wallet_balance_chain.tokens:
            assert token.symbol
            assert token.address
            assert token.decimals >= 0
