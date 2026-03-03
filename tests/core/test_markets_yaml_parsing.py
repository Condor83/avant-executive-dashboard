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

    for wallet_balance_chain in markets.wallet_balances.values():
        for token in wallet_balance_chain.tokens:
            assert token.symbol
            assert token.address
            assert token.decimals >= 0
