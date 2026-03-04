"""Stake DAO adapter tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.stakedao.adapter import StakedaoAdapter
from core.config import MarketsConfig


class _StubStakedaoRpcClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "ethereum"
        return 12_345

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        assert chain_code == "ethereum"
        assert token_address == "0xad1dcc6ca212673d2dfd403a905a1ec57666d910"
        assert wallet_address == "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        return 100 * 10**18

    def convert_to_assets(self, chain_code: str, vault_address: str, shares_raw: int) -> int:
        assert chain_code == "ethereum"
        assert vault_address == "0xad1dcc6ca212673d2dfd403a905a1ec57666d910"
        assert shares_raw == 100 * 10**18
        return shares_raw

    def get_total_supply(self, chain_code: str, token_address: str) -> int:
        assert chain_code == "ethereum"
        assert token_address == "0x2e1776968ec75bfd13dbc5b94ae57034d7e85fb9"
        return 1_000 * 10**18

    def get_curve_balance(self, chain_code: str, pool_address: str, index: int) -> int:
        assert chain_code == "ethereum"
        assert pool_address == "0x2e1776968ec75bfd13dbc5b94ae57034d7e85fb9"
        if index == 0:
            return 2_000_000 * 10**6
        if index == 1:
            return 3_000 * 10**18
        raise AssertionError(index)

    def get_curve_coin(self, chain_code: str, pool_address: str, index: int) -> str:
        assert chain_code == "ethereum"
        assert pool_address == "0x2e1776968ec75bfd13dbc5b94ae57034d7e85fb9"
        if index == 0:
            return "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"
        if index == 1:
            return "0xb8D89678E75a973E74698c976716308abB8a46A4"
        raise AssertionError(index)


def _config() -> MarketsConfig:
    return MarketsConfig.model_validate(
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
            "stakedao": {
                "ethereum": {
                    "wallets": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                    "vaults": [
                        {
                            "vault_address": "0xad1dcc6ca212673d2dfd403a905a1ec57666d910",
                            "asset_address": "0x2e1776968ec75bfd13dbc5b94ae57034d7e85fb9",
                            "asset_decimals": 18,
                            "underlyings": [
                                {
                                    "symbol": "USDC",
                                    "address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                                    "decimals": 6,
                                    "pool_index": 0,
                                },
                                {
                                    "symbol": "savUSD",
                                    "address": "0xb8D89678E75a973E74698c976716308abB8a46A4",
                                    "decimals": 18,
                                    "pool_index": 1,
                                },
                            ],
                        }
                    ],
                }
            },
        }
    )


def test_stakedao_adapter_decomposes_vault_balance_into_underlyings() -> None:
    adapter = StakedaoAdapter(
        markets_config=_config(),
        rpc_client=_StubStakedaoRpcClient(),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 4, 0, 0, tzinfo=UTC),
        prices_by_token={
            ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
            ("ethereum", "0xb8d89678e75a973e74698c976716308abb8a46a4"): Decimal("1"),
        },
    )

    assert not issues
    assert len(positions) == 2

    by_market = {position.market_ref: position for position in positions}
    usdc_market = (
        "0xad1dcc6ca212673d2dfd403a905a1ec57666d910:0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
    )
    savusd_market = (
        "0xad1dcc6ca212673d2dfd403a905a1ec57666d910:0xb8d89678e75a973e74698c976716308abb8a46a4"
    )

    assert usdc_market in by_market
    assert savusd_market in by_market
    assert by_market[usdc_market].supplied_amount == Decimal("200000")
    assert by_market[savusd_market].supplied_amount == Decimal("300")
