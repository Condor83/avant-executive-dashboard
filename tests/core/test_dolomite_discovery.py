"""Tests for Dolomite wallet/account discovery helpers."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import yaml

from adapters.dolomite.adapter import DolomiteSignedWei
from core.config import DolomiteChainConfig, DolomiteMarket, WalletProductAssignment
from core.dolomite_discovery import discover_wallet_positions, wallet_candidates_for_groups


class FakeDiscoveryClient:
    def __init__(
        self,
        *,
        token_addresses: dict[int, str],
        prices_raw: dict[int, int],
        decimals_by_token: dict[str, int],
        account_wei: dict[tuple[str, int, int], DolomiteSignedWei],
        num_markets: int | None,
    ) -> None:
        self.token_addresses = token_addresses
        self.prices_raw = prices_raw
        self.decimals_by_token = decimals_by_token
        self.account_wei = account_wei
        self.num_markets = num_markets

    def get_num_markets(self, chain_code: str, margin_address: str) -> int:
        assert chain_code == "bera"
        assert margin_address
        if self.num_markets is None:
            raise RuntimeError("boom")
        return self.num_markets

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        assert chain_code == "bera"
        assert margin_address
        if market_id not in self.token_addresses:
            raise RuntimeError("unknown market")
        return self.token_addresses[market_id]

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        assert chain_code == "bera"
        assert margin_address
        return self.prices_raw[market_id]

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> DolomiteSignedWei:
        assert chain_code == "bera"
        assert margin_address
        return self.account_wei.get(
            (wallet_address, account_number, market_id),
            DolomiteSignedWei(is_positive=False, value=0),
        )

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        assert chain_code == "bera"
        return self.decimals_by_token[token_address]


def test_wallet_candidates_for_groups_prefers_legacy_wallets(tmp_path: Path) -> None:
    wallet_products_path = tmp_path / "wallet_products.yaml"
    wallet_products_path.write_text(
        yaml.safe_dump(
            {
                "STRATEGY_WALLETS": {
                    "avETH": ["0x1111111111111111111111111111111111111111"],
                    "avETHx": ["0x2222222222222222222222222222222222222222"],
                }
            }
        ),
        encoding="utf-8",
    )

    wallets, warnings = wallet_candidates_for_groups(
        wallet_products_path=wallet_products_path,
        wallet_groups=["avETH", "avETHx"],
        assignments=[],
    )

    assert wallets == [
        "0x1111111111111111111111111111111111111111",
        "0x2222222222222222222222222222222222222222",
    ]
    assert not warnings


def test_wallet_candidates_for_groups_fall_back_to_assignments(tmp_path: Path) -> None:
    wallet_products_path = tmp_path / "wallet_products.yaml"
    wallet_products_path.write_text("assignments: []\n", encoding="utf-8")

    assignments = [
        WalletProductAssignment(
            wallet_address="0xAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
            product_code="eth_senior",
            product_family="eth",
            tranche="senior",
            wallet_type="strategy",
        ),
        WalletProductAssignment(
            wallet_address="0xBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB",
            product_code="eth_junior",
            product_family="eth",
            tranche="junior",
            wallet_type="strategy",
        ),
    ]

    wallets, warnings = wallet_candidates_for_groups(
        wallet_products_path=wallet_products_path,
        wallet_groups=["avETH", "avETHx"],
        assignments=assignments,
    )

    assert wallets == [
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    ]
    assert not warnings


def test_discover_wallet_positions_scans_accounts_and_filters() -> None:
    chain_config = DolomiteChainConfig(
        margin="0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
        wallets=[],
        markets=[
            DolomiteMarket(id=0, symbol="USDC.e", decimals=6),
            DolomiteMarket(id=1, symbol="avETH", decimals=18),
        ],
        account_numbers=[0],
    )
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    client = FakeDiscoveryClient(
        token_addresses={
            0: "0x1111111111111111111111111111111111111111",
            1: "0x2222222222222222222222222222222222222222",
        },
        prices_raw={
            0: 10**30,
            1: 2 * 10**18,
        },
        decimals_by_token={
            "0x1111111111111111111111111111111111111111": 6,
            "0x2222222222222222222222222222222222222222": 18,
        },
        account_wei={
            (wallet_a, 2, 1): DolomiteSignedWei(
                is_positive=False,
                value=600_000_000_000_000_000_000,
            ),
            (wallet_b, 1, 0): DolomiteSignedWei(is_positive=True, value=2_000_000_000),
        },
        num_markets=2,
    )

    result = discover_wallet_positions(
        chain_code="bera",
        chain_config=chain_config,
        wallets=[wallet_a, wallet_b],
        rpc_client=client,
        max_account_number=3,
        min_abs_exposure_usd=Decimal("1000"),
    )

    assert not result.warnings
    assert [row.wallet_address for row in result.rows] == [wallet_b, wallet_a]
    assert result.rows[0].account_number == 1
    assert result.rows[0].market_id == 0
    assert result.rows[0].supplied_usd == Decimal("2000")
    assert result.rows[0].borrowed_usd == Decimal("0")
    assert result.rows[1].account_number == 2
    assert result.rows[1].market_id == 1
    assert result.rows[1].supplied_usd == Decimal("0")
    assert result.rows[1].borrowed_usd == Decimal("1200")
    assert result.market_ids_scanned == [0, 1]


def test_discover_wallet_positions_falls_back_when_get_num_markets_fails() -> None:
    chain_config = DolomiteChainConfig(
        margin="0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
        wallets=[],
        markets=[DolomiteMarket(id=2, symbol="USDC.e", decimals=6)],
        account_numbers=[0],
    )
    wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    client = FakeDiscoveryClient(
        token_addresses={2: "0x1111111111111111111111111111111111111111"},
        prices_raw={2: 10**30},
        decimals_by_token={"0x1111111111111111111111111111111111111111": 6},
        account_wei={(wallet, 0, 2): DolomiteSignedWei(is_positive=True, value=1_500_000_000)},
        num_markets=None,
    )

    result = discover_wallet_positions(
        chain_code="bera",
        chain_config=chain_config,
        wallets=[wallet],
        rpc_client=client,
        max_account_number=0,
        min_abs_exposure_usd=Decimal("1000"),
        fallback_probe_max_market_id=0,
    )

    assert any("get_num_markets failed" in warning for warning in result.warnings)
    assert len(result.rows) == 1
    assert result.rows[0].market_id == 2
    assert result.rows[0].supplied_usd == Decimal("1500")
