"""Euler v2 golden wallet integration tests with deterministic RPC fixtures."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from adapters.euler_v2.adapter import EulerV2Adapter
from core.config import MarketsConfig, load_markets_config
from core.yields import AvantYieldOracle


class FakeEulerClient:
    def __init__(
        self,
        *,
        vault_assets: dict[str, str],
        asset_decimals: dict[str, int],
        total_assets: dict[str, int],
        total_borrows: dict[str, int],
        interest_rates: dict[str, int],
        interest_fees: dict[str, int],
        share_balances: dict[tuple[str, str], int],
        converted_assets: dict[tuple[str, int], int],
        debts: dict[tuple[str, str], int],
    ) -> None:
        self.vault_assets = vault_assets
        self.asset_decimals = asset_decimals
        self.total_assets = total_assets
        self.total_borrows = total_borrows
        self.interest_rates = interest_rates
        self.interest_fees = interest_fees
        self.share_balances = share_balances
        self.converted_assets = converted_assets
        self.debts = debts

    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "avalanche"
        return 60_000_000

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        assert chain_code == "avalanche"
        return self.vault_assets[vault_address]

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        assert chain_code == "avalanche"
        return self.asset_decimals[token_address]

    def get_total_assets(self, chain_code: str, vault_address: str) -> int:
        assert chain_code == "avalanche"
        return self.total_assets[vault_address]

    def get_total_borrows(self, chain_code: str, vault_address: str) -> int:
        assert chain_code == "avalanche"
        return self.total_borrows[vault_address]

    def get_interest_rate(self, chain_code: str, vault_address: str) -> int:
        assert chain_code == "avalanche"
        return self.interest_rates[vault_address]

    def get_interest_fee(self, chain_code: str, vault_address: str) -> int | None:
        assert chain_code == "avalanche"
        return self.interest_fees[vault_address]

    def get_balance_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        assert chain_code == "avalanche"
        return self.share_balances[(vault_address, wallet_address)]

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        assert chain_code == "avalanche"
        return self.converted_assets[(vault_address, shares)]

    def get_debt_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        assert chain_code == "avalanche"
        return self.debts[(vault_address, wallet_address)]


class _StubAvantYieldOracle:
    def __init__(self, apy: Decimal | None, *, error: Exception | None = None) -> None:
        self.apy = apy
        self.error = error
        self.calls: list[str] = []

    def close(self) -> None:
        return None

    def get_token_apy(self, symbol: str) -> Decimal:
        self.calls.append(symbol)
        if self.error is not None:
            raise self.error
        if self.apy is None:
            raise RuntimeError("apy not configured")
        return self.apy


def _minimal_euler_config() -> tuple[MarketsConfig, list[str], list[str]]:
    full = load_markets_config(Path("config/markets.yaml"))
    chain = full.euler_v2["avalanche"]

    wallets = [wallet.lower() for wallet in chain.wallets[:2]]
    vaults = chain.vaults[:2]
    vault_addresses = [vault.address.lower() for vault in vaults]

    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {
                "avalanche": {
                    "wallets": wallets,
                    "vaults": [vault.model_dump() for vault in vaults],
                }
            },
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    return cfg, wallets, vault_addresses


def _subaccount_address(wallet: str, account_id: int) -> str:
    return f"0x{(int(wallet, 16) ^ account_id):040x}"


def test_euler_golden_wallets_returns_positions_and_market_snapshots() -> None:
    cfg, wallets, vaults = _minimal_euler_config()

    usdc = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
    ausd = "0x00000000efe302beaa2b3e6e1b18d08d69a9012a"

    share_balances: dict[tuple[str, str], int] = {
        (vaults[0], wallets[0]): 120_000_000,
        (vaults[0], wallets[1]): 60_000_000,
        (vaults[1], wallets[0]): 80_000_000,
        (vaults[1], wallets[1]): 40_000_000,
    }
    converted_assets: dict[tuple[str, int], int] = {
        (vaults[0], 120_000_000): 125_000_000,
        (vaults[0], 60_000_000): 62_500_000,
        (vaults[1], 80_000_000): 84_000_000,
        (vaults[1], 40_000_000): 42_000_000,
    }

    client = FakeEulerClient(
        vault_assets={
            vaults[0]: usdc,
            vaults[1]: ausd,
        },
        asset_decimals={
            usdc: 6,
            ausd: 6,
        },
        total_assets={
            vaults[0]: 1_000_000_000,
            vaults[1]: 900_000_000,
        },
        total_borrows={
            vaults[0]: 300_000_000,
            vaults[1]: 200_000_000,
        },
        interest_rates={
            vaults[0]: 1_600_000_000,
            vaults[1]: 1_250_000_000,
        },
        interest_fees={
            vaults[0]: 1000,
            vaults[1]: 500,
        },
        share_balances=share_balances,
        converted_assets=converted_assets,
        debts={
            (vaults[0], wallets[0]): 20_000_000,
            (vaults[0], wallets[1]): 0,
            (vaults[1], wallets[0]): 10_000_000,
            (vaults[1], wallets[1]): 5_000_000,
        },
    )

    adapter = EulerV2Adapter(markets_config=cfg, rpc_client=client)
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    prices = {
        ("avalanche", usdc): Decimal("1"),
        ("avalanche", ausd): Decimal("1"),
    }

    position_rows, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )
    market_rows, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(position_rows) == 4
    assert not position_issues
    assert len(market_rows) == 2
    assert not market_issues

    assert {row.wallet_address for row in position_rows} == set(wallets)
    assert {row.market_ref for row in position_rows} == set(vaults)

    for row in position_rows:
        assert row.supply_apy >= Decimal("0")
        assert row.borrow_apy >= Decimal("0")
        assert row.reward_apy == Decimal("0")


def test_euler_subaccount_single_pair_synthesizes_consumer_market() -> None:
    wallet = "0xd1b366727b30d996b81018bb31ec37f64ed8ad28"
    vault_supply = "0xbac3983342b805e66f8756e265b3b0ddf4b685fc"
    vault_borrow = "0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e"
    usdc = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
    savusd = "0x00000000efe302beaa2b3e6e1b18d08d69a9012a"

    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {
                "avalanche": {
                    "wallets": [wallet],
                    "account_ids": [2],
                    "vaults": [
                        {
                            "address": vault_supply,
                            "symbol": "eava9savUSD",
                            "asset_address": savusd,
                            "asset_symbol": "savUSD",
                            "asset_decimals": 6,
                        },
                        {
                            "address": vault_borrow,
                            "symbol": "eava9USDC",
                            "asset_address": usdc,
                            "asset_symbol": "USDC",
                            "asset_decimals": 6,
                        },
                    ],
                }
            },
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )

    subaccount = _subaccount_address(wallet, 2)

    client = FakeEulerClient(
        vault_assets={
            vault_supply: savusd,
            vault_borrow: usdc,
        },
        asset_decimals={
            savusd: 6,
            usdc: 6,
        },
        total_assets={
            vault_supply: 2_000_000_000,
            vault_borrow: 1_500_000_000,
        },
        total_borrows={
            vault_supply: 100_000_000,
            vault_borrow: 900_000_000,
        },
        interest_rates={
            vault_supply: 1_100_000_000,
            vault_borrow: 1_800_000_000,
        },
        interest_fees={
            vault_supply: 500,
            vault_borrow: 500,
        },
        share_balances={
            (vault_supply, subaccount): 400_000_000,
            (vault_borrow, subaccount): 0,
        },
        converted_assets={
            (vault_supply, 400_000_000): 410_000_000,
            (vault_borrow, 0): 0,
        },
        debts={
            (vault_supply, subaccount): 0,
            (vault_borrow, subaccount): 120_000_000,
        },
    )
    adapter = EulerV2Adapter(markets_config=cfg, rpc_client=client)

    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    prices = {
        ("avalanche", usdc): Decimal("1"),
        ("avalanche", savusd): Decimal("1"),
    }

    position_rows, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )

    assert len(position_rows) == 1
    assert not position_issues
    row = position_rows[0]
    assert row.market_ref == f"{vault_supply}/{vault_borrow}"
    assert row.position_key.endswith(":acct2")
    assert row.supplied_usd == Decimal("410")
    assert row.borrowed_usd == Decimal("120")


def test_euler_uses_avant_api_for_avant_native_supplied_asset() -> None:
    wallet = "0xd1b366727b30d996b81018bb31ec37f64ed8ad28"
    vault_supply = "0xbac3983342b805e66f8756e265b3b0ddf4b685fc"
    vault_borrow = "0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e"
    usdc = "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"
    savusd = "0x06d47f3fb376649c3a9dafe069b3d6e35572219e"

    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {
                "avalanche": {
                    "wallets": [wallet],
                    "account_ids": [2],
                    "vaults": [
                        {
                            "address": vault_supply,
                            "symbol": "eavUSD",
                            "asset_address": savusd,
                            "asset_symbol": "savUSD",
                            "asset_decimals": 18,
                        },
                        {
                            "address": vault_borrow,
                            "symbol": "eUSDC",
                            "asset_address": usdc,
                            "asset_symbol": "USDC",
                            "asset_decimals": 6,
                        },
                    ],
                }
            },
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )

    subaccount = _subaccount_address(wallet, 2)
    client = FakeEulerClient(
        vault_assets={
            vault_supply: savusd,
            vault_borrow: usdc,
        },
        asset_decimals={
            savusd: 18,
            usdc: 6,
        },
        total_assets={
            vault_supply: 2_000_000_000_000_000_000,
            vault_borrow: 1_500_000_000,
        },
        total_borrows={
            vault_supply: 100_000_000_000_000_000,
            vault_borrow: 900_000_000,
        },
        interest_rates={
            vault_supply: 1_100_000_000,
            vault_borrow: 1_800_000_000,
        },
        interest_fees={
            vault_supply: 500,
            vault_borrow: 500,
        },
        share_balances={
            (vault_supply, subaccount): 400_000_000_000_000_000,
            (vault_borrow, subaccount): 0,
        },
        converted_assets={
            (vault_supply, 400_000_000_000_000_000): 410_000_000_000_000_000,
            (vault_borrow, 0): 0,
        },
        debts={
            (vault_supply, subaccount): 0,
            (vault_borrow, subaccount): 120_000_000,
        },
    )
    avant_oracle = _StubAvantYieldOracle(Decimal("0.0745"))
    adapter = EulerV2Adapter(
        markets_config=cfg,
        rpc_client=client,
        avant_yield_oracle=cast(AvantYieldOracle, avant_oracle),
    )

    rows, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        prices_by_token={
            ("avalanche", savusd): Decimal("1"),
            ("avalanche", usdc): Decimal("1"),
        },
    )

    assert len(rows) == 1
    assert not issues
    assert rows[0].market_ref == f"{vault_supply}/{vault_borrow}"
    assert rows[0].supply_apy == Decimal("0.0745")
    assert rows[0].borrow_apy > Decimal("0")
    assert avant_oracle.calls == ["savUSD"]


def test_euler_avant_api_failure_falls_back_to_protocol_supply_apy() -> None:
    wallet = "0xd1b366727b30d996b81018bb31ec37f64ed8ad28"
    vault_supply = "0xbac3983342b805e66f8756e265b3b0ddf4b685fc"
    savusd = "0x06d47f3fb376649c3a9dafe069b3d6e35572219e"

    cfg = MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {
                "avalanche": {
                    "wallets": [wallet],
                    "vaults": [
                        {
                            "address": vault_supply,
                            "symbol": "eavUSD",
                            "asset_address": savusd,
                            "asset_symbol": "savUSD",
                            "asset_decimals": 18,
                        },
                    ],
                }
            },
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )
    client = FakeEulerClient(
        vault_assets={vault_supply: savusd},
        asset_decimals={savusd: 18},
        total_assets={vault_supply: 2_000_000_000_000_000_000},
        total_borrows={vault_supply: 100_000_000_000_000_000},
        interest_rates={vault_supply: 1_100_000_000},
        interest_fees={vault_supply: 500},
        share_balances={(vault_supply, wallet): 400_000_000_000_000_000},
        converted_assets={(vault_supply, 400_000_000_000_000_000): 410_000_000_000_000_000},
        debts={(vault_supply, wallet): 0},
    )
    avant_oracle = _StubAvantYieldOracle(None, error=RuntimeError("avant unavailable"))
    adapter = EulerV2Adapter(
        markets_config=cfg,
        rpc_client=client,
        avant_yield_oracle=cast(AvantYieldOracle, avant_oracle),
    )

    rows, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 12, 0, tzinfo=UTC),
        prices_by_token={("avalanche", savusd): Decimal("1")},
    )

    assert len(rows) == 1
    assert rows[0].supply_apy > Decimal("0")
    assert len(issues) == 1
    assert issues[0].error_type == "euler_underlying_apy_fetch_failed"
    assert issues[0].payload_json is not None
    assert issues[0].payload_json["symbol"] == "savUSD"
