"""Euler v2 adapter invariant tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.euler_v2.adapter import EulerV2Adapter
from core.config import MarketsConfig


class FakeEulerClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        return 1

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        return "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        return 6

    def get_total_assets(self, chain_code: str, vault_address: str) -> int:
        return 1_200_000_000

    def get_total_borrows(self, chain_code: str, vault_address: str) -> int:
        return 360_000_000

    def get_interest_rate(self, chain_code: str, vault_address: str) -> int:
        return 1_450_000_000

    def get_interest_fee(self, chain_code: str, vault_address: str) -> int | None:
        return 1000

    def get_balance_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        return 250_000_000

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        return 260_000_000

    def get_debt_of(self, chain_code: str, vault_address: str, wallet_address: str) -> int:
        return 60_000_000


def _config() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {
                "avalanche": {
                    "wallets": [
                        "0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd",
                        "0x7823bf8c30b8aa7b34348fb7ddc07738330517c4",
                    ],
                    "vaults": [
                        {
                            "address": "0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e",
                            "symbol": "eUSDC",
                            "asset_address": "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
                            "asset_symbol": "USDC",
                            "asset_decimals": 6,
                        }
                    ],
                }
            },
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_euler_invariants_hold() -> None:
    adapter = EulerV2Adapter(markets_config=_config(), rpc_client=FakeEulerClient())
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    prices = {("avalanche", "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"): Decimal("1")}

    positions, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )
    markets, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert not position_issues
    assert not market_issues
    assert positions
    assert markets

    epsilon = Decimal("1e-12")
    for position_row in positions:
        assert position_row.supplied_usd >= 0
        assert position_row.borrowed_usd >= 0
        assert (
            abs(position_row.equity_usd - (position_row.supplied_usd - position_row.borrowed_usd))
            <= epsilon
        )

    for market_row in markets:
        assert Decimal("0") <= market_row.utilization <= Decimal("1.5")


def test_euler_asset_mismatch_uses_configured_pricing_surface() -> None:
    payload = _config().model_dump()
    payload["euler_v2"]["avalanche"]["vaults"][0]["asset_address"] = (
        "0x00000000000000000000000000000000000000a1"
    )
    payload["euler_v2"]["avalanche"]["vaults"][0]["asset_symbol"] = "AUSD"
    cfg = MarketsConfig.model_validate(payload)

    adapter = EulerV2Adapter(markets_config=cfg, rpc_client=FakeEulerClient())
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    prices = {
        ("avalanche", "0x00000000000000000000000000000000000000a1"): Decimal("1"),
        ("avalanche", "0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e"): Decimal("1"),
    }

    positions, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )
    markets, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert positions
    assert markets
    assert any(issue.error_type == "euler_asset_mismatch" for issue in position_issues)
    assert any(issue.error_type == "euler_asset_mismatch" for issue in market_issues)
    assert not any(issue.error_type == "price_missing" for issue in position_issues)
    assert not any(issue.error_type == "price_missing" for issue in market_issues)
