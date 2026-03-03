"""Morpho adapter invariant tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.morpho.adapter import (
    MorphoAdapter,
    MorphoMarketParams,
    MorphoMarketState,
    MorphoPosition,
)
from core.config import MarketsConfig


class FakeMorphoClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        return 1

    def get_position(
        self,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_address: str,
    ) -> MorphoPosition:
        return MorphoPosition(
            supply_shares=400_000_000, borrow_shares=120_000_000, collateral=80_000_000
        )

    def get_market_state(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketState:
        return MorphoMarketState(
            total_supply_assets=2_000_000_000,
            total_supply_shares=2_000_000_000,
            total_borrow_assets=600_000_000,
            total_borrow_shares=600_000_000,
            last_update=1,
            fee=0,
        )

    def get_market_params(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketParams:
        return MorphoMarketParams(
            loan_token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            collateral_token="0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
            oracle="0x0000000000000000000000000000000000000001",
            irm="0x0000000000000000000000000000000000000002",
            lltv=915000000000000000,
        )

    def get_irm_borrow_rate(
        self,
        chain_code: str,
        market_params: MorphoMarketParams,
        market_state: MorphoMarketState,
    ) -> int:
        return 1_584_979_923

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        return 6


def _config() -> MarketsConfig:
    market_id = "0x729badf297ee9f2f6b3f717b96fd355fc6ec00422284ce1968e76647b258cf44"
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {
                "ethereum": {
                    "morpho": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
                    "wallets": [
                        "0x1491b385d4f80c524540b05a080179e5550ab0f9",
                        "0x564b58a82e656202541c47616fdca6d2e5ded968",
                    ],
                    "markets": [
                        {
                            "id": market_id,
                            "loan_token": "USDC",
                            "collateral_token": "syrupUSDC",
                            "loan_decimals": 6,
                            "defillama_pool_id": "43641cf5-a92e-416b-bce9-27113d3c0db6",
                        }
                    ],
                    "vaults": [],
                }
            },
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_morpho_invariants_hold() -> None:
    adapter = MorphoAdapter(markets_config=_config(), rpc_client=FakeMorphoClient())
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"): Decimal("1"),
    }

    positions, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )
    markets, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert not position_issues
    assert not market_issues
    assert positions
    assert markets

    epsilon = Decimal("1e-12")
    for row in positions:
        assert row.supplied_usd >= 0
        assert row.borrowed_usd >= 0
        assert abs(row.equity_usd - (row.supplied_usd - row.borrowed_usd)) <= epsilon

    for row in markets:
        assert Decimal("0") <= row.utilization <= Decimal("1.5")
