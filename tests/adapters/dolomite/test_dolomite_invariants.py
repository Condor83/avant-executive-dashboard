"""Dolomite adapter invariant tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.dolomite.adapter import (
    DolomiteAdapter,
    DolomiteMarketIndex,
    DolomiteMarketPar,
    DolomiteSignedWei,
)
from core.config import MarketsConfig


class FakeDolomiteClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        return 1

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        return "0x549943e04f40284185054145c6e4e9568c1d3241"

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        return 10**30

    def get_market_interest_rate(self, chain_code: str, margin_address: str, market_id: int) -> int:
        return 2_000_000_000

    def get_market_current_index(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketIndex:
        return DolomiteMarketIndex(
            borrow=1_050_000_000_000_000_000,
            supply=1_030_000_000_000_000_000,
            last_update=1,
        )

    def get_market_total_par(
        self, chain_code: str, margin_address: str, market_id: int
    ) -> DolomiteMarketPar:
        return DolomiteMarketPar(borrow=600_000_000, supply=1_300_000_000)

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> DolomiteSignedWei:
        return DolomiteSignedWei(is_positive=True, value=500_000_000)


def _config() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {
                "bera": {
                    "margin": "0x003ca23fd5f0ca87d01f6ec6cd14a8ae60c2b97d",
                    "wallets": ["0xc1d023141ad6935f81e5286e577768b75c9ff8eb"],
                    "markets": [
                        {"id": 2, "symbol": "USDC.e", "decimals": 6},
                        {"id": 3, "symbol": "HONEY", "decimals": 18},
                    ],
                }
            },
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_dolomite_invariants_hold() -> None:
    adapter = DolomiteAdapter(markets_config=_config(), rpc_client=FakeDolomiteClient())
    as_of = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    positions, position_issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token={})
    markets, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token={})

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
