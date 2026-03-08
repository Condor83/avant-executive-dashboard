"""Kamino obligation APY weighting and fallback tests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from adapters.kamino.adapter import (
    KaminoAdapter,
    KaminoClient,
    KaminoMarketStats,
    KaminoObligationSnapshot,
    KaminoReserveRate,
)
from core.config import MarketsConfig


class _StubKaminoClient(KaminoClient):
    def close(self) -> None:
        return

    def get_market_stats(self, chain_code: str, market_pubkey: str) -> KaminoMarketStats:
        del chain_code, market_pubkey
        return KaminoMarketStats(
            total_supply_usd=Decimal("25000000"),
            total_borrow_usd=Decimal("18000000"),
            supply_apy=Decimal("0.02"),
            borrow_apy=Decimal("0.03"),
            reserve_rates={
                "reserve-syrup": KaminoReserveRate(
                    reserve_ref="reserve-syrup",
                    liquidity_token="syrupUSDC",
                    liquidity_token_mint=None,
                    max_ltv=Decimal("0.88"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0.0001"),
                    total_supply_usd=Decimal("20000000"),
                    total_borrow_usd=Decimal("0"),
                ),
                "reserve-pyusd": KaminoReserveRate(
                    reserve_ref="reserve-pyusd",
                    liquidity_token="PYUSD",
                    liquidity_token_mint=None,
                    max_ltv=Decimal("0"),
                    supply_apy=Decimal("0.02"),
                    borrow_apy=Decimal("0.07"),
                    total_supply_usd=Decimal("5000000"),
                    total_borrow_usd=Decimal("18000000"),
                ),
            },
        )

    def get_user_obligations(
        self,
        *,
        chain_code: str,
        market_pubkey: str,
        wallet_address: str,
    ) -> list[KaminoObligationSnapshot]:
        del chain_code, market_pubkey, wallet_address
        return [
            KaminoObligationSnapshot(
                obligation_ref="obligation-1",
                supplied_usd=Decimal("17300000"),
                borrowed_usd=Decimal("15580000"),
                deposit_reserve_values={
                    "reserve-syrup": Decimal("9"),
                    "reserve-pyusd": Decimal("1"),
                },
                deposit_reserve_raw_amounts={
                    "reserve-syrup": Decimal("9000000"),
                    "reserve-pyusd": Decimal("1000000"),
                },
                borrow_reserve_values={
                    "reserve-pyusd": Decimal("10"),
                },
                borrow_reserve_raw_amounts={
                    "reserve-pyusd": Decimal("15580000"),
                },
            )
        ]


class _SingleSupplyKaminoClient(KaminoClient):
    def close(self) -> None:
        return

    def get_market_stats(self, chain_code: str, market_pubkey: str) -> KaminoMarketStats:
        del chain_code, market_pubkey
        return KaminoMarketStats(
            total_supply_usd=Decimal("25000000"),
            total_borrow_usd=Decimal("18000000"),
            supply_apy=Decimal("0.02"),
            borrow_apy=Decimal("0.03"),
            reserve_rates={
                "reserve-syrup": KaminoReserveRate(
                    reserve_ref="reserve-syrup",
                    liquidity_token="syrupUSDC",
                    liquidity_token_mint=None,
                    max_ltv=Decimal("0.88"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0.0001"),
                    total_supply_usd=Decimal("20000000"),
                    total_borrow_usd=Decimal("0"),
                ),
                "reserve-pyusd": KaminoReserveRate(
                    reserve_ref="reserve-pyusd",
                    liquidity_token="PYUSD",
                    liquidity_token_mint=None,
                    max_ltv=Decimal("0"),
                    supply_apy=Decimal("0.02"),
                    borrow_apy=Decimal("0.07"),
                    total_supply_usd=Decimal("5000000"),
                    total_borrow_usd=Decimal("18000000"),
                ),
            },
        )

    def get_user_obligations(
        self,
        *,
        chain_code: str,
        market_pubkey: str,
        wallet_address: str,
    ) -> list[KaminoObligationSnapshot]:
        del chain_code, market_pubkey, wallet_address
        return [
            KaminoObligationSnapshot(
                obligation_ref="obligation-1",
                supplied_usd=Decimal("17300000"),
                borrowed_usd=Decimal("15580000"),
                deposit_reserve_values={
                    "reserve-syrup": Decimal("10"),
                },
                deposit_reserve_raw_amounts={
                    "reserve-syrup": Decimal("9000000"),
                },
                borrow_reserve_values={
                    "reserve-pyusd": Decimal("10"),
                },
                borrow_reserve_raw_amounts={
                    "reserve-pyusd": Decimal("15580000"),
                },
            )
        ]


@dataclass(frozen=True)
class _StubYieldOracle:
    apy: Decimal

    def close(self) -> None:
        return

    def get_pool_apy(self, pool_id: str) -> Decimal:
        assert pool_id == "43641cf5-a92e-416b-bce9-27113d3c0db6"
        return self.apy


def _kamino_markets_with_fallback() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {
                "solana": {
                    "wallets": ["29KnJ9mtSMbxAFExns4H8e5Tv9AqSb1Qazy1HhHdUbNH"],
                    "markets": [
                        {
                            "market_pubkey": "6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y",
                            "name": "Maple Market",
                            "defillama_pool_id": "43641cf5-a92e-416b-bce9-27113d3c0db6",
                        }
                    ],
                }
            },
            "zest": {},
            "wallet_balances": {},
        }
    )


def _kamino_markets_with_token_expectations() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {
                "solana": {
                    "wallets": ["29KnJ9mtSMbxAFExns4H8e5Tv9AqSb1Qazy1HhHdUbNH"],
                    "markets": [
                        {
                            "market_pubkey": "6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y",
                            "name": "Maple Market",
                            "defillama_pool_id": "43641cf5-a92e-416b-bce9-27113d3c0db6",
                            "supply_token": {
                                "symbol": "syrupUSDC",
                                "mint": "AvZZF1YaZDziPY2RCK4oJrRVrbN3mTD9NL24hPeaZeUj",
                                "decimals": 6,
                            },
                            "borrow_token": {
                                "symbol": "PYUSD",
                                "mint": "2b1kV6DkPAnxd5ixfnxCpjxmKwqjjaYmCZfHsFu24GXo",
                                "decimals": 6,
                            },
                        }
                    ],
                }
            },
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_position_supply_apy_uses_defillama_fallback_for_zero_yield_collateral() -> None:
    adapter = KaminoAdapter(
        markets_config=_kamino_markets_with_fallback(),
        client=_StubKaminoClient(),
        yield_oracle=_StubYieldOracle(apy=Decimal("0.08")),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert not issues
    assert len(positions) == 1

    # Weighted supply APY: 90% syrup collateral at fallback 8% + 10% PYUSD at 2%.
    assert positions[0].supply_apy == Decimal("0.074")
    # Weighted borrow APY from PYUSD borrow reserve.
    assert positions[0].borrow_apy == Decimal("0.07")
    assert positions[0].equity_usd == positions[0].supplied_usd - positions[0].borrowed_usd


def test_collect_positions_flags_multi_supply_token_when_configured() -> None:
    adapter = KaminoAdapter(
        markets_config=_kamino_markets_with_token_expectations(),
        client=_StubKaminoClient(),
        yield_oracle=_StubYieldOracle(apy=Decimal("0.08")),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert len(positions) == 1
    issue_types = {issue.error_type for issue in issues}
    assert "kamino_multi_supply_token" in issue_types
    assert "kamino_supply_token_mismatch" not in issue_types
    assert "kamino_borrow_token_mismatch" not in issue_types


def test_collect_positions_uses_collateral_fields_for_single_configured_supply_token() -> None:
    adapter = KaminoAdapter(
        markets_config=_kamino_markets_with_token_expectations(),
        client=_SingleSupplyKaminoClient(),
        yield_oracle=_StubYieldOracle(apy=Decimal("0.08")),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert len(positions) == 1
    position = positions[0]
    assert position.supplied_amount == Decimal("0")
    assert position.supplied_usd == Decimal("0")
    assert position.collateral_amount == Decimal("9")
    assert position.collateral_usd == Decimal("17300000")
    assert position.borrowed_amount == Decimal("15.58")
    assert position.borrowed_usd == Decimal("15580000")
    assert position.equity_usd == Decimal("1720000")
    assert not issues
