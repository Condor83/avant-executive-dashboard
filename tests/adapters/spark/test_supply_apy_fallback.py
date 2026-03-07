"""Supply APY fallback tests for Spark reserves with external yield."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from adapters.aave_v3.adapter import (
    ReserveCaps,
    ReserveData,
    ReserveRiskConfiguration,
    UserAccountData,
    UserReserveData,
)
from adapters.spark.adapter import SparkAdapter
from core.config import MarketsConfig


@dataclass(frozen=True)
class _StubRpc:
    def close(self) -> None:  # pragma: no cover
        return

    def get_block_number(self, chain_code: str) -> int:
        del chain_code
        return 123

    def get_user_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
        wallet_address: str,
    ) -> UserReserveData:
        del chain_code, pool_data_provider, asset, wallet_address
        return UserReserveData(
            current_a_token_balance=10**18,
            current_stable_debt=0,
            current_variable_debt=0,
        )

    def get_user_account_data(
        self,
        chain_code: str,
        pool: str,
        wallet_address: str,
    ) -> UserAccountData:
        del chain_code, pool, wallet_address
        return UserAccountData(ltv_bps=0, health_factor_wad=int(Decimal("2") * Decimal("1e18")))

    def get_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveData:
        del chain_code, pool_data_provider, asset
        return ReserveData(
            total_a_token=2 * 10**18,
            total_stable_debt=0,
            total_variable_debt=0,
            liquidity_rate_ray=0,
            variable_borrow_rate_ray=0,
        )

    def get_reserve_caps(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveCaps:
        del chain_code, pool_data_provider, asset
        return ReserveCaps(borrow_cap=0, supply_cap=0)

    def get_reserve_risk_configuration(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveRiskConfiguration:
        del chain_code, pool_data_provider, asset
        return ReserveRiskConfiguration(
            ltv_bps=8_000,
            liquidation_threshold_bps=8_500,
            liquidation_bonus_bps=10_500,
        )


def _markets_with_fallback() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "spark": {
                "ethereum": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": ["0x3333333333333333333333333333333333333333"],
                    "markets": [
                        {
                            "symbol": "weETH",
                            "asset": "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee",
                            "decimals": 18,
                            "supply_apy_fallback_pool_id": "46bd2bdf-6d92-4066-b482-e885ee172264",
                        }
                    ],
                }
            },
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
            "traderjoe_lp": {},
            "stakedao": {},
            "etherex": {},
        }
    )


def test_supply_apy_uses_fallback_when_protocol_rate_is_zero(monkeypatch) -> None:
    adapter = SparkAdapter(markets_config=_markets_with_fallback(), rpc_client=_StubRpc())
    monkeypatch.setattr(adapter, "_fetch_defillama_pool_apy", lambda _pool_id: Decimal("0.0249"))

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 7, 0, 0, tzinfo=UTC),
        prices_by_token={("ethereum", "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"): Decimal("1")},
    )

    assert not issues
    assert len(positions) == 1
    assert positions[0].supply_apy == Decimal("0.0249")
    assert positions[0].reward_apy == Decimal("0")


def test_market_snapshot_metadata_records_fallback_source(monkeypatch) -> None:
    adapter = SparkAdapter(markets_config=_markets_with_fallback(), rpc_client=_StubRpc())
    pool_id = "46bd2bdf-6d92-4066-b482-e885ee172264"
    monkeypatch.setattr(adapter, "_fetch_defillama_pool_apy", lambda _pool_id: Decimal("0.0249"))

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 7, 0, 0, tzinfo=UTC),
        prices_by_token={("ethereum", "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee"): Decimal("1")},
    )

    assert not issues
    assert len(snapshots) == 1
    assert snapshots[0].supply_apy == Decimal("0.0249")
    assert snapshots[0].irm_params_json is not None
    assert snapshots[0].irm_params_json["supply_apy_source"] == "defillama_pool_fallback"
    assert snapshots[0].irm_params_json["supply_apy_fallback_pool_id"] == pool_id
