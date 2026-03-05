"""Shared fixtures for Spark adapter tests."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest

from adapters.aave_v3.adapter import (
    ReserveCaps,
    ReserveData,
    ReserveRiskConfiguration,
    UserAccountData,
    UserReserveData,
)
from adapters.spark.adapter import SparkRpcClient
from core.config import MarketsConfig, canonical_address, load_markets_config


@dataclass(frozen=True)
class ActivePair:
    chain_code: str
    wallet_address: str
    market_ref: str


class FixtureSparkRpcClient(SparkRpcClient):
    """Deterministic in-memory Spark RPC fixture client."""

    def __init__(self, markets_config: MarketsConfig) -> None:
        self._markets = markets_config
        self._market_decimals: dict[tuple[str, str], int] = {}
        self._active_pairs: set[ActivePair] = set()

        for chain_code, chain_config in self._markets.spark.items():
            for market in chain_config.markets:
                market_ref = canonical_address(market.asset)
                self._market_decimals[(chain_code, market_ref)] = market.decimals

            if chain_config.wallets and chain_config.markets:
                self._active_pairs.add(
                    ActivePair(
                        chain_code=chain_code,
                        wallet_address=canonical_address(chain_config.wallets[0]),
                        market_ref=canonical_address(chain_config.markets[0].asset),
                    )
                )

    @staticmethod
    def _to_ray(rate: str) -> int:
        return int((Decimal(rate) * Decimal("1e27")).to_integral_value())

    @staticmethod
    def _scale(decimals: int) -> int:
        return 10**decimals

    def close(self) -> None:  # pragma: no cover
        return

    def get_block_number(self, chain_code: str) -> int:
        return 23_000_000 + len(chain_code)

    def get_user_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
        wallet_address: str,
    ) -> UserReserveData:
        del pool_data_provider
        market_ref = canonical_address(asset)
        wallet = canonical_address(wallet_address)
        decimals = self._market_decimals[(chain_code, market_ref)]
        scale = self._scale(decimals)

        active = ActivePair(chain_code=chain_code, wallet_address=wallet, market_ref=market_ref)
        if active in self._active_pairs:
            return UserReserveData(
                current_a_token_balance=125 * scale,
                current_stable_debt=5 * scale,
                current_variable_debt=20 * scale,
            )

        return UserReserveData(
            current_a_token_balance=0,
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
        return UserAccountData(
            ltv_bps=7_500,
            health_factor_wad=int(Decimal("1.85") * Decimal("1e18")),
        )

    def get_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveData:
        del pool_data_provider
        market_ref = canonical_address(asset)
        decimals = self._market_decimals[(chain_code, market_ref)]
        scale = self._scale(decimals)
        return ReserveData(
            total_a_token=1_500 * scale,
            total_stable_debt=50 * scale,
            total_variable_debt=250 * scale,
            liquidity_rate_ray=self._to_ray("0.042"),
            variable_borrow_rate_ray=self._to_ray("0.081"),
        )

    def get_reserve_caps(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveCaps:
        del chain_code, pool_data_provider, asset
        return ReserveCaps(
            borrow_cap=8_000_000,
            supply_cap=16_000_000,
        )

    def get_reserve_risk_configuration(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveRiskConfiguration:
        del chain_code, pool_data_provider, asset
        return ReserveRiskConfiguration(
            ltv_bps=7_500,
            liquidation_threshold_bps=8_000,
            liquidation_bonus_bps=10_400,
        )


@pytest.fixture()
def markets_config() -> MarketsConfig:
    return load_markets_config(Path("config/markets.yaml"))


@pytest.fixture()
def fixture_rpc_client(markets_config: MarketsConfig) -> FixtureSparkRpcClient:
    return FixtureSparkRpcClient(markets_config)


def build_full_price_map(
    markets_config: MarketsConfig,
    *,
    price_usd: Decimal = Decimal("1"),
) -> dict[tuple[str, str], Decimal]:
    prices: dict[tuple[str, str], Decimal] = {}
    for chain_code, chain_config in markets_config.spark.items():
        for market in chain_config.markets:
            prices[(chain_code, canonical_address(market.asset))] = price_usd
    return prices
