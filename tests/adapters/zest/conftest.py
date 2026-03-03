"""Shared fixtures for Zest adapter tests."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path

import pytest

from adapters.zest.adapter import ZestClient, ZestMarketRates, ZestMarketTotalsRaw
from core.config import MarketsConfig, canonical_address, load_markets_config


@dataclass(frozen=True)
class _FixturePosition:
    supplied_raw: int
    borrowed_raw: int


class FixtureZestClient(ZestClient):
    """Deterministic in-memory Zest client fixture."""

    def __init__(self, markets_config: MarketsConfig) -> None:
        self._markets_config = markets_config
        self._first_wallet_by_chain: dict[str, str] = {}
        self._positions: dict[tuple[str, str], _FixturePosition] = {}
        self._z_token_to_market_ref: dict[tuple[str, str], str] = {}

        for chain_code, chain_config in markets_config.zest.items():
            if chain_config.wallets:
                self._first_wallet_by_chain[chain_code] = chain_config.wallets[0]
            for market in chain_config.markets:
                market_ref = canonical_address(market.asset_contract)
                self._z_token_to_market_ref[(chain_code, market.z_token)] = market_ref
                if market.symbol.lower() == "sbtc":
                    self._positions[(chain_code, market_ref)] = _FixturePosition(
                        supplied_raw=123_000_000,
                        borrowed_raw=25_000_000,
                    )
                elif market.symbol.lower() == "aeusdc":
                    self._positions[(chain_code, market_ref)] = _FixturePosition(
                        supplied_raw=2_500_000_000,
                        borrowed_raw=100_000_000,
                    )
                else:
                    self._positions[(chain_code, market_ref)] = _FixturePosition(
                        supplied_raw=0,
                        borrowed_raw=0,
                    )

    def close(self) -> None:
        return

    def get_block_height(self, chain_code: str) -> int:
        return 216_000 + len(chain_code)

    def get_wallet_supply_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        asset_contract: str,
        z_token_identifier: str,
        wallet_address: str,
    ) -> int:
        del pool_deployer, pool_read, asset_contract
        if wallet_address != self._first_wallet_by_chain.get(chain_code):
            return 0
        market_ref = self._z_token_to_market_ref.get((chain_code, z_token_identifier))
        if market_ref is None:
            return 0
        fixture = self._positions.get((chain_code, market_ref))
        if fixture is not None:
            return fixture.supplied_raw
        return 0

    def get_wallet_borrow_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        borrow_fn: str,
        wallet_address: str,
        asset_contract: str,
    ) -> int:
        del pool_deployer, pool_read, borrow_fn
        if wallet_address != self._first_wallet_by_chain.get(chain_code):
            return 0
        fixture = self._positions.get((chain_code, canonical_address(asset_contract)))
        if fixture is None:
            return 0
        return fixture.borrowed_raw

    def get_market_totals_raw(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        market_symbol: str,
        asset_contract: str,
        z_token_identifier: str,
    ) -> ZestMarketTotalsRaw | None:
        del pool_deployer, pool_read, market_symbol, z_token_identifier
        fixture = self._positions.get((chain_code, canonical_address(asset_contract)))
        if fixture is None:
            return None
        return ZestMarketTotalsRaw(
            total_supply_raw=fixture.supplied_raw * 20,
            total_borrow_raw=fixture.borrowed_raw * 20,
        )

    def get_market_rates(
        self,
        *,
        chain_code: str,
        pool_deployer: str,
        pool_read: str,
        market_symbol: str,
        asset_contract: str,
    ) -> ZestMarketRates | None:
        del chain_code, pool_deployer, pool_read, market_symbol, asset_contract
        return ZestMarketRates(supply_apy=Decimal("0.06"), borrow_apy=Decimal("0.1"))


def build_full_price_map(markets_config: MarketsConfig) -> dict[tuple[str, str], Decimal]:
    prices: dict[tuple[str, str], Decimal] = {}
    for chain_code, chain_config in markets_config.zest.items():
        for market in chain_config.markets:
            symbol_lower = market.symbol.lower()
            price = Decimal("1")
            if symbol_lower == "sbtc":
                price = Decimal("100000")
            prices[(chain_code, canonical_address(market.asset_contract))] = price
    return prices


@pytest.fixture()
def markets_config() -> MarketsConfig:
    return load_markets_config(Path("config/markets.yaml"))


@pytest.fixture()
def fixture_zest_client(markets_config: MarketsConfig) -> FixtureZestClient:
    return FixtureZestClient(markets_config)
