"""Silo v2 adapter market health tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest

from adapters.silo_v2.adapter import (
    SiloApiClient,
    SiloClient,
    SiloHolderPosition,
    SiloMarketHealth,
    SiloTokenMappingError,
    SiloV2Adapter,
    SiloWalletPosition,
)
from core.config import (
    ConsumerMarketsConfig,
    MarketsConfig,
    canonical_address,
    load_consumer_markets_config,
    load_markets_config,
)
from core.yields import AvantYieldOracle

TARGET_WALLET = canonical_address("0xdce3D16e0be9689bb7Ec449Cc1A1A257399dCe44")


class FixtureSiloClient(SiloClient):
    def __init__(self, *, mapping_error: bool = False) -> None:
        self.mapping_error = mapping_error

    def close(self) -> None:
        return

    def get_market_health(
        self,
        *,
        chain_code: str,
        market_ref: str,
        collateral_token_address: str,
        borrow_token_address: str,
    ) -> SiloMarketHealth:
        assert chain_code == "avalanche"
        assert collateral_token_address.startswith("0x")
        assert borrow_token_address.startswith("0x")
        if market_ref == "142":
            return SiloMarketHealth(
                total_supply_raw=None,
                total_borrow_raw=None,
                total_supply_usd=Decimal("2400000"),
                total_borrow_usd=Decimal("960000"),
                supply_apy=Decimal("0.055"),
                borrow_apy=Decimal("0.101"),
                utilization=Decimal("0.4"),
                available_liquidity_raw=1_440_000_000_000,
                max_ltv=Decimal("0.92"),
                block_number_or_slot="58650000",
                raw_payload={"market_id": 142},
            )
        return SiloMarketHealth(
            total_supply_raw=None,
            total_borrow_raw=None,
            total_supply_usd=Decimal("1800000"),
            total_borrow_usd=Decimal("720000"),
            supply_apy=Decimal("0.048"),
            borrow_apy=Decimal("0.088"),
            utilization=Decimal("0.4"),
            available_liquidity_raw=1_080_000_000,
            max_ltv=Decimal("0.87"),
            block_number_or_slot="58650001",
            raw_payload={"market_id": 121},
        )

    def get_wallet_position(
        self,
        *,
        chain_code: str,
        market_ref: str,
        wallet_address: str,
        collateral_token_address: str,
        borrow_token_address: str,
    ) -> SiloWalletPosition:
        assert chain_code == "avalanche"
        assert collateral_token_address.startswith("0x")
        assert borrow_token_address.startswith("0x")
        wallet_key = canonical_address(wallet_address)

        if market_ref == "142" and wallet_key == TARGET_WALLET:
            if self.mapping_error:
                raise SiloTokenMappingError("configured collateral token address missing")
            return SiloWalletPosition(
                wallet_address=wallet_key,
                supplied_raw=448341801018928769007702,
                borrowed_raw=477397361394,
                raw_payload={"wallet": wallet_key},
            )

        return SiloWalletPosition(
            wallet_address=wallet_key,
            supplied_raw=0,
            borrowed_raw=0,
            raw_payload={"wallet": wallet_key},
        )

    def get_top_holders(
        self, *, chain_code: str, market_ref: str, limit: int
    ) -> list[SiloHolderPosition]:
        raise AssertionError("holder reads should not be used in strategy wallet mode")


def _price_map(consumer_markets: ConsumerMarketsConfig) -> dict[tuple[str, str], Decimal]:
    prices: dict[tuple[str, str], Decimal] = {}
    for market in consumer_markets.markets:
        collateral_price = Decimal("1")
        borrow_price = Decimal("1")
        if market.collateral_token.symbol.lower() == "savbtc":
            collateral_price = Decimal("100000")
        if market.borrow_token.symbol.lower() == "btc.b":
            borrow_price = Decimal("100000")
        prices[(market.chain, canonical_address(market.collateral_token.address))] = (
            collateral_price
        )
        prices[(market.chain, canonical_address(market.borrow_token.address))] = borrow_price
    return prices


def test_silo_market_health_preserves_native_utilization_and_liquidity() -> None:
    markets_config: MarketsConfig = load_markets_config("config/markets.yaml")
    consumer_markets = load_consumer_markets_config("config/consumer_markets.yaml")
    adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets,
        client=FixtureSiloClient(),
        top_holders_limit=50,
    )

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(consumer_markets),
    )

    assert not issues
    assert snapshots
    for snapshot in snapshots:
        assert snapshot.utilization == Decimal("0.4")
        assert snapshot.available_liquidity_usd is not None
        assert snapshot.max_ltv is not None
        assert Decimal("0") <= snapshot.utilization <= Decimal("1.5")


def test_silo_strategy_positions_include_supply_plus_protected_and_borrow() -> None:
    markets_config: MarketsConfig = load_markets_config("config/markets.yaml")
    consumer_markets = load_consumer_markets_config("config/consumer_markets.yaml")
    adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets,
        client=FixtureSiloClient(mapping_error=False),
        top_holders_limit=50,
        include_strategy_wallets=True,
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(consumer_markets),
    )

    assert not issues
    assert positions

    position = next(
        (
            row
            for row in positions
            if row.market_ref == "142" and canonical_address(row.wallet_address) == TARGET_WALLET
        ),
        None,
    )
    assert position is not None
    assert position.supplied_amount == Decimal("448341.801018928769007702")
    assert position.borrowed_amount == Decimal("477397.361394")
    assert position.supplied_usd == position.supplied_amount
    assert position.borrowed_usd == position.borrowed_amount
    assert position.equity_usd == position.supplied_usd - position.borrowed_usd
    assert position.supply_apy == Decimal("0.055")
    assert position.borrow_apy == Decimal("0.101")


def test_silo_token_mapping_issue_is_emitted() -> None:
    markets_config: MarketsConfig = load_markets_config("config/markets.yaml")
    consumer_markets = load_consumer_markets_config("config/consumer_markets.yaml")
    adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets,
        client=FixtureSiloClient(mapping_error=True),
        top_holders_limit=50,
        include_strategy_wallets=True,
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(consumer_markets),
    )

    assert all(canonical_address(row.wallet_address) != TARGET_WALLET for row in positions)
    assert any(issue.error_type == "silo_token_mapping_mismatch" for issue in issues)


def test_silo_uses_avant_yield_for_avant_native_collateral() -> None:
    class StubAvantOracle:
        def get_token_apy(self, symbol: str) -> Decimal:
            assert symbol == "savUSD"
            return Decimal("0.0745")

    markets_config: MarketsConfig = load_markets_config("config/markets.yaml")
    consumer_markets = load_consumer_markets_config("config/consumer_markets.yaml")
    adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets,
        client=FixtureSiloClient(mapping_error=False),
        top_holders_limit=50,
        include_strategy_wallets=True,
        avant_yield_oracle=cast(AvantYieldOracle, StubAvantOracle()),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(consumer_markets),
    )

    assert not issues
    position = next(
        row
        for row in positions
        if row.market_ref == "142" and canonical_address(row.wallet_address) == TARGET_WALLET
    )
    assert position.supply_apy == Decimal("0.0745")


def test_silo_emits_dq_when_avant_yield_fetch_fails() -> None:
    class FailingAvantOracle:
        def get_token_apy(self, symbol: str) -> Decimal:
            raise RuntimeError(f"boom:{symbol}")

    markets_config: MarketsConfig = load_markets_config("config/markets.yaml")
    consumer_markets = load_consumer_markets_config("config/consumer_markets.yaml")
    adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets,
        client=FixtureSiloClient(mapping_error=False),
        top_holders_limit=50,
        include_strategy_wallets=True,
        avant_yield_oracle=cast(AvantYieldOracle, FailingAvantOracle()),
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(consumer_markets),
    )

    position = next(
        row
        for row in positions
        if row.market_ref == "142" and canonical_address(row.wallet_address) == TARGET_WALLET
    )
    assert position.supply_apy == Decimal("0.055")
    assert any(issue.error_type == "silo_underlying_apy_fetch_failed" for issue in issues)


def test_silo_api_client_wallet_position_normalizes_supply_and_borrow() -> None:
    payload = {
        "silo0": {
            "tokenAddress": "0x06d47F3fb376649c3A9Dafe069B3D6E35572219E",
            "collateralBalance": "1200",
            "protectedBalance": "300",
            "debtBalance": "0",
        },
        "silo1": {
            "tokenAddress": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "collateralBalance": "0",
            "protectedBalance": "0",
            "debtBalance": "50",
        },
    }

    class StubSiloApiClient(SiloApiClient):
        def __init__(self) -> None:
            super().__init__(base_url="https://example.com")

        def _get_json(
            self,
            path: str,
            params: object | None = None,
            *,
            base_url: str | None = None,
        ) -> object:
            del path, params, base_url
            return payload

    client = StubSiloApiClient()
    try:
        position = client.get_wallet_position(
            chain_code="avalanche",
            market_ref="142",
            wallet_address=TARGET_WALLET,
            collateral_token_address="0x06d47F3fb376649c3A9Dafe069B3D6E35572219E",
            borrow_token_address="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        )
    finally:
        client.close()

    assert position.supplied_raw == 1500
    assert position.borrowed_raw == 50


def test_silo_api_client_wallet_position_raises_on_token_mapping_mismatch() -> None:
    payload = {
        "silo0": {"tokenAddress": "0x1111111111111111111111111111111111111111"},
        "silo1": {"tokenAddress": "0x2222222222222222222222222222222222222222"},
    }

    class StubSiloApiClient(SiloApiClient):
        def __init__(self) -> None:
            super().__init__(base_url="https://example.com")

        def _get_json(
            self,
            path: str,
            params: object | None = None,
            *,
            base_url: str | None = None,
        ) -> object:
            del path, params, base_url
            return payload

    client = StubSiloApiClient()
    try:
        with pytest.raises(SiloTokenMappingError):
            client.get_wallet_position(
                chain_code="avalanche",
                market_ref="142",
                wallet_address=TARGET_WALLET,
                collateral_token_address="0x06d47F3fb376649c3A9Dafe069B3D6E35572219E",
                borrow_token_address="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            )
    finally:
        client.close()


def test_silo_api_client_market_health_uses_collateral_protected_and_debt_liquidity() -> None:
    payload = {
        "silo0": {
            "tokenAddress": "0x06d47F3fb376649c3A9Dafe069B3D6E35572219E",
            "collateralAccruedAssets": "1200",
            "protectedAssets": "300",
            "maxLtv": "920000000000000000",
        },
        "silo1": {
            "tokenAddress": "0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
            "collateralAccruedAssets": "500",
            "debtAccruedAssets": "200",
            "liquidity": "300",
            "utilization": "400000000000000000",
            "collateralBaseApr": "0.05",
            "debtBaseApr": "0.10",
            "maxLtv": "0",
        },
    }

    class StubSiloApiClient(SiloApiClient):
        def __init__(self) -> None:
            super().__init__(base_url="https://example.com")

        def _get_json(
            self,
            path: str,
            params: object | None = None,
            *,
            base_url: str | None = None,
        ) -> object:
            del path, params, base_url
            return payload

    client = StubSiloApiClient()
    try:
        health = client.get_market_health(
            chain_code="avalanche",
            market_ref="142",
            collateral_token_address="0x06d47F3fb376649c3A9Dafe069B3D6E35572219E",
            borrow_token_address="0xB97EF9Ef8734C71904D8002F8b6Bc66Dd9c48a6E",
        )
    finally:
        client.close()

    assert health.total_supply_raw == 1500
    assert health.total_borrow_raw == 200
    assert health.available_liquidity_raw == 300
    assert health.utilization == Decimal("0.4")
    assert health.max_ltv == Decimal("0.92")
