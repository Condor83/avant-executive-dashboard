"""Silo v2 adapter market health and top-holder tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.silo_v2.adapter import (
    SiloClient,
    SiloHolderPosition,
    SiloMarketHealth,
    SiloV2Adapter,
)
from core.config import (
    ConsumerMarketsConfig,
    MarketsConfig,
    canonical_address,
    load_consumer_markets_config,
    load_markets_config,
)


class FixtureSiloClient(SiloClient):
    def close(self) -> None:
        return

    def get_market_health(self, *, chain_code: str, market_ref: str) -> SiloMarketHealth:
        assert chain_code == "avalanche"
        if market_ref == "142":
            return SiloMarketHealth(
                total_supply_raw=None,
                total_borrow_raw=None,
                total_supply_usd=Decimal("2400000"),
                total_borrow_usd=Decimal("960000"),
                supply_apy=Decimal("0.055"),
                borrow_apy=Decimal("0.101"),
                utilization=Decimal("0.4"),
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
            block_number_or_slot="58650001",
            raw_payload={"market_id": 121},
        )

    def get_top_holders(
        self, *, chain_code: str, market_ref: str, limit: int
    ) -> list[SiloHolderPosition]:
        assert chain_code == "avalanche"
        assert limit == 50
        if market_ref == "142":
            return [
                SiloHolderPosition(
                    wallet_address="0x6CC60A0b57bc882A0471980D0e2D4aD7DDf3C4bD",  # Avant
                    supplied_raw=400_000 * 10**18,
                    borrowed_raw=120_000 * 10**6,
                ),
                SiloHolderPosition(
                    wallet_address="0x1111111111111111111111111111111111111111",
                    supplied_raw=250_000 * 10**18,
                    borrowed_raw=50_000 * 10**6,
                ),
            ]
        return [
            SiloHolderPosition(
                wallet_address="0x2222222222222222222222222222222222222222",
                supplied_raw=3 * 10**18,
                borrowed_raw=10_000_000,
            )
        ]


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


def test_silo_market_health_utilization_identity() -> None:
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
    epsilon = Decimal("0.0000000001")
    for snapshot in snapshots:
        expected = (
            Decimal("0")
            if snapshot.total_supply_usd == 0
            else snapshot.total_borrow_usd / snapshot.total_supply_usd
        )
        assert abs(snapshot.utilization - expected) <= epsilon
        assert Decimal("0") <= snapshot.utilization <= Decimal("1.5")


def test_silo_positions_are_normalized_and_filtered() -> None:
    markets_config: MarketsConfig = load_markets_config("config/markets.yaml")
    consumer_markets = load_consumer_markets_config("config/consumer_markets.yaml")
    adapter = SiloV2Adapter(
        markets_config=markets_config,
        consumer_markets_config=consumer_markets,
        client=FixtureSiloClient(),
        top_holders_limit=50,
    )

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(consumer_markets),
    )

    assert not issues
    assert positions

    # Avant strategy wallet is excluded from consumer top-holder output.
    assert all(
        canonical_address(position.wallet_address)
        != canonical_address("0x6CC60A0b57bc882A0471980D0e2D4aD7DDf3C4bD")
        for position in positions
    )

    for position in positions:
        assert position.protocol_code == "silo_v2"
        assert position.supplied_usd >= Decimal("0")
        assert position.borrowed_usd >= Decimal("0")
        assert position.equity_usd == position.supplied_usd - position.borrowed_usd
        assert Decimal("0") <= position.supply_apy <= Decimal("1")
        assert Decimal("0") <= position.borrow_apy <= Decimal("1")
