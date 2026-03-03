"""Kamino market snapshot schema and normalization tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.kamino.adapter import (
    KaminoAdapter,
    KaminoClient,
    KaminoMarketStats,
    KaminoObligationSnapshot,
)
from core.config import load_markets_config


class FixtureKaminoClient(KaminoClient):
    def close(self) -> None:
        return

    def get_slot(self, chain_code: str) -> int:
        assert chain_code == "solana"
        return 312_500_123

    def get_market_stats(self, chain_code: str, market_pubkey: str) -> KaminoMarketStats:
        assert chain_code == "solana"
        assert market_pubkey
        return KaminoMarketStats(
            total_supply_usd=Decimal("2500000"),
            total_borrow_usd=Decimal("1100000"),
            supply_apy=Decimal("0.061"),
            borrow_apy=Decimal("0.094"),
            utilization=Decimal("0.44"),
            available_liquidity_usd=Decimal("1400000"),
            slot="312500123",
            raw_payload={"source": "fixture"},
        )

    def get_user_obligations(
        self,
        *,
        chain_code: str,
        market_pubkey: str,
        wallet_address: str,
    ) -> list[KaminoObligationSnapshot]:
        assert chain_code == "solana"
        assert market_pubkey
        assert wallet_address
        return [
            KaminoObligationSnapshot(
                obligation_ref="obligation-1",
                supplied_usd=Decimal("750000"),
                borrowed_usd=Decimal("250000"),
                health_factor=Decimal("2.1"),
                ltv=Decimal("0.33"),
                block_number_or_slot="312500120",
            )
        ]


def test_kamino_market_snapshots_follow_canonical_schema() -> None:
    markets_config = load_markets_config("config/markets.yaml")
    adapter = KaminoAdapter(markets_config=markets_config, client=FixtureKaminoClient())

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert not issues
    assert snapshots

    for snapshot in snapshots:
        assert snapshot.protocol_code == "kamino"
        assert snapshot.chain_code == "solana"
        assert snapshot.market_ref
        assert snapshot.total_supply_usd >= Decimal("0")
        assert snapshot.total_borrow_usd >= Decimal("0")
        assert Decimal("0") <= snapshot.utilization <= Decimal("1.5")
        assert Decimal("0") <= snapshot.supply_apy <= Decimal("1")
        assert Decimal("0") <= snapshot.borrow_apy <= Decimal("1")
        assert snapshot.block_number_or_slot is not None


def test_kamino_wallet_positions_follow_canonical_schema() -> None:
    markets_config = load_markets_config("config/markets.yaml")
    adapter = KaminoAdapter(markets_config=markets_config, client=FixtureKaminoClient())

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token={},
    )

    assert not issues
    assert positions

    for position in positions:
        assert position.protocol_code == "kamino"
        assert position.chain_code == "solana"
        assert position.wallet_address
        assert position.supplied_usd >= Decimal("0")
        assert position.borrowed_usd >= Decimal("0")
        assert position.equity_usd == position.supplied_usd - position.borrowed_usd
        assert Decimal("0") <= position.supply_apy <= Decimal("1")
        assert Decimal("0") <= position.borrow_apy <= Decimal("1")
