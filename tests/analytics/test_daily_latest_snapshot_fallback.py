"""Daily compute fallback tests for latest-snapshot development mode."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.yield_engine import PARTIAL_CONFIDENCE, YieldEngine
from core.db.models import (
    Chain,
    DataQuality,
    Market,
    PositionSnapshot,
    Product,
    Protocol,
    Wallet,
    WalletProductMap,
    YieldDaily,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_compute_daily_uses_latest_snapshot_when_policy_is_latest_snapshot(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 3)
    latest_snapshot_ts = datetime(2026, 3, 4, 15, 0, tzinfo=UTC)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x1111111111111111111111111111111111111111",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, protocol, wallet, product])
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "reserve"},
        )
        session.add(market)
        session.flush()
        session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))

        session.add(
            PositionSnapshot(
                as_of_ts_utc=latest_snapshot_ts,
                wallet_id=wallet.wallet_id,
                market_id=market.market_id,
                position_key="pos-latest",
                supplied_amount=Decimal("100"),
                supplied_usd=Decimal("100"),
                borrowed_amount=Decimal("20"),
                borrowed_usd=Decimal("20"),
                supply_apy=Decimal("0.10"),
                borrow_apy=Decimal("0.05"),
                reward_apy=Decimal("0.02"),
                equity_usd=Decimal("80"),
                source="rpc",
                block_number_or_slot="123",
            )
        )
        session.commit()

    with Session(engine) as session:
        summary = YieldEngine(session, boundary_policy="latest_snapshot").compute_daily(
            business_date=business_date
        )
        session.commit()

        assert summary.sod_ts_utc == latest_snapshot_ts
        assert summary.eod_ts_utc == latest_snapshot_ts
        assert summary.position_rows_written == 1

        position_row = session.execute(
            select(YieldDaily).where(
                YieldDaily.business_date == business_date,
                YieldDaily.position_key == "pos-latest",
            )
        ).scalar_one()
        assert position_row.confidence_score == PARTIAL_CONFIDENCE

        total_row = session.execute(
            select(YieldDaily).where(
                YieldDaily.business_date == business_date,
                YieldDaily.position_key.is_(None),
                YieldDaily.wallet_id.is_(None),
                YieldDaily.product_id.is_(None),
                YieldDaily.protocol_id.is_(None),
            )
        ).scalar_one()
        expected_gross = Decimal("11") / Decimal("365")
        assert abs(total_row.gross_yield_usd - expected_gross) <= Decimal("0.000000000001")
        assert total_row.avg_equity_usd == Decimal("80")

        latest_fallback_issues = session.scalars(
            select(DataQuality).where(
                DataQuality.error_type == "daily_latest_snapshot_fallback_used"
            )
        ).all()
        assert len(latest_fallback_issues) == 1
