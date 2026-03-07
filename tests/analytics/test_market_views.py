"""Served market view builder tests."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.market_views import MarketViewEngine
from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import (
    Chain,
    Market,
    MarketExposureDaily,
    MarketSnapshot,
    MarketSummaryDaily,
    PositionSnapshot,
    Protocol,
    Wallet,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_market_views_persist_large_utilization_ratios(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 4)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=9)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="morpho")
        wallet = Wallet(
            address="0x3333333333333333333333333333333333333333",
            wallet_type="strategy",
        )
        session.add_all([chain, protocol, wallet])
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "market"},
        )
        session.add(market)
        session.flush()
        market_id = market.market_id

        session.add(
            MarketSnapshot(
                as_of_ts_utc=as_of_ts_utc,
                block_number_or_slot="1",
                market_id=market_id,
                total_supply_usd=Decimal("0.000000000001"),
                total_borrow_usd=Decimal("100"),
                utilization=Decimal("0.5"),
                supply_apy=Decimal("0.04"),
                borrow_apy=Decimal("0.06"),
                available_liquidity_usd=Decimal("0"),
                max_ltv=None,
                liquidation_threshold=None,
                liquidation_penalty=None,
                caps_json=None,
                irm_params_json=None,
                source="rpc",
            )
        )
        session.add(
            PositionSnapshot(
                as_of_ts_utc=as_of_ts_utc,
                block_number_or_slot="1",
                wallet_id=wallet.wallet_id,
                market_id=market_id,
                position_key="pos-large-utilization",
                supplied_amount=Decimal("100"),
                supplied_usd=Decimal("100"),
                borrowed_amount=Decimal("50"),
                borrowed_usd=Decimal("50"),
                supply_apy=Decimal("0.04"),
                borrow_apy=Decimal("0.06"),
                reward_apy=Decimal("0"),
                equity_usd=Decimal("50"),
                health_factor=None,
                ltv=None,
                source="rpc",
            )
        )
        session.commit()

    with Session(engine) as session:
        summary = MarketViewEngine(session, thresholds=None).compute_daily(
            business_date=business_date
        )
        session.commit()

        assert summary.exposure_rows_written == 1
        assert summary.summary_rows_written == 1

        exposure_row = session.scalar(
            select(MarketExposureDaily).where(MarketExposureDaily.business_date == business_date)
        )
        assert exposure_row is not None
        assert exposure_row.utilization == Decimal("100000000000000")

        summary_row = session.scalar(
            select(MarketSummaryDaily).where(MarketSummaryDaily.business_date == business_date)
        )
        assert summary_row is not None
        assert summary_row.weighted_utilization == Decimal("100000000000000")
