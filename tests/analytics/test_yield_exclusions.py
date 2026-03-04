"""Yield engine exclusion tests for non-deployed capital buckets."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.yield_engine import YieldEngine, denver_business_bounds_utc
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


def test_compute_daily_excludes_positions_with_include_in_yield_false(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 3)
    sod_ts, eod_ts = denver_business_bounds_utc(business_date)

    with Session(engine) as session:
        chain = Chain(chain_code="avalanche")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x1111111111111111111111111111111111111111",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, protocol, wallet, product])
        session.flush()

        included_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "reserve"},
        )
        excluded_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={
                "kind": "wallet_balance_token",
                "include_in_yield": False,
                "capital_bucket": "pending_deployment",
            },
        )
        session.add_all([included_market, excluded_market])
        session.flush()

        session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))

        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=sod_ts,
                    wallet_id=wallet.wallet_id,
                    market_id=included_market.market_id,
                    position_key="included",
                    supplied_amount=Decimal("100"),
                    supplied_usd=Decimal("100"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.10"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("100"),
                    source="rpc",
                    block_number_or_slot="1",
                ),
                PositionSnapshot(
                    as_of_ts_utc=eod_ts,
                    wallet_id=wallet.wallet_id,
                    market_id=included_market.market_id,
                    position_key="included",
                    supplied_amount=Decimal("100"),
                    supplied_usd=Decimal("100"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.10"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("100"),
                    source="rpc",
                    block_number_or_slot="2",
                ),
                PositionSnapshot(
                    as_of_ts_utc=sod_ts,
                    wallet_id=wallet.wallet_id,
                    market_id=excluded_market.market_id,
                    position_key="excluded",
                    supplied_amount=Decimal("1000"),
                    supplied_usd=Decimal("1000"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.20"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("1000"),
                    source="rpc",
                    block_number_or_slot="1",
                ),
                PositionSnapshot(
                    as_of_ts_utc=eod_ts,
                    wallet_id=wallet.wallet_id,
                    market_id=excluded_market.market_id,
                    position_key="excluded",
                    supplied_amount=Decimal("1000"),
                    supplied_usd=Decimal("1000"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.20"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("1000"),
                    source="rpc",
                    block_number_or_slot="2",
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = YieldEngine(session).compute_daily(business_date=business_date)
        session.commit()

        assert summary.position_rows_written == 1

        position_rows = session.scalars(
            select(YieldDaily).where(
                YieldDaily.business_date == business_date,
                YieldDaily.position_key.is_not(None),
            )
        ).all()
        assert [row.position_key for row in position_rows] == ["included"]

        exclusion_issues = session.scalars(
            select(DataQuality).where(
                DataQuality.error_type == "position_excluded_from_yield",
            )
        ).all()
        assert len(exclusion_issues) == 1
        issue = exclusion_issues[0]
        assert issue.payload_json == {
            "business_date": business_date.isoformat(),
            "capital_bucket": "pending_deployment",
            "excluded_positions": 1,
        }
