"""Portfolio served view tests."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.portfolio_views import PortfolioViewEngine
from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import (
    Chain,
    Market,
    PortfolioPositionCurrent,
    PortfolioPositionDaily,
    PositionSnapshot,
    Product,
    Protocol,
    Token,
    Wallet,
    WalletProductMap,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_portfolio_views_use_collateral_leg_for_morpho_positions(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 5)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=6)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="morpho")
        wallet = Wallet(
            address="0x7777777777777777777777777777777777777777",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, protocol, wallet, product])
        session.flush()

        collateral_token = Token(
            chain_id=chain.chain_id,
            address_or_mint="0x9d39a5de30e57443bff2a8307a4256c8797a3497",
            symbol="sUSDe",
            decimals=18,
        )
        base_token = Token(
            chain_id=chain.chain_id,
            address_or_mint="0x6c3ea9036406852006290770bedfcaba0e23a0e8",
            symbol="PYUSD",
            decimals=6,
        )
        session.add_all([collateral_token, base_token])
        session.flush()
        collateral_token_id = collateral_token.token_id
        base_token_id = base_token.token_id

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0x90ef0c5a0dc7c4de4ad4585002d44e9d411d212d2f6258e94948beecf8b4c0d5",
            market_kind="market",
            display_name="sUSDe / PYUSD",
            base_asset_token_id=base_token_id,
            collateral_token_id=collateral_token_id,
            metadata_json={"kind": "market", "loan_token": "PYUSD", "collateral_token": "sUSDe"},
        )
        session.add(market)
        session.flush()

        session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))
        session.add(
            PositionSnapshot(
                as_of_ts_utc=as_of_ts_utc,
                block_number_or_slot="1",
                wallet_id=wallet.wallet_id,
                market_id=market.market_id,
                position_key="morpho-collateral-pos",
                supplied_amount=Decimal("0"),
                supplied_usd=Decimal("0"),
                collateral_amount=Decimal("100"),
                collateral_usd=Decimal("100"),
                borrowed_amount=Decimal("50"),
                borrowed_usd=Decimal("50"),
                supply_apy=Decimal("0.04"),
                borrow_apy=Decimal("0.03"),
                reward_apy=Decimal("0"),
                equity_usd=Decimal("50"),
                health_factor=None,
                ltv=Decimal("0.5"),
                source="rpc",
            )
        )
        session.commit()

    with Session(engine) as session:
        summary = PortfolioViewEngine(session).compute_daily(
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
        )
        session.commit()

        assert summary.current_rows_written == 1
        assert summary.daily_rows_written == 1

        current_row = session.scalar(select(PortfolioPositionCurrent))
        assert current_row is not None
        assert current_row.supply_token_id == collateral_token_id
        assert current_row.supply_amount == Decimal("100")
        assert current_row.supply_usd == Decimal("100")
        assert current_row.borrow_amount == Decimal("50")
        assert current_row.borrow_usd == Decimal("50")
        assert current_row.net_equity_usd == Decimal("50")
        assert current_row.leverage_ratio == Decimal("2")

        daily_row = session.scalar(select(PortfolioPositionDaily))
        assert daily_row is not None
        assert daily_row.supply_usd == Decimal("100")
        assert daily_row.borrow_usd == Decimal("50")
