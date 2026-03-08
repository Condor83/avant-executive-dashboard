"""Executive summary served view tests."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.executive_summary import ExecutiveSummaryEngine
from analytics.portfolio_views import PortfolioViewEngine
from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import (
    Chain,
    ExecutiveSummaryDaily,
    Market,
    PositionSnapshot,
    Product,
    Protocol,
    Token,
    Wallet,
    WalletProductMap,
    YieldDaily,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_executive_summary_tracks_market_stability_ops_separately(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 5)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=6)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        strategy_protocol = Protocol(protocol_code="aave_v3")
        ops_protocol = Protocol(protocol_code="traderjoe_lp")
        wallet = Wallet(
            address="0x9999999999999999999999999999999999999999",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, strategy_protocol, ops_protocol, wallet, product])
        session.flush()

        token_usdc = Token(
            chain_id=chain.chain_id,
            address_or_mint="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            symbol="USDC",
            decimals=6,
        )
        token_wbtc = Token(
            chain_id=chain.chain_id,
            address_or_mint="0x2260fac5e5542a773aa44fbcfedf7c193bc2c599",
            symbol="WBTC",
            decimals=8,
        )
        session.add_all([token_usdc, token_wbtc])
        session.flush()

        session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))

        strategy_market = Market(
            chain_id=chain.chain_id,
            protocol_id=strategy_protocol.protocol_id,
            native_market_key="aave-usdc",
            market_address="0xstrategy",
            market_kind="reserve",
            display_name="USDC Reserve",
            base_asset_token_id=token_usdc.token_id,
            metadata_json={"kind": "reserve"},
        )
        ops_market = Market(
            chain_id=chain.chain_id,
            protocol_id=ops_protocol.protocol_id,
            native_market_key="ops-buy-wall",
            market_address="0xops",
            market_kind="liquidity_book_pool",
            display_name="USDC / WBTC Pool",
            base_asset_token_id=token_wbtc.token_id,
            collateral_token_id=token_usdc.token_id,
            metadata_json={
                "kind": "liquidity_book_pool",
                "capital_bucket": "market_stability_ops",
                "include_in_yield": False,
                "exposure_class": "ops_buy_wall",
            },
        )
        session.add_all([strategy_market, ops_market])
        session.flush()

        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=strategy_market.market_id,
                    position_key="strategy-pos",
                    supplied_amount=Decimal("1_000"),
                    supplied_usd=Decimal("1_000"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("200"),
                    borrowed_usd=Decimal("200"),
                    supply_apy=Decimal("0.05"),
                    borrow_apy=Decimal("0.03"),
                    reward_apy=Decimal("0.01"),
                    equity_usd=Decimal("800"),
                    health_factor=Decimal("1.5"),
                    ltv=Decimal("0.5"),
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=ops_market.market_id,
                    position_key="ops-pos",
                    supplied_amount=Decimal("600"),
                    supplied_usd=Decimal("600"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("600"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                YieldDaily(
                    business_date=business_date,
                    wallet_id=wallet.wallet_id,
                    product_id=product.product_id,
                    protocol_id=strategy_protocol.protocol_id,
                    market_id=strategy_market.market_id,
                    row_key="position:strategy-pos",
                    position_key="strategy-pos",
                    gross_yield_usd=Decimal("20"),
                    strategy_fee_usd=Decimal("3"),
                    avant_gop_usd=Decimal("1.7"),
                    net_yield_usd=Decimal("15.3"),
                    avg_equity_usd=Decimal("800"),
                    gross_roe=Decimal("0.025"),
                    post_strategy_fee_roe=Decimal("0.02125"),
                    net_roe=Decimal("0.019125"),
                    avant_gop_roe=Decimal("0.002125"),
                    method="apy_prorated_sod_eod",
                    confidence_score=Decimal("1"),
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        PortfolioViewEngine(session).compute_daily(
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
        )
        ExecutiveSummaryEngine(session).compute_daily(business_date=business_date)
        session.commit()

        row = session.scalar(select(ExecutiveSummaryDaily))
        assert row is not None
        assert row.nav_usd == Decimal("800")
        assert row.portfolio_net_equity_usd == Decimal("800")
        assert row.market_stability_ops_net_equity_usd == Decimal("600")
        assert row.total_gross_yield_daily_usd == Decimal("20")
        assert row.total_net_yield_daily_usd == Decimal("15.3")


def test_executive_summary_persists_ops_only_capital(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 5)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=6)

    with Session(engine) as session:
        chain = Chain(chain_code="avalanche")
        ops_protocol = Protocol(protocol_code="traderjoe_lp")
        wallet = Wallet(
            address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            wallet_type="strategy",
        )
        session.add_all([chain, ops_protocol, wallet])
        session.flush()

        token_usdc = Token(
            chain_id=chain.chain_id,
            address_or_mint="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
            symbol="USDC",
            decimals=6,
        )
        token_wavax = Token(
            chain_id=chain.chain_id,
            address_or_mint="0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7",
            symbol="WAVAX",
            decimals=18,
        )
        session.add_all([token_usdc, token_wavax])
        session.flush()

        ops_market = Market(
            chain_id=chain.chain_id,
            protocol_id=ops_protocol.protocol_id,
            native_market_key="ops-only",
            market_address="0xops-only",
            market_kind="liquidity_book_pool",
            display_name="USDC / WAVAX Pool",
            base_asset_token_id=token_wavax.token_id,
            collateral_token_id=token_usdc.token_id,
            metadata_json={
                "kind": "liquidity_book_pool",
                "capital_bucket": "market_stability_ops",
                "include_in_yield": False,
                "exposure_class": "ops_buy_wall",
            },
        )
        session.add(ops_market)
        session.flush()

        session.add(
            PositionSnapshot(
                as_of_ts_utc=as_of_ts_utc,
                block_number_or_slot="1",
                wallet_id=wallet.wallet_id,
                market_id=ops_market.market_id,
                position_key="ops-only-pos",
                supplied_amount=Decimal("500"),
                supplied_usd=Decimal("500"),
                collateral_amount=None,
                collateral_usd=None,
                borrowed_amount=Decimal("0"),
                borrowed_usd=Decimal("0"),
                supply_apy=Decimal("0"),
                borrow_apy=Decimal("0"),
                reward_apy=Decimal("0"),
                equity_usd=Decimal("500"),
                health_factor=None,
                ltv=None,
                source="rpc",
            )
        )
        session.commit()

    with Session(engine) as session:
        PortfolioViewEngine(session).compute_daily(
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
        )
        summary = ExecutiveSummaryEngine(session).compute_daily(business_date=business_date)
        session.commit()

        assert summary.rows_written == 1
        row = session.scalar(select(ExecutiveSummaryDaily))
        assert row is not None
        assert row.nav_usd == Decimal("0")
        assert row.portfolio_net_equity_usd == Decimal("0")
        assert row.market_stability_ops_net_equity_usd == Decimal("500")
