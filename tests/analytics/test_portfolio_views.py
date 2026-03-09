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
    PortfolioSummaryDaily,
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


def test_portfolio_views_clamp_negative_day_fees_and_sum_mtd_from_daily_rows(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 5)
    prior_date = date(2026, 3, 4)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=6)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x8888888888888888888888888888888888888888",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, protocol, wallet, product])
        session.flush()

        supply_token = Token(
            chain_id=chain.chain_id,
            address_or_mint="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            symbol="USDC",
            decimals=6,
        )
        session.add(supply_token)
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xfee-waterfall-market",
            market_kind="reserve",
            display_name="USDC Reserve",
            base_asset_token_id=supply_token.token_id,
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
                position_key="fee-clamp-pos",
                supplied_amount=Decimal("1_000"),
                supplied_usd=Decimal("1_000"),
                collateral_amount=None,
                collateral_usd=None,
                borrowed_amount=Decimal("0"),
                borrowed_usd=Decimal("0"),
                supply_apy=Decimal("0"),
                borrow_apy=Decimal("0"),
                reward_apy=Decimal("0"),
                equity_usd=Decimal("1_000"),
                health_factor=None,
                ltv=None,
                source="rpc",
            )
        )
        session.add_all(
            [
                YieldDaily(
                    business_date=prior_date,
                    wallet_id=wallet.wallet_id,
                    product_id=product.product_id,
                    protocol_id=protocol.protocol_id,
                    market_id=market.market_id,
                    row_key="position:fee-clamp-pos:prior",
                    position_key="fee-clamp-pos",
                    gross_yield_usd=Decimal("1_000"),
                    strategy_fee_usd=Decimal("150"),
                    avant_gop_usd=Decimal("85"),
                    net_yield_usd=Decimal("765"),
                    avg_equity_usd=Decimal("1_000"),
                    gross_roe=Decimal("1"),
                    post_strategy_fee_roe=Decimal("0.85"),
                    net_roe=Decimal("0.765"),
                    avant_gop_roe=Decimal("0.085"),
                    method="apy_prorated_sod_eod",
                    confidence_score=Decimal("1"),
                ),
                YieldDaily(
                    business_date=business_date,
                    wallet_id=wallet.wallet_id,
                    product_id=product.product_id,
                    protocol_id=protocol.protocol_id,
                    market_id=market.market_id,
                    row_key="position:fee-clamp-pos:current",
                    position_key="fee-clamp-pos",
                    gross_yield_usd=Decimal("-100"),
                    strategy_fee_usd=Decimal("0"),
                    avant_gop_usd=Decimal("0"),
                    net_yield_usd=Decimal("-100"),
                    avg_equity_usd=Decimal("1_000"),
                    gross_roe=Decimal("-0.1"),
                    post_strategy_fee_roe=Decimal("-0.1"),
                    net_roe=Decimal("-0.1"),
                    avant_gop_roe=Decimal("0"),
                    method="apy_prorated_sod_eod",
                    confidence_score=Decimal("1"),
                ),
            ]
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
        assert summary.summary_rows_written == 1

        current_row = session.scalar(select(PortfolioPositionCurrent))
        assert current_row is not None
        assert current_row.gross_yield_daily_usd == Decimal("-100")
        assert current_row.net_yield_daily_usd == Decimal("-100")
        assert current_row.strategy_fee_daily_usd == Decimal("0")
        assert current_row.avant_gop_daily_usd == Decimal("0")
        assert current_row.gross_yield_mtd_usd == Decimal("900")
        assert current_row.net_yield_mtd_usd == Decimal("665")
        assert current_row.strategy_fee_mtd_usd == Decimal("150")
        assert current_row.avant_gop_mtd_usd == Decimal("85")

        daily_row = session.scalar(select(PortfolioPositionDaily))
        assert daily_row is not None
        assert daily_row.gross_yield_usd == Decimal("-100")
        assert daily_row.net_yield_usd == Decimal("-100")
        assert daily_row.strategy_fee_usd == Decimal("0")
        assert daily_row.avant_gop_usd == Decimal("0")

        summary_row = session.scalar(select(PortfolioSummaryDaily))
        assert summary_row is not None
        assert summary_row.total_gross_yield_daily_usd == Decimal("-100")
        assert summary_row.total_net_yield_daily_usd == Decimal("-100")
        assert summary_row.total_strategy_fee_daily_usd == Decimal("0")
        assert summary_row.total_avant_gop_daily_usd == Decimal("0")


def test_portfolio_summary_applies_fee_waterfall_to_aggregated_daily_gross(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 5)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=6)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x9999999999999999999999999999999999999999",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, protocol, wallet, product])
        session.flush()

        supply_token = Token(
            chain_id=chain.chain_id,
            address_or_mint="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            symbol="USDC",
            decimals=6,
        )
        session.add(supply_token)
        session.flush()

        market_gain = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xrollup-gain",
            market_kind="reserve",
            display_name="USDC Gain",
            base_asset_token_id=supply_token.token_id,
        )
        market_loss = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xrollup-loss",
            market_kind="reserve",
            display_name="USDC Loss",
            base_asset_token_id=supply_token.token_id,
        )
        session.add_all([market_gain, market_loss])
        session.flush()

        session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))
        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=market_gain.market_id,
                    position_key="rollup-gain",
                    supplied_amount=Decimal("1_000"),
                    supplied_usd=Decimal("1_000"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("1_000"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="2",
                    wallet_id=wallet.wallet_id,
                    market_id=market_loss.market_id,
                    position_key="rollup-loss",
                    supplied_amount=Decimal("900"),
                    supplied_usd=Decimal("900"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("100"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                YieldDaily(
                    business_date=business_date,
                    wallet_id=wallet.wallet_id,
                    product_id=product.product_id,
                    protocol_id=protocol.protocol_id,
                    market_id=market_gain.market_id,
                    row_key="position:rollup-gain",
                    position_key="rollup-gain",
                    gross_yield_usd=Decimal("100"),
                    strategy_fee_usd=Decimal("15"),
                    avant_gop_usd=Decimal("8.5"),
                    net_yield_usd=Decimal("76.5"),
                    avg_equity_usd=Decimal("1_000"),
                    gross_roe=Decimal("0.1"),
                    post_strategy_fee_roe=Decimal("0.085"),
                    net_roe=Decimal("0.0765"),
                    avant_gop_roe=Decimal("0.0085"),
                    method="apy_prorated_sod_eod",
                    confidence_score=Decimal("1"),
                ),
                YieldDaily(
                    business_date=business_date,
                    wallet_id=wallet.wallet_id,
                    product_id=product.product_id,
                    protocol_id=protocol.protocol_id,
                    market_id=market_loss.market_id,
                    row_key="position:rollup-loss",
                    position_key="rollup-loss",
                    gross_yield_usd=Decimal("-90"),
                    strategy_fee_usd=Decimal("0"),
                    avant_gop_usd=Decimal("0"),
                    net_yield_usd=Decimal("-90"),
                    avg_equity_usd=Decimal("100"),
                    gross_roe=Decimal("-0.9"),
                    post_strategy_fee_roe=Decimal("-0.9"),
                    net_roe=Decimal("-0.9"),
                    avant_gop_roe=Decimal("0"),
                    method="apy_prorated_sod_eod",
                    confidence_score=Decimal("1"),
                ),
                YieldDaily(
                    business_date=business_date,
                    wallet_id=None,
                    product_id=None,
                    protocol_id=None,
                    market_id=None,
                    row_key="total",
                    position_key=None,
                    gross_yield_usd=Decimal("10"),
                    strategy_fee_usd=Decimal("1.5"),
                    avant_gop_usd=Decimal("0.85"),
                    net_yield_usd=Decimal("7.65"),
                    avg_equity_usd=Decimal("1_100"),
                    gross_roe=Decimal("0.0090909091"),
                    post_strategy_fee_roe=Decimal("0.0077272727"),
                    net_roe=Decimal("0.0069545455"),
                    avant_gop_roe=Decimal("0.0007727273"),
                    method="apy_prorated_sod_eod",
                    confidence_score=Decimal("1"),
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = PortfolioViewEngine(session).compute_daily(
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
        )
        session.commit()

        assert summary.summary_rows_written == 1

        summary_row = session.scalar(select(PortfolioSummaryDaily))
        assert summary_row is not None
        assert summary_row.total_gross_yield_daily_usd == Decimal("10")
        assert summary_row.total_strategy_fee_daily_usd == Decimal("1.5")
        assert summary_row.total_avant_gop_daily_usd == Decimal("0.85")
        assert summary_row.total_net_yield_daily_usd == Decimal("7.65")
        assert summary_row.avg_leverage_ratio == Decimal("1.7272727273")
