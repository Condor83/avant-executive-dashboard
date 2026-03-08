"""Served market view builder tests."""

from __future__ import annotations

from datetime import date, timedelta
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.market_exposures import build_market_exposure_descriptors
from analytics.market_views import MarketViewEngine
from analytics.yield_engine import denver_business_bounds_utc
from core.db.models import (
    Chain,
    Market,
    MarketExposure,
    MarketExposureDaily,
    MarketSnapshot,
    MarketSummaryDaily,
    PositionSnapshot,
    Protocol,
    Token,
    Wallet,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _token(*, symbol: str, chain_id: int, address: str) -> Token:
    return Token(
        chain_id=chain_id,
        address_or_mint=address,
        symbol=symbol,
        decimals=18,
    )


class _StubAvantYieldOracle:
    def __init__(self, mapping: dict[str, Decimal]) -> None:
        self.mapping = {key.upper(): value for key, value in mapping.items()}

    def get_token_apy(self, symbol: str) -> Decimal:
        return self.mapping[symbol.upper()]


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

        token = _token(
            symbol="USDC",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000000999",
        )
        session.add(token)
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="market",
            market_address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            base_asset_token_id=token.token_id,
            collateral_token_id=token.token_id,
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


def test_market_views_pair_reserve_usage_into_one_exposure(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 6)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=9)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x4444444444444444444444444444444444444444",
            wallet_type="strategy",
        )
        session.add_all([chain, protocol, wallet])
        session.flush()

        supply_token = _token(
            symbol="USDe",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000001000",
        )
        debt_token = _token(
            symbol="USDC",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000001001",
        )
        session.add_all([supply_token, debt_token])
        session.flush()

        supply_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="reserve",
            market_address="0x0000000000000000000000000000000000002000",
            display_name="USDe reserve",
            base_asset_token_id=supply_token.token_id,
            collateral_token_id=supply_token.token_id,
            metadata_json={"kind": "reserve"},
        )
        borrow_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="reserve",
            market_address="0x0000000000000000000000000000000000002001",
            display_name="USDC reserve",
            base_asset_token_id=debt_token.token_id,
            collateral_token_id=debt_token.token_id,
            metadata_json={"kind": "reserve"},
        )
        session.add_all([supply_market, borrow_market])
        session.flush()

        session.add_all(
            [
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=supply_market.market_id,
                    total_supply_usd=Decimal("1000"),
                    total_borrow_usd=Decimal("400"),
                    utilization=Decimal("0.4"),
                    supply_apy=Decimal("0.08"),
                    borrow_apy=Decimal("0.10"),
                    available_liquidity_usd=Decimal("600"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=borrow_market.market_id,
                    total_supply_usd=Decimal("2500"),
                    total_borrow_usd=Decimal("1500"),
                    utilization=Decimal("0.6"),
                    supply_apy=Decimal("0.02"),
                    borrow_apy=Decimal("0.11"),
                    available_liquidity_usd=Decimal("1000"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=supply_market.market_id,
                    position_key="aave_v3:ethereum:wallet:usde",
                    supplied_amount=Decimal("100"),
                    supplied_usd=Decimal("100"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.08"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("100"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=borrow_market.market_id,
                    position_key="aave_v3:ethereum:wallet:usdc",
                    supplied_amount=Decimal("0"),
                    supplied_usd=Decimal("0"),
                    borrowed_amount=Decimal("60"),
                    borrowed_usd=Decimal("60"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0.11"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("-60"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = MarketViewEngine(session, thresholds=None).compute_daily(
            business_date=business_date
        )
        session.commit()

        assert summary.exposure_rows_written == 1
        exposure_row = session.scalar(
            select(MarketExposureDaily).where(MarketExposureDaily.business_date == business_date)
        )
        assert exposure_row is not None
        assert exposure_row.total_supply_usd == Decimal("1000")
        assert exposure_row.total_borrow_usd == Decimal("1500")
        assert exposure_row.weighted_supply_apy == Decimal("0.08")
        assert exposure_row.weighted_borrow_apy == Decimal("0.11")
        assert exposure_row.available_liquidity_usd == Decimal("1000")
        assert exposure_row.utilization == Decimal("0.6")
        assert exposure_row.strategy_position_count == 1
        assert exposure_row.customer_position_count == 0
        assert exposure_row.scope_segment == "strategy_only"

        exposure = session.get(MarketExposure, exposure_row.market_exposure_id)
        assert exposure is not None
        assert exposure.display_name == "USDe / USDC"

        summary_row = session.scalar(
            select(MarketSummaryDaily).where(MarketSummaryDaily.business_date == business_date)
        )
        assert summary_row is not None
        assert summary_row.total_supply_usd == Decimal("3500")
        assert summary_row.total_borrow_usd == Decimal("1900")
        assert summary_row.weighted_utilization == Decimal("0.542857142857142857")


def test_market_views_map_monitored_consumer_market_to_native_components(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 6)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=9)

    with Session(engine) as session:
        chain = Chain(chain_code="avalanche")
        protocol = Protocol(protocol_code="euler_v2")
        wallet = Wallet(
            address="0x5555555555555555555555555555555555555555",
            wallet_type="strategy",
        )
        session.add_all([chain, protocol, wallet])
        session.flush()

        supply_token = _token(
            symbol="savUSD",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000003000",
        )
        debt_token = _token(
            symbol="USDC",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000003001",
        )
        session.add_all([supply_token, debt_token])
        session.flush()

        supply_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="market",
            market_address="0x0000000000000000000000000000000000004000",
            display_name="savUSD vault",
            base_asset_token_id=supply_token.token_id,
            collateral_token_id=supply_token.token_id,
            metadata_json={"kind": "market"},
        )
        borrow_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="market",
            market_address="0x0000000000000000000000000000000000004001",
            display_name="USDC vault",
            base_asset_token_id=debt_token.token_id,
            collateral_token_id=debt_token.token_id,
            metadata_json={"kind": "market"},
        )
        monitored_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="consumer_market",
            market_address=f"{supply_market.market_address}/{borrow_market.market_address}",
            display_name="savUSD / USDC",
            base_asset_token_id=debt_token.token_id,
            collateral_token_id=supply_token.token_id,
            metadata_json={"kind": "consumer_market"},
        )
        session.add_all([supply_market, borrow_market, monitored_market])
        session.flush()

        session.add_all(
            [
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=supply_market.market_id,
                    total_supply_usd=Decimal("900"),
                    total_borrow_usd=Decimal("250"),
                    utilization=Decimal("0.2777777778"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0.09"),
                    available_liquidity_usd=Decimal("650"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=borrow_market.market_id,
                    total_supply_usd=Decimal("1500"),
                    total_borrow_usd=Decimal("700"),
                    utilization=Decimal("0.4666666667"),
                    supply_apy=Decimal("0.03"),
                    borrow_apy=Decimal("0.08"),
                    available_liquidity_usd=Decimal("800"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=supply_market.market_id,
                    position_key="euler_v2:avalanche:wallet:seed",
                    supplied_amount=Decimal("0"),
                    supplied_usd=Decimal("0"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    reward_apy=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    equity_usd=Decimal("0"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = MarketViewEngine(
            session,
            thresholds=None,
            avant_yield_oracle=_StubAvantYieldOracle({"savUSD": Decimal("0.0745")}),
        ).compute_daily(business_date=business_date)
        session.commit()

        assert summary.exposure_rows_written == 1
        exposure_row = session.scalar(
            select(MarketExposureDaily).where(MarketExposureDaily.business_date == business_date)
        )
        assert exposure_row is not None
        assert exposure_row.total_supply_usd == Decimal("900")
        assert exposure_row.total_borrow_usd == Decimal("700")
        assert exposure_row.weighted_supply_apy == Decimal("0.0745")
        assert exposure_row.weighted_borrow_apy == Decimal("0.08")
        assert exposure_row.utilization == Decimal("0.4666666667")
        assert exposure_row.customer_position_count == 1
        assert exposure_row.strategy_position_count == 0
        assert exposure_row.scope_segment == "customer_only"

        summary_row = session.scalar(
            select(MarketSummaryDaily).where(MarketSummaryDaily.business_date == business_date)
        )
        assert summary_row is not None
        assert summary_row.total_supply_usd == Decimal("2400")
        assert summary_row.total_borrow_usd == Decimal("950")
        assert summary_row.weighted_utilization == Decimal("0.395833333333333333")


def test_market_exposure_descriptors_keep_direct_monitored_consumer_markets(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="morpho")
        session.add_all([chain, protocol])
        session.flush()

        loan_token = _token(
            symbol="USDC",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000005000",
        )
        collateral_token = _token(
            symbol="savUSD",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000005001",
        )
        session.add_all([loan_token, collateral_token])
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="consumer_market",
            market_address="0x0000000000000000000000000000000000006000",
            display_name="savUSD / USDC",
            base_asset_token_id=loan_token.token_id,
            collateral_token_id=collateral_token.token_id,
            metadata_json={"kind": "consumer_market"},
        )
        session.add(market)
        session.commit()

        descriptors = build_market_exposure_descriptors(session)

        assert len(descriptors) == 1
        descriptor = descriptors[0]
        assert descriptor.display_name == "savUSD / USDC"
        assert descriptor.monitored is True
        assert descriptor.component_roles == ((market.market_id, "primary_market"),)


def test_market_summary_dedupes_shared_borrow_reserve_components(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 6)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    as_of_ts_utc = sod_ts_utc + timedelta(hours=9)

    with Session(engine) as session:
        chain = Chain(chain_code="plasma")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x6666666666666666666666666666666666666666",
            wallet_type="strategy",
        )
        session.add_all([chain, protocol, wallet])
        session.flush()

        usde = _token(
            symbol="USDe",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000007000",
        )
        susde = _token(
            symbol="sUSDe",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000007001",
        )
        usdt0 = _token(
            symbol="USDT0",
            chain_id=chain.chain_id,
            address="0x0000000000000000000000000000000000007002",
        )
        session.add_all([usde, susde, usdt0])
        session.flush()

        usde_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="reserve",
            market_address="0x0000000000000000000000000000000000007100",
            display_name="USDe reserve",
            base_asset_token_id=usde.token_id,
            collateral_token_id=usde.token_id,
            metadata_json={"kind": "reserve"},
        )
        susde_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="reserve",
            market_address="0x0000000000000000000000000000000000007101",
            display_name="sUSDe reserve",
            base_asset_token_id=susde.token_id,
            collateral_token_id=susde.token_id,
            metadata_json={"kind": "reserve"},
        )
        usdt0_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_kind="reserve",
            market_address="0x0000000000000000000000000000000000007102",
            display_name="USDT0 reserve",
            base_asset_token_id=usdt0.token_id,
            collateral_token_id=usdt0.token_id,
            metadata_json={"kind": "reserve"},
        )
        session.add_all([usde_market, susde_market, usdt0_market])
        session.flush()

        session.add_all(
            [
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=usde_market.market_id,
                    total_supply_usd=Decimal("600"),
                    total_borrow_usd=Decimal("100"),
                    utilization=Decimal("0.1666666667"),
                    supply_apy=Decimal("0.02"),
                    borrow_apy=Decimal("0.03"),
                    available_liquidity_usd=Decimal("500"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=susde_market.market_id,
                    total_supply_usd=Decimal("300"),
                    total_borrow_usd=Decimal("0"),
                    utilization=Decimal("0"),
                    supply_apy=Decimal("0.04"),
                    borrow_apy=Decimal("0.05"),
                    available_liquidity_usd=Decimal("300"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                MarketSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    market_id=usdt0_market.market_id,
                    total_supply_usd=Decimal("1500"),
                    total_borrow_usd=Decimal("1200"),
                    utilization=Decimal("0.8"),
                    supply_apy=Decimal("0.01"),
                    borrow_apy=Decimal("0.09"),
                    available_liquidity_usd=Decimal("300"),
                    max_ltv=None,
                    liquidation_threshold=None,
                    liquidation_penalty=None,
                    caps_json=None,
                    irm_params_json=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=usde_market.market_id,
                    position_key="aave_v3:plasma:wallet:usde",
                    supplied_amount=Decimal("10"),
                    supplied_usd=Decimal("10"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.02"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("10"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=susde_market.market_id,
                    position_key="aave_v3:plasma:wallet:susde",
                    supplied_amount=Decimal("5"),
                    supplied_usd=Decimal("5"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0.04"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("5"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    block_number_or_slot="1",
                    wallet_id=wallet.wallet_id,
                    market_id=usdt0_market.market_id,
                    position_key="aave_v3:plasma:wallet:usdt0",
                    supplied_amount=Decimal("0"),
                    supplied_usd=Decimal("0"),
                    borrowed_amount=Decimal("6"),
                    borrowed_usd=Decimal("6"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0.09"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("-6"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        summary = MarketViewEngine(session, thresholds=None).compute_daily(
            business_date=business_date
        )
        session.commit()

        assert summary.exposure_rows_written == 2
        exposure_rows = session.scalars(
            select(MarketExposureDaily)
            .where(MarketExposureDaily.business_date == business_date)
            .order_by(MarketExposureDaily.market_exposure_id.asc())
        ).all()
        assert len(exposure_rows) == 2
        assert all(row.utilization == Decimal("0.8") for row in exposure_rows)

        summary_row = session.scalar(
            select(MarketSummaryDaily).where(MarketSummaryDaily.business_date == business_date)
        )
        assert summary_row is not None
        assert summary_row.total_supply_usd == Decimal("2400")
        assert summary_row.total_borrow_usd == Decimal("1300")
        assert summary_row.weighted_utilization == Decimal("0.541666666666666667")
        assert summary_row.total_available_liquidity_usd == Decimal("1100")
