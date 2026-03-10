"""Test fixtures for served API integration tests."""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from analytics.executive_summary import ExecutiveSummaryEngine
from analytics.market_views import MarketViewEngine
from analytics.portfolio_views import PortfolioViewEngine
from analytics.yield_engine import YieldEngine, denver_business_bounds_utc
from api.app import create_app
from api.deps import get_session
from core.db.models import (
    Alert,
    Chain,
    DataQuality,
    HolderScorecardDaily,
    HolderSupplyCoverageDaily,
    Market,
    MarketSnapshot,
    PositionSnapshot,
    Product,
    Protocol,
    Token,
    Wallet,
    WalletProductMap,
)

BUSINESS_DATE = date(2026, 3, 3)


@dataclass
class SeedMetadata:
    business_date: date


@pytest.fixture()
def seeded_session(
    postgres_database_url: str,
) -> Generator[tuple[Session, SeedMetadata], None, None]:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    session = Session(engine)

    sod_ts, eod_ts = denver_business_bounds_utc(BUSINESS_DATE)

    chain_eth = Chain(chain_code="ethereum")
    chain_arb = Chain(chain_code="arbitrum")
    proto_aave = Protocol(protocol_code="aave_v3")
    proto_morpho = Protocol(protocol_code="morpho")
    proto_traderjoe = Protocol(protocol_code="traderjoe_lp")
    product_senior = Product(product_code="stablecoin_senior")
    product_junior = Product(product_code="stablecoin_junior")
    product_btc = Product(product_code="btc_senior")
    w1 = Wallet(address="0x1111111111111111111111111111111111111111", wallet_type="strategy")
    w2 = Wallet(address="0x2222222222222222222222222222222222222222", wallet_type="strategy")
    w3 = Wallet(address="0x3333333333333333333333333333333333333333", wallet_type="strategy")
    session.add_all(
        [
            chain_eth,
            chain_arb,
            proto_aave,
            proto_morpho,
            proto_traderjoe,
            product_senior,
            product_junior,
            product_btc,
            w1,
            w2,
            w3,
        ]
    )
    session.flush()

    token_usdc = Token(
        chain_id=chain_eth.chain_id,
        address_or_mint="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
        symbol="USDC",
        decimals=6,
    )
    token_wbtc = Token(
        chain_id=chain_eth.chain_id,
        address_or_mint="0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
        symbol="WBTC",
        decimals=8,
    )
    session.add_all([token_usdc, token_wbtc])
    session.flush()

    markets = [
        Market(
            chain_id=chain_eth.chain_id,
            protocol_id=proto_aave.protocol_id,
            native_market_key="aave-usdc",
            market_address="0xaaa1",
            market_kind="reserve",
            display_name="USDC Reserve",
            base_asset_token_id=token_usdc.token_id,
        ),
        Market(
            chain_id=chain_eth.chain_id,
            protocol_id=proto_aave.protocol_id,
            native_market_key="aave-wbtc",
            market_address="0xaaa2",
            market_kind="reserve",
            display_name="WBTC Reserve",
            base_asset_token_id=token_wbtc.token_id,
        ),
        Market(
            chain_id=chain_arb.chain_id,
            protocol_id=proto_morpho.protocol_id,
            native_market_key="morpho-usdc",
            market_address="0xbbb1",
            market_kind="market",
            display_name="USDC / USDC",
            base_asset_token_id=token_usdc.token_id,
            collateral_token_id=token_usdc.token_id,
        ),
        Market(
            chain_id=chain_arb.chain_id,
            protocol_id=proto_morpho.protocol_id,
            native_market_key="morpho-wbtc",
            market_address="0xbbb2",
            market_kind="market",
            display_name="WBTC / WBTC",
            base_asset_token_id=token_wbtc.token_id,
            collateral_token_id=token_wbtc.token_id,
        ),
    ]
    session.add_all(markets)
    session.flush()

    session.add_all(
        [
            WalletProductMap(wallet_id=w1.wallet_id, product_id=product_senior.product_id),
            WalletProductMap(wallet_id=w2.wallet_id, product_id=product_junior.product_id),
            WalletProductMap(wallet_id=w3.wallet_id, product_id=product_btc.product_id),
        ]
    )

    position_snapshots = [
        dict(
            wallet_id=w1.wallet_id,
            market_id=markets[0].market_id,
            position_key="pos-w1-m1",
            supplied_amount=Decimal("1000"),
            supplied_usd=Decimal("1000"),
            borrowed_amount=Decimal("200"),
            borrowed_usd=Decimal("200"),
            supply_apy=Decimal("0.05"),
            borrow_apy=Decimal("0.03"),
            reward_apy=Decimal("0.01"),
            equity_usd=Decimal("800"),
        ),
        dict(
            wallet_id=w1.wallet_id,
            market_id=markets[1].market_id,
            position_key="pos-w1-m2",
            supplied_amount=Decimal("500"),
            supplied_usd=Decimal("500"),
            borrowed_amount=Decimal("100"),
            borrowed_usd=Decimal("100"),
            supply_apy=Decimal("0.04"),
            borrow_apy=Decimal("0.02"),
            reward_apy=Decimal("0.005"),
            equity_usd=Decimal("400"),
        ),
        dict(
            wallet_id=w2.wallet_id,
            market_id=markets[2].market_id,
            position_key="pos-w2-m3",
            supplied_amount=Decimal("2000"),
            supplied_usd=Decimal("2000"),
            borrowed_amount=Decimal("500"),
            borrowed_usd=Decimal("500"),
            supply_apy=Decimal("0.06"),
            borrow_apy=Decimal("0.04"),
            reward_apy=Decimal("0.02"),
            equity_usd=Decimal("1500"),
        ),
        dict(
            wallet_id=w3.wallet_id,
            market_id=markets[3].market_id,
            position_key="pos-w3-m4",
            supplied_amount=Decimal("300"),
            supplied_usd=Decimal("300"),
            borrowed_amount=Decimal("50"),
            borrowed_usd=Decimal("50"),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.015"),
            reward_apy=Decimal("0.008"),
            equity_usd=Decimal("250"),
        ),
    ]
    for snapshot_data in position_snapshots:
        for ts, block in [(sod_ts, "100"), (eod_ts, "200")]:
            session.add(
                PositionSnapshot(
                    as_of_ts_utc=ts,
                    block_number_or_slot=block,
                    source="rpc",
                    health_factor=Decimal("1.5"),
                    ltv=Decimal("0.5"),
                    **snapshot_data,
                )
            )

    market_snapshots = [
        dict(
            market_id=markets[0].market_id,
            total_supply_usd=Decimal("10000"),
            total_borrow_usd=Decimal("5000"),
            utilization=Decimal("0.5"),
            supply_apy=Decimal("0.05"),
            borrow_apy=Decimal("0.03"),
            available_liquidity_usd=Decimal("5000"),
            max_ltv=Decimal("0.8"),
            liquidation_threshold=Decimal("0.85"),
            liquidation_penalty=Decimal("0.05"),
        ),
        dict(
            market_id=markets[1].market_id,
            total_supply_usd=Decimal("8000"),
            total_borrow_usd=Decimal("3000"),
            utilization=Decimal("0.375"),
            supply_apy=Decimal("0.04"),
            borrow_apy=Decimal("0.02"),
            available_liquidity_usd=Decimal("5000"),
            max_ltv=Decimal("0.7"),
            liquidation_threshold=Decimal("0.75"),
            liquidation_penalty=Decimal("0.10"),
        ),
        dict(
            market_id=markets[2].market_id,
            total_supply_usd=Decimal("20000"),
            total_borrow_usd=Decimal("12000"),
            utilization=Decimal("0.6"),
            supply_apy=Decimal("0.06"),
            borrow_apy=Decimal("0.04"),
            available_liquidity_usd=Decimal("8000"),
            max_ltv=Decimal("0.75"),
            liquidation_threshold=Decimal("0.80"),
            liquidation_penalty=Decimal("0.08"),
        ),
        dict(
            market_id=markets[3].market_id,
            total_supply_usd=Decimal("5000"),
            total_borrow_usd=Decimal("2000"),
            utilization=Decimal("0.4"),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.015"),
            available_liquidity_usd=Decimal("3000"),
            max_ltv=Decimal("0.6"),
            liquidation_threshold=Decimal("0.7"),
            liquidation_penalty=Decimal("0.1"),
        ),
    ]
    for snapshot_data in market_snapshots:
        for ts, block in [(sod_ts, "100"), (eod_ts, "200")]:
            session.add(
                MarketSnapshot(
                    as_of_ts_utc=ts,
                    block_number_or_slot=block,
                    source="rpc",
                    **snapshot_data,
                )
            )

    session.commit()

    with Session(engine) as engine_session:
        YieldEngine(engine_session).compute_daily(business_date=BUSINESS_DATE)
        engine_session.commit()

    with Session(engine) as ops_session:
        ops_market = Market(
            chain_id=chain_eth.chain_id,
            protocol_id=proto_traderjoe.protocol_id,
            native_market_key="traderjoe-buy-wall",
            market_address="0xops1",
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
        ops_session.add(ops_market)
        ops_session.flush()
        for ts, block in [(sod_ts, "150"), (eod_ts, "250")]:
            ops_session.add(
                PositionSnapshot(
                    as_of_ts_utc=ts,
                    block_number_or_slot=block,
                    wallet_id=w1.wallet_id,
                    market_id=ops_market.market_id,
                    position_key="ops-w1-buy-wall",
                    supplied_amount=Decimal("600"),
                    supplied_usd=Decimal("600"),
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("600"),
                    health_factor=None,
                    ltv=None,
                    source="rpc",
                )
            )
        ops_session.commit()

    with Session(engine) as alert_session:
        alert_session.add_all(
            [
                Alert(
                    ts_utc=datetime.now(UTC),
                    alert_type="high_utilization",
                    severity="high",
                    entity_type="market",
                    entity_id=str(markets[2].market_id),
                    payload_json={"utilization": "0.6"},
                    status="open",
                ),
                Alert(
                    ts_utc=datetime.now(UTC),
                    alert_type="low_health_factor",
                    severity="med",
                    entity_type="position",
                    entity_id="pos-w1-m1",
                    payload_json={"health_factor": "1.5"},
                    status="ack",
                ),
            ]
        )
        alert_session.commit()

    with Session(engine) as dq_session:
        dq_session.add_all(
            [
                DataQuality(
                    as_of_ts_utc=datetime.now(UTC),
                    stage="ingestion",
                    protocol_code="aave_v3",
                    chain_code="ethereum",
                    error_type="timeout",
                    error_message="RPC call timed out after 15s",
                ),
                DataQuality(
                    as_of_ts_utc=datetime.now(UTC),
                    stage="reconciliation",
                    protocol_code="morpho",
                    chain_code="arbitrum",
                    error_type="balance_mismatch",
                    error_message="Observed balance differs by more than threshold",
                ),
            ]
        )
        dq_session.commit()

    with Session(engine) as served_session:
        PortfolioViewEngine(served_session).compute_daily(business_date=BUSINESS_DATE)
        MarketViewEngine(served_session).compute_daily(business_date=BUSINESS_DATE)
        ExecutiveSummaryEngine(served_session).compute_daily(business_date=BUSINESS_DATE)
        served_session.add(
            HolderSupplyCoverageDaily(
                business_date=BUSINESS_DATE,
                as_of_ts_utc=eod_ts,
                chain_code="avalanche",
                token_symbol="savUSD",
                token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                raw_holder_wallet_count=763,
                monitoring_wallet_count=154,
                core_wallet_count=57,
                signoff_wallet_count=44,
                wallets_with_same_chain_deployed_supply=4,
                wallets_with_cross_chain_supply=12,
                gross_supply_usd=Decimal("87270000"),
                strategy_supply_usd=Decimal("5400000"),
                strategy_deployed_supply_usd=Decimal("3200000"),
                internal_supply_usd=Decimal("1250000"),
                explicit_excluded_supply_usd=Decimal("0"),
                net_customer_float_usd=Decimal("80620000"),
                direct_holder_supply_usd=Decimal("10532498.36"),
                core_direct_holder_supply_usd=Decimal("9642581.53"),
                signoff_direct_holder_supply_usd=Decimal("7839687.67"),
                same_chain_deployed_supply_usd=Decimal("0"),
                cross_chain_supply_usd=Decimal("7508005.17"),
                core_same_chain_deployed_supply_usd=Decimal("0"),
                signoff_same_chain_deployed_supply_usd=Decimal("0"),
                covered_supply_usd=Decimal("18040503.53"),
                core_covered_supply_usd=Decimal("17150586.70"),
                signoff_covered_supply_usd=Decimal("15347692.84"),
                covered_supply_pct=Decimal("0.2237760230"),
                core_covered_supply_pct=Decimal("0.2127360074"),
                signoff_covered_supply_pct=Decimal("0.1903712850"),
            )
        )
        served_session.add(
            HolderScorecardDaily(
                business_date=BUSINESS_DATE,
                as_of_ts_utc=eod_ts,
                tracked_holders=44,
                top10_holder_share=Decimal("0.61"),
                top25_holder_share=Decimal("0.82"),
                top100_holder_share=Decimal("1"),
                wallet_held_avant_usd=Decimal("14500000"),
                configured_deployed_avant_usd=Decimal("4800000"),
                total_canonical_avant_exposure_usd=Decimal("19300000"),
                base_share=Decimal("0.18"),
                staked_share=Decimal("0.74"),
                boosted_share=Decimal("0.08"),
                single_asset_pct=Decimal("0.66"),
                multi_asset_pct=Decimal("0.34"),
                single_wrapper_pct=Decimal("0.57"),
                multi_wrapper_pct=Decimal("0.43"),
                configured_collateral_users_pct=Decimal("0.27"),
                configured_leveraged_pct=Decimal("0.18"),
                whale_enter_count_7d=2,
                whale_exit_count_7d=1,
                whale_borrow_up_count_7d=3,
                whale_collateral_up_count_7d=4,
                markets_needing_capacity_review=2,
                dq_verified_holder_pct=Decimal("0.91"),
                visibility_gap_wallet_count=118,
            )
        )
        served_session.commit()

    test_session = Session(engine)
    try:
        yield test_session, SeedMetadata(business_date=BUSINESS_DATE)
    finally:
        test_session.close()
        engine.dispose()


@pytest.fixture()
def api_client(
    seeded_session: tuple[Session, SeedMetadata],
) -> Generator[tuple[TestClient, SeedMetadata], None, None]:
    session, metadata = seeded_session
    app = create_app()

    def _override_session() -> Generator[Session, None, None]:
        yield session

    app.dependency_overrides[get_session] = _override_session
    client = TestClient(app)
    try:
        yield client, metadata
    finally:
        app.dependency_overrides.clear()
