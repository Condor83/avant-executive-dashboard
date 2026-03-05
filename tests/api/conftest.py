"""Test fixtures for API integration tests."""

from __future__ import annotations

import os
from collections.abc import Generator
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import psycopg
import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from psycopg import sql
from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import Session

from analytics.market_engine import MarketEngine
from analytics.yield_engine import YieldEngine, denver_business_bounds_utc
from api.app import create_app
from api.deps import get_session
from core.db.models import (
    Alert,
    Chain,
    DataQuality,
    Market,
    MarketSnapshot,
    PositionSnapshot,
    Product,
    Protocol,
    Token,
    Wallet,
    WalletProductMap,
)

DEFAULT_TEST_ADMIN_URL = "postgresql+psycopg://postgres:postgres@localhost:5432/postgres"

BUSINESS_DATE = date(2026, 3, 3)


@dataclass
class SeedMetadata:
    """Metadata about the seeded test data for assertions."""

    business_date: date
    chains: list[Chain]
    protocols: list[Protocol]
    products: list[Product]
    wallets: list[Wallet]
    markets: list[Market]
    tokens: list[Token]


@pytest.fixture()
def postgres_database_url() -> Generator[str, None, None]:
    """Create an isolated Postgres database and return its SQLAlchemy URL."""
    admin_url = make_url(os.getenv("AVANT_TEST_DATABASE_URL", DEFAULT_TEST_ADMIN_URL))
    db_name = f"avant_test_{uuid4().hex[:12]}"
    admin_psycopg_dsn = admin_url.render_as_string(hide_password=False).replace("+psycopg", "")
    test_url = admin_url.set(database=db_name).render_as_string(hide_password=False)

    try:
        with psycopg.connect(admin_psycopg_dsn, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
    except psycopg.OperationalError as exc:
        pytest.skip(f"Postgres not reachable for DB tests: {exc}")

    try:
        yield test_url
    finally:
        with psycopg.connect(admin_psycopg_dsn, autocommit=True) as conn:
            conn.execute(
                "SELECT pg_terminate_backend(pid) "
                "FROM pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_backend_pid()",
                (db_name,),
            )
            conn.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))


@pytest.fixture()
def seeded_session(
    postgres_database_url: str,
) -> Generator[tuple[Session, SeedMetadata], None, None]:
    """Apply migrations, seed dimensions + facts, run engines, return session."""
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    session = Session(engine)

    sod_ts, eod_ts = denver_business_bounds_utc(BUSINESS_DATE)

    # -- Dimensions --
    chain_eth = Chain(chain_code="ethereum")
    chain_arb = Chain(chain_code="arbitrum")
    proto_aave = Protocol(protocol_code="aave_v3")
    proto_morpho = Protocol(protocol_code="morpho")
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

    m1 = Market(
        chain_id=chain_eth.chain_id,
        protocol_id=proto_aave.protocol_id,
        market_address="0xaaa1",
        base_asset_token_id=token_usdc.token_id,
    )
    m2 = Market(
        chain_id=chain_eth.chain_id,
        protocol_id=proto_aave.protocol_id,
        market_address="0xaaa2",
        base_asset_token_id=token_wbtc.token_id,
    )
    m3 = Market(
        chain_id=chain_arb.chain_id,
        protocol_id=proto_morpho.protocol_id,
        market_address="0xbbb1",
        base_asset_token_id=token_usdc.token_id,
    )
    m4 = Market(
        chain_id=chain_arb.chain_id,
        protocol_id=proto_morpho.protocol_id,
        market_address="0xbbb2",
    )
    session.add_all([m1, m2, m3, m4])
    session.flush()

    session.add_all(
        [
            WalletProductMap(wallet_id=w1.wallet_id, product_id=product_senior.product_id),
            WalletProductMap(wallet_id=w2.wallet_id, product_id=product_junior.product_id),
            WalletProductMap(wallet_id=w3.wallet_id, product_id=product_btc.product_id),
        ]
    )

    # -- Position snapshots (SOD + EOD for 4 positions) --
    _pos_snapshots = [
        # w1 on m1 (aave/eth/USDC)
        dict(
            wallet_id=w1.wallet_id,
            market_id=m1.market_id,
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
        # w1 on m2 (aave/eth/WBTC)
        dict(
            wallet_id=w1.wallet_id,
            market_id=m2.market_id,
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
        # w2 on m3 (morpho/arb/USDC)
        dict(
            wallet_id=w2.wallet_id,
            market_id=m3.market_id,
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
        # w3 on m4 (morpho/arb)
        dict(
            wallet_id=w3.wallet_id,
            market_id=m4.market_id,
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
    for ps_data in _pos_snapshots:
        for ts, block in [(sod_ts, "100"), (eod_ts, "200")]:
            session.add(
                PositionSnapshot(
                    as_of_ts_utc=ts,
                    block_number_or_slot=block,
                    source="rpc",
                    health_factor=Decimal("1.5"),
                    ltv=Decimal("0.5"),
                    **ps_data,
                )
            )

    # -- Market snapshots (SOD + EOD for 4 markets) --
    _mkt_snapshots = [
        dict(
            market_id=m1.market_id,
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
            market_id=m2.market_id,
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
            market_id=m3.market_id,
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
            market_id=m4.market_id,
            total_supply_usd=Decimal("5000"),
            total_borrow_usd=Decimal("2000"),
            utilization=Decimal("0.4"),
            supply_apy=Decimal("0.03"),
            borrow_apy=Decimal("0.015"),
            available_liquidity_usd=Decimal("3000"),
            max_ltv=None,
            liquidation_threshold=None,
            liquidation_penalty=None,
        ),
    ]
    for ms_data in _mkt_snapshots:
        for ts, block in [(sod_ts, "100"), (eod_ts, "200")]:
            session.add(
                MarketSnapshot(
                    as_of_ts_utc=ts,
                    block_number_or_slot=block,
                    source="rpc",
                    **ms_data,
                )
            )

    session.commit()

    # -- Run engines --
    with Session(engine) as eng_session:
        YieldEngine(eng_session).compute_daily(business_date=BUSINESS_DATE)
        eng_session.commit()

    with Session(engine) as eng_session:
        MarketEngine(eng_session).compute_daily(business_date=BUSINESS_DATE)
        eng_session.commit()

    # -- Seed alerts --
    with Session(engine) as alert_session:
        alert_session.add_all(
            [
                Alert(
                    ts_utc=datetime.now(UTC),
                    alert_type="high_utilization",
                    severity="high",
                    entity_type="market",
                    entity_id=str(m3.market_id),
                    payload_json={"utilization": 0.6},
                    status="open",
                ),
                Alert(
                    ts_utc=datetime.now(UTC),
                    alert_type="low_health_factor",
                    severity="med",
                    entity_type="position",
                    entity_id="pos-w1-m1",
                    payload_json={"health_factor": 1.5},
                    status="open",
                ),
                Alert(
                    ts_utc=datetime.now(UTC),
                    alert_type="concentration_risk",
                    severity="low",
                    entity_type="market",
                    entity_id=str(m1.market_id),
                    payload_json={"share": 0.1},
                    status="resolved",
                ),
            ]
        )
        alert_session.commit()

    # -- Seed data quality issues --
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
                    error_message="DeBank balance differs by >5%",
                ),
            ]
        )
        dq_session.commit()

    metadata = SeedMetadata(
        business_date=BUSINESS_DATE,
        chains=[chain_eth, chain_arb],
        protocols=[proto_aave, proto_morpho],
        products=[product_senior, product_junior, product_btc],
        wallets=[w1, w2, w3],
        markets=[m1, m2, m3, m4],
        tokens=[token_usdc, token_wbtc],
    )

    # Re-open session for test use
    test_session = Session(engine)
    try:
        yield test_session, metadata
    finally:
        test_session.close()
        engine.dispose()


@pytest.fixture()
def api_client(
    seeded_session: tuple[Session, SeedMetadata],
) -> Generator[tuple[TestClient, SeedMetadata], None, None]:
    """Return a TestClient with the seeded session injected."""
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
