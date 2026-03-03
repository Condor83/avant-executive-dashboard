"""Boundary readiness tests for Denver SOD/EOD snapshot selection."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc, select_business_day_boundaries
from core.db.models import Chain, Market, PositionSnapshot, Protocol, Wallet


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _seed_minimal_dimensions(session: Session) -> tuple[int, int]:
    chain = Chain(chain_code="ethereum")
    protocol = Protocol(protocol_code="aave_v3")
    wallet = Wallet(address="0x1111111111111111111111111111111111111111", wallet_type="strategy")
    session.add_all([chain, protocol, wallet])
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
    return wallet.wallet_id, market.market_id


def _snapshot(
    *,
    ts_utc: datetime,
    wallet_id: int,
    market_id: int,
    key_suffix: str,
) -> PositionSnapshot:
    return PositionSnapshot(
        as_of_ts_utc=ts_utc,
        wallet_id=wallet_id,
        market_id=market_id,
        position_key=f"pos-{key_suffix}",
        supplied_amount=Decimal("100"),
        supplied_usd=Decimal("100"),
        borrowed_amount=Decimal("10"),
        borrowed_usd=Decimal("10"),
        supply_apy=Decimal("0.1"),
        borrow_apy=Decimal("0.03"),
        reward_apy=Decimal("0"),
        equity_usd=Decimal("90"),
        source="rpc",
        block_number_or_slot="1",
    )


def test_boundary_check_passes_when_exact_sod_and_eod_exist(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 3)
    sod_ts, eod_ts = denver_business_bounds_utc(business_date)

    with Session(engine) as session:
        wallet_id, market_id = _seed_minimal_dimensions(session)
        session.add_all(
            [
                _snapshot(
                    ts_utc=sod_ts, wallet_id=wallet_id, market_id=market_id, key_suffix="sod"
                ),
                _snapshot(
                    ts_utc=eod_ts, wallet_id=wallet_id, market_id=market_id, key_suffix="eod"
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        result = select_business_day_boundaries(session, business_date=business_date)

    assert result.ready_for_signoff is True
    assert result.exact_sod_present is True
    assert result.exact_eod_present is True
    assert result.used_sod_fallback is False
    assert result.used_eod_fallback is False
    assert result.sod_ts_utc == sod_ts
    assert result.eod_ts_utc == eod_ts


def test_boundary_check_warns_when_exact_boundaries_are_missing(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 3)
    sod_ts, _ = denver_business_bounds_utc(business_date)
    intra_sod = sod_ts.replace(hour=8)
    intra_eod = sod_ts.replace(hour=23)

    with Session(engine) as session:
        wallet_id, market_id = _seed_minimal_dimensions(session)
        session.add_all(
            [
                _snapshot(
                    ts_utc=intra_sod, wallet_id=wallet_id, market_id=market_id, key_suffix="intra1"
                ),
                _snapshot(
                    ts_utc=intra_eod, wallet_id=wallet_id, market_id=market_id, key_suffix="intra2"
                ),
            ]
        )
        session.commit()

    with Session(engine) as session:
        result = select_business_day_boundaries(session, business_date=business_date)

    assert result.ready_for_signoff is False
    assert result.exact_sod_present is False
    assert result.exact_eod_present is False
    assert result.used_sod_fallback is True
    assert result.used_eod_fallback is True
    assert result.sod_ts_utc == intra_sod
    assert result.eod_ts_utc == intra_eod
