"""Market exposure API values match served DB rows."""

from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.market_exposures import build_market_exposure_usage_metrics
from core.db.models import (
    Chain,
    Market,
    MarketExposureDaily,
    MarketSnapshot,
    Price,
    Protocol,
    Token,
)
from tests.api.conftest import SeedMetadata


def test_market_exposure_count_matches_db(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session

    api_rows = client.get("/markets/exposures").json()
    db_rows = session.scalars(
        select(MarketExposureDaily).where(MarketExposureDaily.business_date == meta.business_date)
    ).all()

    assert len(api_rows) == len(db_rows)


def test_market_exposure_supply_matches_db(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session

    api_rows = client.get("/markets/exposures").json()
    db_rows = session.scalars(
        select(MarketExposureDaily).where(MarketExposureDaily.business_date == meta.business_date)
    ).all()
    db_by_exposure = {row.market_exposure_id: row.total_supply_usd for row in db_rows}

    for api_row in api_rows:
        assert Decimal(api_row["total_supply_usd"]) == db_by_exposure[api_row["market_exposure_id"]]


def test_watch_only_filter_returns_alerting_rows(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    rows = client.get("/markets/exposures?watch_only=true").json()

    assert len(rows) == 1
    assert rows[0]["active_alert_count"] == 1
    assert rows[0]["watch_status"] == "alerting"


def test_market_exposure_enriches_pair_monitor_fields(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session

    protocol_id = session.scalar(
        select(Protocol.protocol_id).where(Protocol.protocol_code == "aave_v3")
    )
    chain_id = session.scalar(select(Chain.chain_id).where(Chain.chain_code == "ethereum"))
    wbtc_token_id = session.scalar(
        select(Token.token_id).where(Token.chain_id == chain_id).where(Token.symbol == "WBTC")
    )
    usdc_token_id = session.scalar(
        select(Token.token_id).where(Token.chain_id == chain_id).where(Token.symbol == "USDC")
    )
    wbtc_market_id = session.scalar(
        select(Market.market_id)
        .where(Market.protocol_id == protocol_id)
        .where(Market.base_asset_token_id == wbtc_token_id)
    )
    usdc_market_id = session.scalar(
        select(Market.market_id)
        .where(Market.protocol_id == protocol_id)
        .where(Market.base_asset_token_id == usdc_token_id)
    )

    latest_ts = session.scalar(
        select(MarketSnapshot.as_of_ts_utc).order_by(MarketSnapshot.as_of_ts_utc.desc()).limit(1)
    )
    assert latest_ts is not None

    wbtc_snapshot = session.scalar(
        select(MarketSnapshot)
        .where(MarketSnapshot.market_id == wbtc_market_id)
        .where(MarketSnapshot.as_of_ts_utc == latest_ts)
    )
    usdc_snapshot = session.scalar(
        select(MarketSnapshot)
        .where(MarketSnapshot.market_id == usdc_market_id)
        .where(MarketSnapshot.as_of_ts_utc == latest_ts)
    )
    assert wbtc_snapshot is not None
    assert usdc_snapshot is not None
    wbtc_snapshot.caps_json = {"supply_cap": "10", "borrow_cap": "1"}
    usdc_snapshot.caps_json = {"supply_cap": "20000", "borrow_cap": "10000"}

    session.add_all(
        [
            Price(
                ts_utc=latest_ts,
                token_id=wbtc_token_id,
                price_usd=Decimal("100"),
                source="rpc",
                confidence=Decimal("1"),
            ),
            Price(
                ts_utc=latest_ts,
                token_id=usdc_token_id,
                price_usd=Decimal("1"),
                source="rpc",
                confidence=Decimal("1"),
            ),
        ]
    )
    session.commit()

    rows = client.get("/markets/exposures?protocol_code=aave_v3").json()
    row = next(
        item
        for item in rows
        if item["protocol_code"] == "aave_v3"
        and item["supply_symbol"] == "WBTC"
        and item["debt_symbol"] == "USDC"
    )

    usage_metrics = build_market_exposure_usage_metrics(session)
    expected_borrow_usd = usage_metrics[row["exposure_slug"]][2]
    expected_collateral_yield = usage_metrics[row["exposure_slug"]][3]
    assert expected_collateral_yield is not None

    assert Decimal(row["collateral_yield_apy"]) == Decimal(str(expected_collateral_yield))
    assert Decimal(row["spread_apy"]) == expected_collateral_yield - Decimal(
        row["weighted_borrow_apy"]
    )
    assert Decimal(row["avant_borrow_share"]) == expected_borrow_usd / Decimal(
        row["total_borrow_usd"]
    )
    assert Decimal(row["supply_cap_usd"]) == Decimal("1000")
    assert Decimal(row["borrow_cap_usd"]) == Decimal("10000")
    assert Decimal(row["collateral_max_ltv"]) == Decimal("0.7000000000")
