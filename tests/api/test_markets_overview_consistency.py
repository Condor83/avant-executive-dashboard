"""Markets overview consistency: API values match DB values."""

from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.db.models import MarketOverviewDaily
from tests.api.conftest import SeedMetadata


def test_overview_row_count_matches_db(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    resp = client.get("/markets/overview")
    assert resp.status_code == 200
    api_rows = resp.json()

    db_count = len(
        session.scalars(
            select(MarketOverviewDaily).where(
                MarketOverviewDaily.business_date == meta.business_date
            )
        ).all()
    )
    assert len(api_rows) == db_count


def test_overview_total_supply_matches_db(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    api_rows = client.get("/markets/overview").json()

    db_rows = session.scalars(
        select(MarketOverviewDaily).where(MarketOverviewDaily.business_date == meta.business_date)
    ).all()
    db_supply_by_market = {r.market_id: r.total_supply_usd for r in db_rows}

    for api_row in api_rows:
        market_id = api_row["market_id"]
        assert Decimal(api_row["total_supply_usd"]) == db_supply_by_market[market_id]


def test_overview_avant_supply_share_matches_db(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    api_rows = client.get("/markets/overview").json()

    db_rows = session.scalars(
        select(MarketOverviewDaily).where(MarketOverviewDaily.business_date == meta.business_date)
    ).all()
    db_share_by_market = {r.market_id: r.avant_supply_share for r in db_rows}

    for api_row in api_rows:
        market_id = api_row["market_id"]
        db_val = db_share_by_market[market_id]
        api_val = api_row["avant_supply_share"]
        if db_val is None:
            assert api_val is None
        else:
            assert Decimal(api_val) == db_val
