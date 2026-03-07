"""Market exposure API values match served DB rows."""

from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.db.models import MarketExposureDaily
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
