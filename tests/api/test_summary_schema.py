"""Validate the served executive summary contract."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata


def test_summary_has_expected_sections(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, meta = api_client
    response = client.get("/summary/executive")
    assert response.status_code == 200

    data = response.json()
    assert data["business_date"] == str(meta.business_date)
    assert set(data) == {
        "business_date",
        "executive",
        "portfolio_summary",
        "market_summary",
        "freshness",
    }


def test_summary_executive_fields_are_populated(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    executive = client.get("/summary/executive").json()["executive"]

    assert float(executive["nav_usd"]) > 0
    assert float(executive["portfolio_net_equity_usd"]) > 0
    assert float(executive["market_stability_ops_net_equity_usd"]) > 0
    assert float(executive["total_net_yield_mtd_usd"]) > 0
    assert float(executive["portfolio_aggregate_roe_annualized"]) > 0
    assert executive["open_alert_count"] == 1
    assert executive["customer_metrics_ready"] is False


def test_summary_freshness_fields_present(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, _ = api_client
    freshness = client.get("/summary/executive").json()["freshness"]

    assert freshness["last_position_snapshot_utc"] is not None
    assert freshness["last_market_snapshot_utc"] is not None
    assert freshness["position_snapshot_age_hours"] is not None
    assert freshness["market_snapshot_age_hours"] is not None
    assert freshness["open_dq_issues_24h"] == 2
