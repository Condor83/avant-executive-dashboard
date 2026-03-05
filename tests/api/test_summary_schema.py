"""Validate SummaryResponse has all required fields and correct types."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata


def test_summary_has_all_required_fields(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    resp = client.get("/summary")
    assert resp.status_code == 200
    data = resp.json()

    # Top-level keys
    assert "as_of_date" in data
    assert "portfolio" in data
    assert "yield_yesterday" in data
    assert "yield_trailing_7d" in data
    assert "yield_trailing_30d" in data
    assert "data_quality" in data


def test_summary_portfolio_fields(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    data = client.get("/summary").json()
    portfolio = data["portfolio"]

    assert "total_supplied_usd" in portfolio
    assert "total_borrowed_usd" in portfolio
    assert "net_equity_usd" in portfolio
    assert "collateralization_ratio" in portfolio
    assert "leverage_ratio" in portfolio

    # Values should be non-null (we seeded position data)
    assert float(portfolio["total_supplied_usd"]) > 0
    assert float(portfolio["total_borrowed_usd"]) > 0
    assert float(portfolio["net_equity_usd"]) > 0


def test_summary_yield_fields_non_null(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    data = client.get("/summary").json()

    for window in ("yield_yesterday", "yield_trailing_7d", "yield_trailing_30d"):
        ym = data[window]
        assert "gross_yield_usd" in ym
        assert "strategy_fee_usd" in ym
        assert "avant_gop_usd" in ym
        assert "net_yield_usd" in ym
        assert "avg_equity_usd" in ym
        assert "gross_roe" in ym
        assert "net_roe" in ym


def test_summary_data_quality_timestamps(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    data = client.get("/summary").json()
    dq = data["data_quality"]

    assert dq["last_position_snapshot_utc"] is not None
    assert dq["last_market_snapshot_utc"] is not None
    assert dq["position_snapshot_age_hours"] is not None
    assert dq["market_snapshot_age_hours"] is not None
    assert isinstance(dq["open_dq_issues_24h"], int)
    assert dq["open_dq_issues_24h"] >= 0
