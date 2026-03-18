"""Validate the served executive summary contract."""

from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata


def test_summary_has_expected_sections(
    api_client: tuple[TestClient, SeedMetadata],
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "api.routers.summary.AvantYieldOracle.get_token_apy",
        lambda self, symbol: Decimal("0.1111"),
    )
    client, meta = api_client
    response = client.get("/summary/executive")
    assert response.status_code == 200

    data = response.json()
    assert data["business_date"] == str(meta.business_date)
    assert set(data) == {
        "business_date",
        "executive",
        "holder_summary",
        "portfolio_summary",
        "market_summary",
        "product_performance",
        "protocol_concentration",
        "freshness",
    }


def test_summary_executive_fields_are_populated(
    api_client: tuple[TestClient, SeedMetadata],
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "api.routers.summary.AvantYieldOracle.get_token_apy",
        lambda self, symbol: Decimal("0.1111"),
    )
    client, _ = api_client
    executive = client.get("/summary/executive").json()["executive"]

    assert float(executive["nav_usd"]) > 0
    assert float(executive["portfolio_net_equity_usd"]) > 0
    assert float(executive["market_stability_ops_net_equity_usd"]) > 0
    assert float(executive["total_net_yield_mtd_usd"]) > 0
    assert float(executive["portfolio_aggregate_roe_annualized"]) > 0
    assert executive["open_alert_count"] == 1
    assert executive["customer_metrics_ready"] is False


def test_summary_freshness_fields_present(
    api_client: tuple[TestClient, SeedMetadata],
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "api.routers.summary.AvantYieldOracle.get_token_apy",
        lambda self, symbol: Decimal("0.1111"),
    )
    client, _ = api_client
    freshness = client.get("/summary/executive").json()["freshness"]

    assert freshness["last_position_snapshot_utc"] is not None
    assert freshness["last_market_snapshot_utc"] is not None
    assert freshness["position_snapshot_age_hours"] is not None
    assert freshness["market_snapshot_age_hours"] is not None
    assert freshness["open_dq_issues_24h"] == 2


def test_summary_holder_block_is_present(
    api_client: tuple[TestClient, SeedMetadata],
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "api.routers.summary.AvantYieldOracle.get_token_apy",
        lambda self, symbol: Decimal("0.1111"),
    )
    client, _ = api_client
    payload = client.get("/summary/executive").json()
    holder = payload["holder_summary"]

    assert holder is not None
    assert holder["supply_coverage_token_symbol"] == "savUSD"
    assert holder["supply_coverage_chain_code"] == "avalanche"
    assert holder["monitored_holder_count"] == 154
    assert holder["attributed_holder_count"] == 44
    assert float(holder["attribution_completion_pct"]) > 0
    assert holder["core_holder_wallet_count"] == 44
    assert holder["whale_wallet_count"] == 0
    assert float(holder["strategy_supply_usd"]) > 0
    assert float(holder["strategy_deployed_supply_usd"]) > 0
    assert float(holder["net_customer_float_usd"]) > 0
    assert float(holder["covered_supply_usd"]) > 0
    assert float(holder["covered_supply_pct"]) > 0
    assert float(holder["cross_chain_supply_usd"]) > 0
    assert float(holder["total_canonical_avant_exposure_usd"]) > 0
    assert float(holder["top10_holder_share"]) > 0
    assert holder["visibility_gap_wallet_count"] == 118
    assert payload["product_performance"] is not None
    assert len(payload["product_performance"]) == 6
    assert payload["protocol_concentration"] is not None
    assert len(payload["protocol_concentration"]) > 0
