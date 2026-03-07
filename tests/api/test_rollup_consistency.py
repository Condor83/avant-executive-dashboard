"""Served summaries reconcile to served position and market rows."""

from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata

TOLERANCE = Decimal("1E-15")


def _approx_eq(left: Decimal, right: Decimal) -> bool:
    return abs(left - right) <= TOLERANCE


def test_positions_sum_to_portfolio_summary(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, _ = api_client
    positions = client.get("/portfolio/positions/current").json()["positions"]
    summary = client.get("/portfolio/summary").json()

    total_equity = sum((Decimal(row["net_equity_usd"]) for row in positions), Decimal("0"))
    total_supply = sum((Decimal(row["supply_leg"]["usd_value"]) for row in positions), Decimal("0"))
    total_borrow = sum(
        (
            sum((Decimal(leg["usd_value"]) for leg in row["borrow_legs"]), Decimal("0"))
            for row in positions
        ),
        Decimal("0"),
    )

    assert _approx_eq(total_equity, Decimal(summary["total_net_equity_usd"]))
    assert _approx_eq(total_supply, Decimal(summary["total_supply_usd"]))
    assert _approx_eq(total_borrow, Decimal(summary["total_borrow_usd"]))


def test_executive_summary_matches_portfolio_and_market_summaries(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    summary = client.get("/summary/executive").json()
    portfolio_summary = client.get("/portfolio/summary").json()
    market_summary = client.get("/markets/summary").json()

    assert _approx_eq(
        Decimal(summary["executive"]["portfolio_net_equity_usd"]),
        Decimal(portfolio_summary["total_net_equity_usd"]),
    )
    assert _approx_eq(
        Decimal(summary["executive"]["portfolio_aggregate_roe_daily"]),
        Decimal(portfolio_summary["aggregate_roe_daily"]),
    )
    assert _approx_eq(
        Decimal(summary["executive"]["portfolio_aggregate_roe_annualized"]),
        Decimal(portfolio_summary["aggregate_roe_annualized"]),
    )
    assert _approx_eq(
        Decimal(summary["executive"]["market_total_supply_usd"]),
        Decimal(market_summary["total_supply_usd"]),
    )
    assert summary["executive"]["markets_at_risk_count"] == market_summary["markets_at_risk_count"]
