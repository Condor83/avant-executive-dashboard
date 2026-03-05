"""Rollup consistency: summary totals == sum of product-level values."""

from __future__ import annotations

from decimal import Decimal

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata

# Sub-dust tolerance for Numeric(38,18) aggregation rounding
TOLERANCE = Decimal("1E-15")


def _approx_eq(a: Decimal, b: Decimal) -> bool:
    return abs(a - b) <= TOLERANCE


def test_product_yields_sum_to_summary_yesterday(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    summary = client.get("/summary").json()
    products = client.get("/portfolio/products").json()

    yesterday = summary["yield_yesterday"]
    product_gross = sum(
        (Decimal(p["yesterday"]["gross_yield_usd"]) for p in products), Decimal("0")
    )
    product_net = sum((Decimal(p["yesterday"]["net_yield_usd"]) for p in products), Decimal("0"))
    product_equity = sum(
        (Decimal(p["yesterday"]["avg_equity_usd"]) for p in products), Decimal("0")
    )

    assert _approx_eq(product_gross, Decimal(yesterday["gross_yield_usd"]))
    assert _approx_eq(product_net, Decimal(yesterday["net_yield_usd"]))
    assert _approx_eq(product_equity, Decimal(yesterday["avg_equity_usd"]))


def test_product_yields_sum_to_summary_trailing_7d(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    summary = client.get("/summary").json()
    products = client.get("/portfolio/products").json()

    trailing = summary["yield_trailing_7d"]
    product_gross = sum(
        (Decimal(p["trailing_7d"]["gross_yield_usd"]) for p in products), Decimal("0")
    )
    product_net = sum((Decimal(p["trailing_7d"]["net_yield_usd"]) for p in products), Decimal("0"))

    assert _approx_eq(product_gross, Decimal(trailing["gross_yield_usd"]))
    assert _approx_eq(product_net, Decimal(trailing["net_yield_usd"]))


def test_product_yields_sum_to_summary_trailing_30d(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    summary = client.get("/summary").json()
    products = client.get("/portfolio/products").json()

    trailing = summary["yield_trailing_30d"]
    product_gross = sum(
        (Decimal(p["trailing_30d"]["gross_yield_usd"]) for p in products), Decimal("0")
    )
    product_net = sum((Decimal(p["trailing_30d"]["net_yield_usd"]) for p in products), Decimal("0"))

    assert _approx_eq(product_gross, Decimal(trailing["gross_yield_usd"]))
    assert _approx_eq(product_net, Decimal(trailing["net_yield_usd"]))
