"""Positions endpoint filter, sort, and pagination tests."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata


def test_default_pagination(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    resp = client.get("/portfolio/positions")
    assert resp.status_code == 200
    data = resp.json()

    assert data["page"] == 1
    assert data["page_size"] == 50
    assert data["total_count"] >= 1
    assert len(data["positions"]) <= data["page_size"]
    assert len(data["positions"]) == data["total_count"]


def test_protocol_filter_narrows_results(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    all_resp = client.get("/portfolio/positions").json()
    aave_resp = client.get("/portfolio/positions?protocol_code=aave_v3").json()

    assert aave_resp["total_count"] < all_resp["total_count"]
    assert aave_resp["total_count"] > 0
    for pos in aave_resp["positions"]:
        assert pos["protocol_code"] == "aave_v3"


def test_product_filter_narrows_results(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    all_resp = client.get("/portfolio/positions").json()
    senior_resp = client.get("/portfolio/positions?product_code=stablecoin_senior").json()

    assert senior_resp["total_count"] < all_resp["total_count"]
    assert senior_resp["total_count"] > 0
    for pos in senior_resp["positions"]:
        assert pos["product_code"] == "stablecoin_senior"


def test_sort_by_gross_yield_asc(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    resp = client.get("/portfolio/positions?sort_by=gross_yield_usd&sort_dir=asc").json()
    yields = [
        float(p["gross_yield_usd"]) for p in resp["positions"] if p["gross_yield_usd"] is not None
    ]
    assert yields == sorted(yields)


def test_pagination_page_2(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    page1 = client.get("/portfolio/positions?page=1&page_size=2").json()
    page2 = client.get("/portfolio/positions?page=2&page_size=2").json()

    assert page1["page"] == 1
    assert page2["page"] == 2
    assert page1["total_count"] == page2["total_count"]

    # Pages should have different positions (if total > page_size)
    if page1["total_count"] > 2:
        keys_1 = {p["position_key"] for p in page1["positions"]}
        keys_2 = {p["position_key"] for p in page2["positions"]}
        assert keys_1.isdisjoint(keys_2)


def test_total_count_consistent_across_pages(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    page1 = client.get("/portfolio/positions?page=1&page_size=1").json()
    page2 = client.get("/portfolio/positions?page=2&page_size=1").json()
    full = client.get("/portfolio/positions?page=1&page_size=100").json()

    assert page1["total_count"] == page2["total_count"] == full["total_count"]
