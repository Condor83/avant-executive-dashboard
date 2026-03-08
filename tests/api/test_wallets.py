"""Wallet summary API contract tests."""

from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from core.db.models import Product, Wallet, WalletProductMap
from tests.api.conftest import SeedMetadata


def test_wallets_current_returns_one_row_per_wallet(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    response = client.get("/wallets/current")

    assert response.status_code == 200
    data = response.json()

    assert data["business_date"] == str(meta.business_date)
    assert data["total_count"] == 3
    assert [row["wallet_address"] for row in data["wallets"]] == [
        "0x2222222222222222222222222222222222222222",
        "0x1111111111111111111111111111111111111111",
        "0x3333333333333333333333333333333333333333",
    ]
    assert data["wallets"][0]["product_code"] == "stablecoin_junior"
    assert data["wallets"][0]["product_label"] == "avUSDx (Junior Stable)"
    assert data["wallets"][0]["total_supply_usd"] == "2000.000000000000000000"
    assert data["wallets"][0]["total_borrow_usd"] == "500.000000000000000000"
    assert data["wallets"][0]["total_tvl_usd"] == "1500.000000000000000000"
    assert data["wallets"][1]["total_supply_usd"] == "1500.000000000000000000"
    assert data["wallets"][1]["total_borrow_usd"] == "300.000000000000000000"
    assert data["wallets"][1]["total_tvl_usd"] == "1200.000000000000000000"
    assert data["wallets"][2]["total_supply_usd"] == "300.000000000000000000"
    assert data["wallets"][2]["total_borrow_usd"] == "50.000000000000000000"
    assert data["wallets"][2]["total_tvl_usd"] == "250.000000000000000000"


def test_wallets_current_hides_strategy_wallets_with_zero_exposure(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, _ = api_client
    session, _ = seeded_session

    product = session.query(Product).filter_by(product_code="stablecoin_senior").one()
    wallet = Wallet(
        address="0x4444444444444444444444444444444444444444",
        wallet_type="strategy",
    )
    session.add(wallet)
    session.flush()
    session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))
    session.commit()

    data = client.get("/wallets/current").json()

    assert data["total_count"] == 3
    assert all(
        row["wallet_address"] != "0x4444444444444444444444444444444444444444"
        for row in data["wallets"]
    )
