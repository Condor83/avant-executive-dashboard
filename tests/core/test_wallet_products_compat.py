"""Wallet-products compatibility parsing tests."""

from __future__ import annotations

from pathlib import Path

from core.config import load_wallet_products_config


def test_wallet_products_legacy_shape_parses_to_assignments() -> None:
    wallet_products = load_wallet_products_config(Path("config/wallet_products.yaml"))

    assert wallet_products.assignments
    assert any(item.product_code == "stablecoin_senior" for item in wallet_products.assignments)
    assert any(item.product_code == "stablecoin_junior" for item in wallet_products.assignments)


def test_wallet_products_assignments_are_unique_by_wallet() -> None:
    wallet_products = load_wallet_products_config(Path("config/wallet_products.yaml"))

    addresses = [assignment.wallet_address for assignment in wallet_products.assignments]
    assert len(addresses) == len(set(addresses))
