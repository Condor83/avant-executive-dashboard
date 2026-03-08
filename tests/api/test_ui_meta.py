"""UI metadata endpoint returns server-owned labels and vocabularies."""

from __future__ import annotations

from fastapi.testclient import TestClient

from tests.api.conftest import SeedMetadata


def test_ui_metadata_contains_products_protocols_and_alert_labels(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    data = client.get("/meta/ui").json()
    sort_option_values = {option["value"] for option in data["position_sort_options"]}
    sort_option_labels = {option["label"] for option in data["position_sort_options"]}

    assert len(data["products"]) == 3
    assert any(option["value"] == "stablecoin_senior" for option in data["products"])
    assert any(option["value"] == "aave_v3" for option in data["protocols"])
    assert any(option["value"] == "ethereum" for option in data["chains"])
    assert any(
        option["value"] == "0x1111111111111111111111111111111111111111"
        for option in data["wallets"]
    )
    assert any("savUSD" in option["label"] for option in data["wallets"])
    assert any(
        option["value"] == "med" and option["label"] == "Medium"
        for option in data["alert_severity_options"]
    )
    assert any(
        option["value"] == "ack" and option["label"] == "Acknowledged"
        for option in data["alert_status_options"]
    )
    assert "strategy_fee_daily_usd" in sort_option_values
    assert "avant_gop_daily_usd" in sort_option_values
    assert "gross_yield_mtd_usd" not in sort_option_values
    assert "net_yield_mtd_usd" not in sort_option_values
    assert "Daily Performance Fee" in sort_option_labels
    assert "Daily GOP" in sort_option_labels
