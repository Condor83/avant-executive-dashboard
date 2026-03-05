"""Unit tests for manual/aliased pricing behavior."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, cast

from core.pricing import PriceOracle
from core.types import PriceRequest


class _FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeClient:
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.urls: list[str] = []

    def get(self, url: str) -> _FakeResponse:
        self.urls.append(url)
        return _FakeResponse(self.payload)

    def close(self) -> None:
        return None


def test_manual_linea_avusd_price_override_is_applied() -> None:
    client = _FakeClient(payload={"coins": {}})
    oracle = PriceOracle(
        base_url="https://coins.llama.fi",
        timeout_seconds=5,
        client=cast(Any, client),
    )
    try:
        result = oracle.fetch_prices(
            [
                PriceRequest(
                    token_id=616,
                    chain_code="linea",
                    address_or_mint="0x37C44fc08E403EFC0946c0623CB1164a52ce1576",
                    symbol="avUSD",
                )
            ],
            as_of_ts_utc=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
        )
    finally:
        oracle.close()

    assert not result.issues
    assert len(result.quotes) == 1
    assert result.quotes[0].price_usd == Decimal("1")
    assert client.urls == []


def test_linea_savusd_uses_avalanche_alias_price() -> None:
    client = _FakeClient(
        payload={
            "coins": {
                "avax:0x06d47f3fb376649c3a9dafe069b3d6e35572219e": {"price": "1.1517097923089619"}
            }
        }
    )
    oracle = PriceOracle(
        base_url="https://coins.llama.fi",
        timeout_seconds=5,
        client=cast(Any, client),
    )
    try:
        result = oracle.fetch_prices(
            [
                PriceRequest(
                    token_id=617,
                    chain_code="linea",
                    address_or_mint="0x5c247948FD58bb02b6C4678D9940F5E6b9AF1127",
                    symbol="savUSD",
                )
            ],
            as_of_ts_utc=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
        )
    finally:
        oracle.close()

    assert not result.issues
    assert len(result.quotes) == 1
    assert result.quotes[0].price_usd == Decimal("1.1517097923089619")
    assert len(client.urls) == 1
    assert "avax:0x06d47f3fb376649c3a9dafe069b3d6e35572219e" in client.urls[0]


def test_linea_savusd_alias_missing_emits_price_missing_with_alias_payload() -> None:
    client = _FakeClient(payload={"coins": {}})
    oracle = PriceOracle(
        base_url="https://coins.llama.fi",
        timeout_seconds=5,
        client=cast(Any, client),
    )
    try:
        result = oracle.fetch_prices(
            [
                PriceRequest(
                    token_id=617,
                    chain_code="linea",
                    address_or_mint="0x5c247948fd58bb02b6c4678d9940f5e6b9af1127",
                    symbol="savUSD",
                )
            ],
            as_of_ts_utc=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
        )
    finally:
        oracle.close()

    assert not result.quotes
    assert len(result.issues) == 1
    issue = result.issues[0]
    assert issue.error_type == "price_missing"
    assert issue.payload_json is not None
    assert issue.payload_json["alias_target_chain"] == "avalanche"
    assert issue.payload_json["alias_target_address"] == (
        "0x06d47f3fb376649c3a9dafe069b3d6e35572219e"
    )
