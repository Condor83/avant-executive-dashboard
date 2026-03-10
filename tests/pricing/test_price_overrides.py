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


class _RoutingFakeClient:
    def __init__(self, routes: dict[str, dict[str, Any]]) -> None:
        self.routes = routes
        self.urls: list[str] = []

    def get(self, url: str) -> _FakeResponse:
        self.urls.append(url)
        for needle, payload in self.routes.items():
            if needle in url:
                return _FakeResponse(payload)
        return _FakeResponse({"coins": {}})

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


def test_bera_savusd_uses_avalanche_alias_price() -> None:
    client = _FakeClient(
        payload={
            "coins": {
                "avax:0x06d47f3fb376649c3a9dafe069b3d6e35572219e": {"price": "1.1529774516241514"}
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
                    token_id=901,
                    chain_code="bera",
                    address_or_mint="0xa744fe3688291ac3a4a7ec917678783ad9946a1e",
                    symbol="savUSD",
                )
            ],
            as_of_ts_utc=datetime(2026, 3, 5, 0, 0, tzinfo=UTC),
        )
    finally:
        oracle.close()

    assert not result.issues
    assert len(result.quotes) == 1
    assert result.quotes[0].price_usd == Decimal("1.1529774516241514")
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


def test_bera_savusd_alias_missing_emits_price_missing_with_alias_payload() -> None:
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
                    token_id=901,
                    chain_code="bera",
                    address_or_mint="0xa744fe3688291ac3a4a7ec917678783ad9946a1e",
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


def test_avant_price_history_fallback_is_applied_when_defillama_missing() -> None:
    client = _RoutingFakeClient(
        routes={
            "/prices/current/": {"coins": {}},
            "/priceHistory": {
                "lastUpdated": "03/05/26",
                "data": [
                    {"date": "02/26/26", "avusdx": 1.170928},
                    {"date": "03/05/26", "avusdx": 1.173372},
                ],
            },
        }
    )
    oracle = PriceOracle(
        base_url="https://coins.llama.fi",
        timeout_seconds=5,
        avant_api_base_url="https://app.avantprotocol.com/api",
        client=cast(Any, client),
    )
    try:
        result = oracle.fetch_prices(
            [
                PriceRequest(
                    token_id=902,
                    chain_code="avalanche",
                    address_or_mint="0xdd1cdfa52e7d8474d434cd016fd346701db6b3b9",
                    symbol="avUSDx",
                )
            ],
            as_of_ts_utc=datetime(2026, 3, 9, 0, 0, tzinfo=UTC),
        )
    finally:
        oracle.close()

    assert not result.issues
    assert len(result.quotes) == 1
    assert result.quotes[0].price_usd == Decimal("1.173372")
    assert result.quotes[0].source == "avant_api"
    assert any("/priceHistory" in url for url in client.urls)


def test_avant_price_history_fallback_uses_latest_row_not_after_as_of_date() -> None:
    client = _RoutingFakeClient(
        routes={
            "/prices/current/": {"coins": {}},
            "/priceHistory": {
                "lastUpdated": "03/05/26",
                "data": [
                    {"date": "02/19/26", "avusdx": 1.170928},
                    {"date": "03/05/26", "avusdx": 1.173372},
                ],
            },
        }
    )
    oracle = PriceOracle(
        base_url="https://coins.llama.fi",
        timeout_seconds=5,
        avant_api_base_url="https://app.avantprotocol.com/api",
        client=cast(Any, client),
    )
    try:
        result = oracle.fetch_prices(
            [
                PriceRequest(
                    token_id=902,
                    chain_code="avalanche",
                    address_or_mint="0xdd1cdfa52e7d8474d434cd016fd346701db6b3b9",
                    symbol="avUSDx",
                )
            ],
            as_of_ts_utc=datetime(2026, 3, 1, 0, 0, tzinfo=UTC),
        )
    finally:
        oracle.close()

    assert not result.issues
    assert len(result.quotes) == 1
    assert result.quotes[0].price_usd == Decimal("1.170928")
    assert result.quotes[0].source == "avant_api"
