"""Unit tests for the Avant-native yield helper."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import httpx
import pytest

from core.yields import AvantYieldOracle


class _StubResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _StubClient:
    def __init__(self, payloads: dict[str, object]) -> None:
        self.payloads = payloads
        self.calls = 0
        self.last_url: str | None = None

    def get(self, url: str) -> _StubResponse:
        self.calls += 1
        self.last_url = url
        path = url.split("/api", 1)[1]
        return _StubResponse(self.payloads[path])

    def close(self) -> None:
        return None


def test_get_token_apy_normalizes_and_caches_senior_token() -> None:
    client = _StubClient(
        {
            "/savusdApy": {"savusdApy": "7.45", "lastUpdated": "2026-03-06T06:10:14.874Z"},
        }
    )
    oracle = AvantYieldOracle(
        base_url="https://app.avantprotocol.com/api",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    assert oracle.get_token_apy("savUSD") == Decimal("0.0745")
    assert oracle.get_token_apy("savUSD") == Decimal("0.0745")
    assert client.calls == 1
    assert client.last_url == "https://app.avantprotocol.com/api/savusdApy"


def test_get_token_apy_normalizes_junior_token() -> None:
    client = _StubClient(
        {
            "/apy/avusdx": {"apy": "11.21", "lastUpdated": "2026-03-06"},
        }
    )
    oracle = AvantYieldOracle(
        base_url="https://app.avantprotocol.com/api",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    assert oracle.get_token_apy("avUSDx") == Decimal("0.1121")
    assert client.last_url == "https://app.avantprotocol.com/api/apy/avusdx"


def test_get_token_apy_raises_when_symbol_is_unknown() -> None:
    oracle = AvantYieldOracle(
        base_url="https://app.avantprotocol.com/api",
        timeout_seconds=1.0,
        client=cast(httpx.Client, _StubClient({})),
    )

    with pytest.raises(RuntimeError, match="not configured"):
        oracle.get_token_apy("wbravUSDC")


def test_get_token_apy_raises_when_payload_has_no_apy() -> None:
    client = _StubClient(
        {
            "/savethApy": {"lastUpdated": "2026-03-06T06:10:15.445Z"},
        }
    )
    oracle = AvantYieldOracle(
        base_url="https://app.avantprotocol.com/api",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    with pytest.raises(RuntimeError, match="missing APY field"):
        oracle.get_token_apy("savETH")


def test_get_token_apy_raises_on_negative_apy() -> None:
    client = _StubClient(
        {
            "/savbtcApy": {"savbtcApy": "-1.00", "lastUpdated": "2026-03-06T06:10:15.914Z"},
        }
    )
    oracle = AvantYieldOracle(
        base_url="https://app.avantprotocol.com/api",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    with pytest.raises(RuntimeError, match="negative"):
        oracle.get_token_apy("savBTC")
