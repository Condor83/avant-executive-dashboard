"""Unit tests for shared DefiLlama yields helper."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import httpx
import pytest

from core.yields import DefiLlamaYieldOracle


class _StubResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, object]:
        return self.payload


class _StubClient:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls = 0
        self.last_url: str | None = None

    def get(self, url: str) -> _StubResponse:
        self.calls += 1
        self.last_url = url
        return _StubResponse(self.payload)

    def close(self) -> None:
        return None


def test_get_pool_apy_normalizes_and_caches() -> None:
    client = _StubClient(
        {
            "status": "success",
            "data": [{"timestamp": 1, "apy": 4.4}],
        }
    )
    oracle = DefiLlamaYieldOracle(
        base_url="https://yields.llama.fi",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    assert oracle.get_pool_apy("pool-id") == Decimal("0.044")
    assert oracle.get_pool_apy("pool-id") == Decimal("0.044")
    assert client.calls == 1
    assert client.last_url == "https://yields.llama.fi/chart/pool-id"


def test_get_pool_apy_raises_when_rows_missing() -> None:
    client = _StubClient({"status": "success", "data": []})
    oracle = DefiLlamaYieldOracle(timeout_seconds=1.0, client=cast(httpx.Client, client))

    with pytest.raises(RuntimeError, match="no rows"):
        oracle.get_pool_apy("pool-id")


def test_get_pool_apy_raises_on_negative_apy() -> None:
    client = _StubClient({"status": "success", "data": [{"apy": -1.0}]})
    oracle = DefiLlamaYieldOracle(timeout_seconds=1.0, client=cast(httpx.Client, client))

    with pytest.raises(RuntimeError, match="negative"):
        oracle.get_pool_apy("pool-id")
