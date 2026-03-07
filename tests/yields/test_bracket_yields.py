"""Unit tests for the Bracket yield helper."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import httpx
import pytest

from adapters.bracket import BracketNavYieldOracle


class _StubResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _StubClient:
    def __init__(self, payloads: list[dict[str, object]]) -> None:
        self.payloads = payloads
        self.calls = 0
        self.requests: list[dict[str, object]] = []

    def post(self, url: str, *, json: dict[str, object], headers: dict[str, str]) -> _StubResponse:
        del url, headers
        self.calls += 1
        self.requests.append(json)
        index = min(self.calls - 1, len(self.payloads) - 1)
        return _StubResponse(self.payloads[index])

    def close(self) -> None:
        return None


def _nav_rows(start_nav: int, end_onav: int, count: int = 30) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for idx in range(count):
        nav = start_nav + idx * 100
        onav = nav + 100
        rows.append(
            {
                "epoch": idx + 1,
                "nav": str(nav),
                "onav": str(onav),
                "created": f"2026-02-{idx + 1:02d}T18:00:00.000Z",
                "updated": f"2026-02-{idx + 2:02d}T18:00:00.000Z",
            }
        )
    rows[-1]["onav"] = str(end_onav)
    return rows


def test_get_token_apy_prefers_current_apy_series_and_caches() -> None:
    client = _StubClient(
        [
            {
                "data": {
                    "vaults_short_detail": [
                        {
                            "symbol": "bravUSDC",
                            "apy_series": [
                                {"epoch": 1, "apy": 0},
                                {"epoch": 2, "apy": 13.2033812794633},
                            ],
                        }
                    ]
                }
            }
        ]
    )
    oracle = BracketNavYieldOracle(
        graphql_url="https://app.bracket.fi/api/vaults/graphql",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    assert oracle.get_token_apy("wbravUSDC") == Decimal("0.132033812794633")
    assert oracle.get_token_apy("wbravUSDC") == Decimal("0.132033812794633")
    assert client.calls == 1
    assert client.requests[0] == {
        "query": (
            "query vaultsShortDetail{ vaults_short_detail {   symbol   apy_series { epoch apy } }}"
        ),
    }


def test_get_token_apy_raises_when_symbol_is_unknown() -> None:
    oracle = BracketNavYieldOracle(
        timeout_seconds=1.0,
        client=cast(httpx.Client, _StubClient([{"data": {"vaults_short_detail": []}}])),
    )

    with pytest.raises(RuntimeError, match="not configured"):
        oracle.get_token_apy("savUSD")


def test_get_token_apy_falls_back_to_nav_when_apy_series_is_missing() -> None:
    client = _StubClient(
        [
            {
                "data": {
                    "vaults_short_detail": [
                        {
                            "symbol": "bravUSDC",
                            "apy_series": [],
                        }
                    ]
                }
            },
            {
                "data": {
                    "vault": {
                        "navs": _nav_rows(1_000_000, 1_003_000),
                    }
                }
            },
        ]
    )
    oracle = BracketNavYieldOracle(
        graphql_url="https://app.bracket.fi/api/vaults/graphql",
        timeout_seconds=1.0,
        client=cast(httpx.Client, client),
    )

    assert oracle.get_token_apy("wbravUSDC") == Decimal("0.0365")
    assert client.calls == 2


def test_get_token_apy_raises_when_history_too_short() -> None:
    client = _StubClient(
        [
            {
                "data": {
                    "vaults_short_detail": [
                        {
                            "symbol": "bravUSDC",
                            "apy_series": [],
                        }
                    ]
                }
            },
            {
                "data": {
                    "vault": {
                        "navs": _nav_rows(1_000_000, 1_001_000, count=7),
                    }
                }
            },
        ]
    )
    oracle = BracketNavYieldOracle(timeout_seconds=1.0, client=cast(httpx.Client, client))

    with pytest.raises(RuntimeError, match="too short"):
        oracle.get_token_apy("wbravUSDC")
