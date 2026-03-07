"""Unit tests for Pendle history helpers."""

from __future__ import annotations

from decimal import Decimal
from typing import cast

import httpx

from adapters.pendle import PendleHistoryClient


class _StubResponse:
    def __init__(self, payload: object) -> None:
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> object:
        return self.payload


class _StubClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object] | None]] = []

    def get(
        self,
        url: str,
        *,
        params: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
    ) -> _StubResponse:
        del headers
        self.calls.append((url, params))
        if url.endswith("/v1/markets/all"):
            return _StubResponse(
                {
                    "markets": [
                        {
                            "address": "0xmarket1",
                            "pt": "1-0xpttoken",
                        },
                        {
                            "address": "0xmarket2",
                            "pt": "1-0xothertoken",
                        },
                    ]
                }
            )
        if params and params.get("resumeToken") == "next-page":
            return _StubResponse(
                {
                    "total": 2,
                    "limit": 1000,
                    "skip": 0,
                    "results": [
                        {
                            "market": "0xmarket1",
                            "action": "SELL_PT",
                            "timestamp": "2026-03-03T00:00:00Z",
                            "impliedApy": 0.07,
                            "notional": {"pt": 25},
                        }
                    ],
                }
            )
        return _StubResponse(
            {
                "total": 2,
                "limit": 1000,
                "skip": 0,
                "resumeToken": "next-page",
                "results": [
                    {
                        "market": "0xmarket1",
                        "action": "BUY_PT",
                        "timestamp": "2026-03-01T00:00:00Z",
                        "impliedApy": 0.05,
                        "notional": {"pt": 100},
                    }
                ],
            }
        )

    def close(self) -> None:
        return None


def test_get_market_addresses_for_pt_normalizes_and_caches() -> None:
    client = _StubClient()
    history = PendleHistoryClient(timeout_seconds=1.0, client=cast(httpx.Client, client))

    assert history.get_market_addresses_for_pt(chain_id=1, pt_token_address="0xpttoken") == {
        "0xmarket1"
    }
    assert history.get_market_addresses_for_pt(chain_id=1, pt_token_address="0xPTTOKEN") == {
        "0xmarket1"
    }
    market_calls = [call for call in client.calls if call[0].endswith("/v1/markets/all")]
    assert len(market_calls) == 1


def test_get_wallet_trades_paginates_and_normalizes() -> None:
    client = _StubClient()
    history = PendleHistoryClient(timeout_seconds=1.0, client=cast(httpx.Client, client))

    trades = history.get_wallet_trades(chain_id=1, wallet_address="0xabc")

    assert [trade.action for trade in trades] == ["BUY_PT", "SELL_PT"]
    assert trades[0].market_address == "0xmarket1"
    assert trades[0].implied_apy == Decimal("0.05")
    assert trades[0].pt_notional == Decimal("100")
    tx_calls = [call for call in client.calls if "/transactions/" in call[0]]
    assert len(tx_calls) == 2
