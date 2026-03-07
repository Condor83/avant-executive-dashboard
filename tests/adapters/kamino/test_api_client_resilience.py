"""Kamino API client resilience tests."""

from __future__ import annotations

from decimal import Decimal

import httpx

from adapters.kamino.adapter import KaminoApiClient


def test_get_user_obligations_retries_timeout_once() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json=[])

    client = KaminoApiClient(
        base_url="https://api.kamino.finance",
        timeout_seconds=1.0,
        max_attempts=2,
        backoff_seconds=0.0,
    )
    original_client = client._client
    try:
        client._client = httpx.Client(transport=httpx.MockTransport(handler), timeout=1.0)
        original_client.close()
        obligations = client.get_user_obligations(
            chain_code="solana",
            market_pubkey="6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y",
            wallet_address="29KnJ9mtSMbxAFExns4H8e5Tv9AqSb1Qazy1HhHdUbNH",
        )
    finally:
        client.close()

    assert attempts == 2
    assert obligations == []


def test_get_market_stats_tolerates_metrics_history_failure(monkeypatch) -> None:
    client = KaminoApiClient(base_url="https://api.kamino.finance", timeout_seconds=1.0)
    try:

        def fake_get_json(path: str) -> object:
            if path.endswith("/reserves/metrics"):
                return [
                    {
                        "reserve": "reserve-1",
                        "liquidityToken": "syrupUSDC",
                        "liquidityTokenMint": "mint-1",
                        "totalSupplyUsd": "100",
                        "totalBorrowUsd": "40",
                        "supplyApy": "5",
                        "borrowApy": "10",
                    }
                ]
            if path.endswith("/metrics/history"):
                raise RuntimeError("history timeout")
            raise AssertionError(path)

        monkeypatch.setattr(client, "_get_json", fake_get_json)
        stats = client.get_market_stats(
            chain_code="solana",
            market_pubkey="6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y",
        )
    finally:
        client.close()

    assert stats.total_supply_usd == Decimal("100")
    assert stats.total_borrow_usd == Decimal("40")
    assert stats.supply_apy == Decimal("0.05")
    assert stats.borrow_apy == Decimal("0.1")
    assert stats.slot is None
    assert stats.raw_payload is not None
    assert stats.raw_payload.get("history_error") == "history timeout"


def test_get_user_obligations_derives_health_factor_from_liquidation_limit(monkeypatch) -> None:
    client = KaminoApiClient(base_url="https://api.kamino.finance", timeout_seconds=1.0)
    try:

        def fake_get_json(path: str) -> object:
            assert path.endswith(
                "/kamino-market/6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y/users/29KnJ9mtSMbxAFExns4H8e5Tv9AqSb1Qazy1HhHdUbNH/obligations"
            )
            return [
                {
                    "obligationAddress": "obligation-1",
                    "refreshedStats": {
                        "userTotalDeposit": "20078649.932098440928",
                        "userTotalBorrow": "15589850.437096009937",
                        "borrowLiquidationLimit": "18070784.938888596835",
                        "loanToValue": "0.8540830951682102467",
                    },
                    "state": {
                        "deposits": [],
                        "borrows": [],
                    },
                }
            ]

        monkeypatch.setattr(client, "_get_json", fake_get_json)
        obligations = client.get_user_obligations(
            chain_code="solana",
            market_pubkey="6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y",
            wallet_address="29KnJ9mtSMbxAFExns4H8e5Tv9AqSb1Qazy1HhHdUbNH",
        )
    finally:
        client.close()

    assert len(obligations) == 1
    assert obligations[0].health_factor == Decimal("1.159137800057992184818938528")
