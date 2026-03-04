"""Tests for DeBank Cloud HTTP client behavior."""

from __future__ import annotations

import httpx
import pytest

from core.debank_cloud import DebankCloudClient, DebankResponseError


def test_get_user_complex_protocols_uses_access_key_header_and_query_params() -> None:
    seen_access_keys: list[str] = []
    observed_query: list[tuple[str, str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_access_keys.append(request.headers.get("AccessKey", ""))
        observed_query.extend(list(request.url.params.multi_items()))
        assert request.url.path == "/v1/user/all_complex_protocol_list"
        return httpx.Response(200, json=[{"id": "aave_v3"}])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    debank = DebankCloudClient(
        base_url="https://pro-openapi.debank.com",
        api_key="test-key",
        client=client,
        sleep_fn=lambda _: None,
        jitter_fn=lambda: 0.0,
    )
    try:
        result = debank.get_user_complex_protocols(
            "0x1111111111111111111111111111111111111111",
            chain_ids=["eth", "base"],
        )
    finally:
        debank.close()

    assert result == [{"id": "aave_v3"}]
    assert seen_access_keys == ["test-key"]
    assert ("id", "0x1111111111111111111111111111111111111111") in observed_query
    assert ("chain_ids", "eth,base") in observed_query


def test_retries_on_retryable_statuses() -> None:
    attempt = 0
    sleep_calls: list[float] = []

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempt
        del request
        attempt += 1
        if attempt < 3:
            return httpx.Response(503, text="temporarily unavailable")
        return httpx.Response(200, json=[{"id": "spark"}])

    client = httpx.Client(transport=httpx.MockTransport(handler))
    debank = DebankCloudClient(
        base_url="https://pro-openapi.debank.com",
        api_key="test-key",
        client=client,
        sleep_fn=lambda seconds: sleep_calls.append(seconds),
        jitter_fn=lambda: 0.0,
    )
    try:
        payload = debank.get_user_complex_protocols("0x1111111111111111111111111111111111111111")
    finally:
        debank.close()

    assert attempt == 3
    assert len(sleep_calls) == 2
    assert payload == [{"id": "spark"}]


def test_raises_for_non_retryable_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        del request
        return httpx.Response(401, text="unauthorized")

    client = httpx.Client(transport=httpx.MockTransport(handler))
    debank = DebankCloudClient(
        base_url="https://pro-openapi.debank.com",
        api_key="test-key",
        client=client,
        sleep_fn=lambda _: None,
        jitter_fn=lambda: 0.0,
    )
    try:
        with pytest.raises(DebankResponseError, match="status=401"):
            debank.get_user_complex_protocols("0x1111111111111111111111111111111111111111")
    finally:
        debank.close()
