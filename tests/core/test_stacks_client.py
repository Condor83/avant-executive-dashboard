"""Unit tests for Stacks API response parsing."""

from __future__ import annotations

from core.stacks_client import StacksClient


class _StubStacksClient(StacksClient):
    def __init__(self, payload: object) -> None:
        super().__init__(base_url="https://api.hiro.so")
        self._payload = payload

    def _get(self, path: str, params: object | None = None) -> object:
        del path, params
        return self._payload


def test_get_block_height_accepts_paginated_hiro_shape() -> None:
    client = _StubStacksClient(
        {
            "limit": 20,
            "offset": 0,
            "total": 1,
            "results": [{"height": 6930948}],
        }
    )
    assert client.get_block_height() == 6930948
    client.close()
