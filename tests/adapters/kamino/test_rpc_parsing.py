"""Solana account parsing unit tests for Kamino support code."""

from __future__ import annotations

import base64

from core.solana_client import decode_base64_account_data, parse_u64_le


def test_decode_base64_account_data_from_rpc_tuple() -> None:
    payload = base64.b64encode(b"kamino-state").decode()
    decoded = decode_base64_account_data([payload, "base64"])
    assert decoded == b"kamino-state"


def test_parse_u64_little_endian() -> None:
    raw = (123_456_789).to_bytes(8, "little") + b"\x00\x01"
    assert parse_u64_le(raw, 0) == 123_456_789
