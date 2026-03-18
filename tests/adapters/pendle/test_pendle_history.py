"""Pendle history client parsing tests."""

from __future__ import annotations

from decimal import Decimal

from adapters.pendle.history import PendleHistoryClient


def test_parse_market_accepts_full_timestamp_expiry() -> None:
    client = PendleHistoryClient()

    parsed = client._parse_market(
        {
            "chainId": 1,
            "address": "0xf968b785b4bfd5a6c0fc197b42264beeecf58d85",
            "name": "avUSD",
            "expiry": "2026-05-14T00:00:00.000+00:00",
            "pt": "1-0xcc16cd49194e7aa3dca780c742580e2f9b418874",
            "yt": "1-0x5d928577454dfb826dd6163c75a487ab96032c2d",
            "sy": "1-0xaa6a8d538e36f23975beab59f5def2f19152d114",
            "underlyingAsset": "1-0xf4c13d631450de6b12a19829e37c8e2826891dc4",
            "details": {},
        }
    )

    assert parsed is not None
    assert parsed.expiry is not None
    assert parsed.expiry.isoformat() == "2026-05-14T00:00:00+00:00"
    client.close()


def test_parse_market_captures_pt_and_sy_pool_inventory() -> None:
    client = PendleHistoryClient()

    parsed = client._parse_market(
        {
            "chainId": 1,
            "address": "0xf968b785b4bfd5a6c0fc197b42264beeecf58d85",
            "name": "avUSD",
            "expiry": "2026-05-14T00:00:00.000Z",
            "pt": "1-0xcc16cd49194e7aa3dca780c742580e2f9b418874",
            "yt": "1-0x5d928577454dfb826dd6163c75a487ab96032c2d",
            "sy": "1-0xaa6a8d538e36f23975beab59f5def2f19152d114",
            "underlyingAsset": "1-0xf4c13d631450de6b12a19829e37c8e2826891dc4",
            "details": {
                "totalPt": 488676.15919477417,
                "totalSy": 1899012.0702663884,
            },
        }
    )

    assert parsed is not None
    assert parsed.total_pt == Decimal("488676.15919477417")
    assert parsed.total_sy == Decimal("1899012.0702663884")
    client.close()
