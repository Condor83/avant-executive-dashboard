"""DefiLlama pricing tests recorded with VCR."""

from __future__ import annotations

from datetime import UTC, datetime

import vcr  # type: ignore[import-untyped]

from core.pricing import PriceOracle
from core.types import PriceRequest


def test_defillama_prices_vcr() -> None:
    recorder = vcr.VCR(
        cassette_library_dir="tests/pricing/cassettes",
        record_mode="once",
        match_on=["method", "scheme", "host", "port", "path", "query"],
    )

    oracle = PriceOracle(base_url="https://coins.llama.fi", timeout_seconds=20)
    try:
        with recorder.use_cassette("defillama_usdc_price.yaml"):
            result = oracle.fetch_prices(
                [
                    PriceRequest(
                        token_id=1,
                        chain_code="ethereum",
                        address_or_mint="0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                        symbol="USDC",
                    )
                ],
                as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
            )

        assert not result.issues
        assert len(result.quotes) == 1
        assert result.quotes[0].price_usd > 0
    finally:
        oracle.close()
