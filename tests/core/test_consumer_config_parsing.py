"""Consumer-specific config parsing assertions."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

from core.config import (
    load_avant_tokens_config,
    load_consumer_thresholds_config,
    load_holder_exclusions_config,
)


def test_avant_tokens_yaml_parses_expected_registry_fields() -> None:
    config = load_avant_tokens_config(Path("config/avant_tokens.yaml"))

    assert len(config.tokens) >= 4
    wbr = next(token for token in config.tokens if token.symbol == "wbravUSDC")
    assert wbr.chain_code == "ethereum"
    assert wbr.wrapper_class == "boosted"
    assert wbr.pricing_policy == "convert_to_assets"
    assert wbr.underlying_token_address == "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"


def test_consumer_thresholds_yaml_parses_expected_defaults() -> None:
    config = load_consumer_thresholds_config(Path("config/consumer_thresholds.yaml"))

    assert config.verified_min_total_avant_usd == 100
    assert config.cohort_min_total_avant_usd == 50000
    assert config.classification_dust_floor_usd == 100
    assert config.capacity.utilization_threshold == Decimal("0.85")
    assert config.capacity.review_score_threshold == 2
    assert config.risk_bands.critical_health_factor_lt == Decimal("1.05")
    assert config.whales.top_assets_count == 25
    assert config.whales.leaderboard_count == 10
    assert config.whales.wallet_usd_threshold == Decimal("1000000")
    assert config.supply_coverage.primary_chain_code == "avalanche"
    assert config.supply_coverage.primary_token_symbol == "savUSD"


def test_holder_exclusions_yaml_parses_expected_defaults() -> None:
    config = load_holder_exclusions_config(Path("config/holder_exclusions.yaml"))

    assert len(config.exclusions) == 1
    exclusion = config.exclusions[0]
    assert exclusion.address == "0x8fcc42c414e29e8e3dbfa1628cf45e8ed80c999d"
    assert exclusion.chain_code == "avalanche"
    assert exclusion.classification == "infrastructure"
    assert exclusion.exclude_from_monitoring is True
    assert exclusion.exclude_from_customer_float is False
