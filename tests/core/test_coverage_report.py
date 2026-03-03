"""Coverage report helper tests."""

from pathlib import Path

from core.config import load_markets_config
from core.coverage_report import expected_coverage_from_config


def test_expected_coverage_from_config_has_adapter_protocols() -> None:
    markets = load_markets_config(Path("config/markets.yaml"))
    coverage = expected_coverage_from_config(markets)

    assert set(coverage.keys()) == {"morpho", "euler_v2", "dolomite"}

    assert coverage["morpho"].expected_wallet_market_pairs > 0
    assert coverage["morpho"].expected_markets > 0
    assert coverage["euler_v2"].expected_wallet_market_pairs > 0
    assert coverage["euler_v2"].expected_markets > 0
    assert coverage["dolomite"].expected_wallet_market_pairs > 0
    assert coverage["dolomite"].expected_markets > 0
