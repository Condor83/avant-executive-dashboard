"""PT fixed-yield override config parsing tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from core.config import load_pt_fixed_yield_overrides_config


def test_load_pt_fixed_yield_overrides_config_reads_repo_file() -> None:
    overrides = load_pt_fixed_yield_overrides_config(Path("config/pt_fixed_yield_overrides.yaml"))

    assert len(overrides) == 3
    first = overrides[
        "morpho:ethereum:0x1491b385d4f80c524540b05a080179e5550ab0f9:"
        "0x6a8f296524b7f300b56e3653bbfc0ce4dd0dff225f1d5ec09c4fd190886ae53e"
    ]
    assert str(first.fixed_apy) == "0.0854"
    assert first.source == "pendle_manual"


def test_load_pt_fixed_yield_overrides_config_rejects_duplicates(tmp_path: Path) -> None:
    path = tmp_path / "pt_fixed_yield_overrides.yaml"
    path.write_text(
        """
overrides:
  - position_key: "same"
    fixed_apy: "0.1"
    source: "etherscan_manual"
    tx_hash: "0x1"
    acquired_at_utc: "2026-01-01T00:00:00Z"
  - position_key: "same"
    fixed_apy: "0.2"
    source: "etherscan_manual"
    tx_hash: "0x2"
    acquired_at_utc: "2026-01-02T00:00:00Z"
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="duplicate PT fixed-yield override"):
        load_pt_fixed_yield_overrides_config(path)
