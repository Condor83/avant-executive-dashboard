"""Morpho collateral carry APY override tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from adapters.morpho.adapter import (
    MorphoAdapter,
    MorphoMarketParams,
    MorphoMarketState,
    MorphoPosition,
)
from core.config import MarketsConfig
from core.yields import DefiLlamaYieldOracle


class _StubRpc:
    def close(self) -> None:  # pragma: no cover
        return None

    def get_block_number(self, chain_code: str) -> int:
        del chain_code
        return 1

    def get_position(
        self,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_address: str,
    ) -> MorphoPosition:
        del chain_code, morpho_address, market_id, wallet_address
        return MorphoPosition(
            supply_shares=300_000_000, borrow_shares=100_000_000, collateral=200_000_000
        )

    def get_market_state(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketState:
        del chain_code, morpho_address, market_id
        return MorphoMarketState(
            total_supply_assets=2_000_000_000,
            total_supply_shares=2_000_000_000,
            total_borrow_assets=500_000_000,
            total_borrow_shares=500_000_000,
            last_update=1,
            fee=0,
        )

    def get_market_params(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketParams:
        del chain_code, morpho_address, market_id
        return MorphoMarketParams(
            loan_token="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            collateral_token="0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
            oracle="0x0000000000000000000000000000000000000001",
            irm="0x0000000000000000000000000000000000000002",
            lltv=915000000000000000,
        )

    def get_irm_borrow_rate(
        self,
        chain_code: str,
        market_params: MorphoMarketParams,
        market_state: MorphoMarketState,
    ) -> int:
        del chain_code, market_params, market_state
        return 1_584_979_923

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        del chain_code, token_address
        return 6


class _StubYieldOracle:
    def __init__(self, apy: Decimal | None) -> None:
        self.apy = apy

    def close(self) -> None:
        return None

    def get_pool_apy(self, pool_id: str) -> Decimal:
        del pool_id
        if self.apy is None:
            raise RuntimeError("llama unavailable")
        return self.apy


def _config() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "morpho": {
                "ethereum": {
                    "morpho": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
                    "wallets": ["0x1491b385d4f80c524540b05a080179e5550ab0f9"],
                    "markets": [
                        {
                            "id": (
                                "0x729badf297ee9f2f6b3f717b96fd355fc6ec00422284ce1968e76647b258cf44"
                            ),
                            "loan_token": "USDC",
                            "collateral_token": "syrupUSDC",
                            "loan_decimals": 6,
                            "defillama_pool_id": "43641cf5-a92e-416b-bce9-27113d3c0db6",
                        }
                    ],
                    "vaults": [],
                }
            },
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_position_supply_apy_uses_configured_defillama_pool() -> None:
    adapter = MorphoAdapter(
        markets_config=_config(),
        rpc_client=_StubRpc(),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.078"))),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, position_issues = adapter.collect_positions(
        as_of_ts_utc=as_of, prices_by_token=prices
    )
    markets, market_issues = adapter.collect_markets(
        as_of_ts_utc=as_of,
        prices_by_token=prices,
    )

    assert not position_issues
    assert not market_issues
    assert len(positions) == 1
    assert len(markets) == 1

    assert positions[0].supply_apy == Decimal("0.078")
    assert positions[0].borrow_apy == markets[0].borrow_apy
    assert markets[0].supply_apy != Decimal("0.078")


def test_position_supply_apy_falls_back_to_protocol_on_defillama_error() -> None:
    adapter = MorphoAdapter(
        markets_config=_config(),
        rpc_client=_StubRpc(),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(None)),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)
    markets, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert not market_issues
    assert len(positions) == 1
    assert len(markets) == 1
    assert positions[0].supply_apy == markets[0].supply_apy
    assert any(issue.error_type == "morpho_collateral_apy_fallback_failed" for issue in issues)
