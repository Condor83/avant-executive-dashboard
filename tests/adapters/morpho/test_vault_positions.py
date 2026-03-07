"""Morpho vault position ingestion tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from adapters.morpho.adapter import (
    MorphoAdapter,
    MorphoMarketParams,
    MorphoMarketState,
    MorphoPosition,
)
from adapters.morpho.vault_yields import MorphoVaultApyQuote
from core.config import MarketsConfig


class _StubMorphoClient:
    def close(self) -> None:
        return None

    def get_block_number(self, chain_code: str) -> int:
        assert chain_code == "ethereum"
        return 123

    def get_position(
        self,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_address: str,
    ) -> MorphoPosition:
        del chain_code, morpho_address, market_id, wallet_address
        return MorphoPosition(supply_shares=0, borrow_shares=0, collateral=0)

    def get_market_state(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketState:
        del chain_code, morpho_address, market_id
        return MorphoMarketState(
            total_supply_assets=0,
            total_supply_shares=0,
            total_borrow_assets=0,
            total_borrow_shares=0,
            last_update=0,
            fee=0,
        )

    def get_market_params(
        self, chain_code: str, morpho_address: str, market_id: str
    ) -> MorphoMarketParams:
        del chain_code, morpho_address, market_id
        return MorphoMarketParams(
            loan_token="0x0000000000000000000000000000000000000000",
            collateral_token="0x0000000000000000000000000000000000000000",
            oracle="0x0000000000000000000000000000000000000000",
            irm="0x0000000000000000000000000000000000000000",
            lltv=0,
        )

    def get_irm_borrow_rate(
        self,
        chain_code: str,
        market_params: MorphoMarketParams,
        market_state: MorphoMarketState,
    ) -> int:
        del chain_code, market_params, market_state
        return 0

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        del chain_code, token_address
        return 6

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        assert chain_code == "ethereum"
        assert token_address == "0x951a9f4a2ce19b9dea6b37e691d076a345b6c0f8"
        assert wallet_address == "0x1111111111111111111111111111111111111111"
        return 2_000_000

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        del chain_code, vault_address
        return "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        assert chain_code == "ethereum"
        assert vault_address == "0x951a9f4a2ce19b9dea6b37e691d076a345b6c0f8"
        return shares


class _StubVaultYieldClient:
    def __init__(self, quote: MorphoVaultApyQuote | None = None, error: Exception | None = None):
        self.quote = quote
        self.error = error
        self.calls: list[tuple[str, int, str]] = []

    def close(self) -> None:
        return None

    def get_vault_apy(self, *, address: str, chain_id: int, lookback: str) -> MorphoVaultApyQuote:
        self.calls.append((address, chain_id, lookback))
        if self.error is not None:
            raise self.error
        assert self.quote is not None
        return self.quote


def _config() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "spark": {},
            "morpho": {
                "ethereum": {
                    "morpho": "0xbbbbbbbbbb9cc5e90e3b3af64bdaf62c37eeffcb",
                    "wallets": ["0x1111111111111111111111111111111111111111"],
                    "markets": [],
                    "vaults": [
                        {
                            "address": "0x951a9f4A2cE19B9DeA6B37e691d076A345b6c0F8",
                            "note": "MetaMorpho vault",
                            "asset_address": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
                            "asset_symbol": "USDC",
                            "asset_decimals": 6,
                            "chain_id": 1,
                            "apy_source": "morpho_api",
                            "apy_lookback": "SIX_HOURS",
                        }
                    ],
                }
            },
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_morpho_vault_positions_are_ingested_as_supply() -> None:
    vault_yield_client = _StubVaultYieldClient(
        quote=MorphoVaultApyQuote(
            net_apy=Decimal("0.10660328634479907"),
            base_apy_excluding_rewards=Decimal("0.10352315677551797"),
            reward_apy=Decimal("0.003080129569281099"),
            lookback="SIX_HOURS",
        )
    )
    adapter = MorphoAdapter(
        markets_config=_config(),
        rpc_client=_StubMorphoClient(),
        vault_yield_client=vault_yield_client,
    )
    as_of = datetime(2026, 3, 4, 0, 0, tzinfo=UTC)

    prices = {("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1")}
    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert not issues
    assert len(positions) == 1
    position = positions[0]
    assert position.market_ref == "0x951a9f4a2ce19b9dea6b37e691d076a345b6c0f8"
    assert position.supplied_amount == Decimal("2")
    assert position.supplied_usd == Decimal("2")
    assert position.borrowed_amount == Decimal("0")
    assert position.borrowed_usd == Decimal("0")
    assert position.equity_usd == Decimal("2")
    assert position.supply_apy == Decimal("0.10352315677551797")
    assert position.reward_apy == Decimal("0.003080129569281099")
    assert position.borrow_apy == Decimal("0")
    assert position.ltv is None
    assert vault_yield_client.calls == [
        ("0x951a9f4a2ce19b9dea6b37e691d076a345b6c0f8", 1, "SIX_HOURS")
    ]


def test_morpho_vault_positions_emit_dq_when_apy_fetch_fails() -> None:
    adapter = MorphoAdapter(
        markets_config=_config(),
        rpc_client=_StubMorphoClient(),
        vault_yield_client=_StubVaultYieldClient(error=RuntimeError("morpho unavailable")),
    )
    as_of = datetime(2026, 3, 4, 0, 0, tzinfo=UTC)

    prices = {("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1")}
    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    position = positions[0]
    assert position.supply_apy == Decimal("0")
    assert position.reward_apy == Decimal("0")
    assert position.borrow_apy == Decimal("0")
    assert any(issue.error_type == "morpho_vault_apy_fetch_failed" for issue in issues)
