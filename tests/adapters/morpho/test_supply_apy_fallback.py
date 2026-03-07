"""Morpho collateral carry APY override tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from adapters.bracket import BracketNavYieldOracle
from adapters.morpho.adapter import (
    MorphoAdapter,
    MorphoMarketParams,
    MorphoMarketState,
    MorphoPosition,
)
from core.config import MarketsConfig
from core.yields import AvantYieldOracle, DefiLlamaYieldOracle


class _StubRpc:
    def __init__(
        self,
        *,
        collateral_token_address: str = "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
        collateral_decimals: int = 6,
    ) -> None:
        self.collateral_token_address = collateral_token_address
        self.collateral_decimals = collateral_decimals

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
            collateral_token=self.collateral_token_address,
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
        return self.collateral_decimals

    def get_erc20_balance(self, chain_code: str, token_address: str, wallet_address: str) -> int:
        del chain_code, token_address, wallet_address
        return 0

    def get_vault_asset(self, chain_code: str, vault_address: str) -> str:
        del chain_code, vault_address
        return "0x0000000000000000000000000000000000000000"

    def convert_to_assets(self, chain_code: str, vault_address: str, shares: int) -> int:
        del chain_code, vault_address
        return shares


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


class _StubAvantYieldOracle:
    def __init__(self, apy: Decimal | None, *, error: Exception | None = None) -> None:
        self.apy = apy
        self.error = error
        self.calls: list[str] = []

    def close(self) -> None:
        return None

    def get_token_apy(self, symbol: str) -> Decimal:
        self.calls.append(symbol)
        if self.error is not None:
            raise self.error
        if self.apy is None:
            raise RuntimeError("avant unavailable")
        return self.apy


class _StubBracketYieldOracle:
    def __init__(self, apy: Decimal | None, *, error: Exception | None = None) -> None:
        self.apy = apy
        self.error = error
        self.calls: list[str] = []

    def close(self) -> None:
        return None

    @staticmethod
    def supports_token(symbol: str) -> bool:
        return symbol.strip().upper() == "WBRAVUSDC"

    def get_token_apy(self, symbol: str) -> Decimal:
        self.calls.append(symbol)
        if self.error is not None:
            raise self.error
        if self.apy is None:
            raise RuntimeError("bracket unavailable")
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
                            "loan_token_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                            "collateral_token": "syrupUSDC",
                            "collateral_token_address": (
                                "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"
                            ),
                            "loan_decimals": 6,
                            "collateral_decimals": 6,
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


def _config_without_pool_id(collateral_token: str = "syrupUSDC") -> MarketsConfig:
    collateral_addresses = {
        "syrupUSDC": "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b",
        "savUSD": "0xb8d89678e75a973e74698c976716308abb8a46a4",
        "USDe": "0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
        "wbravUSDC": "0x7309e1e2e74af170c69bde8fcb30397f8697d5ff",
        "PT-savUSD-14MAY2026": "0x606b5c773dc4d6e625c411cf60565f8c30c467d2",
    }
    collateral_decimals_by_symbol = {
        "syrupUSDC": 6,
        "savUSD": 18,
        "USDe": 18,
        "wbravUSDC": 6,
        "PT-savUSD-14MAY2026": 18,
    }
    collateral_address = collateral_addresses[collateral_token]
    collateral_decimals = collateral_decimals_by_symbol[collateral_token]
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
                                "0xe07d416323a1afbfe0bf2fe27ffb549ff565cf5c86d21b79fc60664038e597c9"
                            ),
                            "loan_token": "USDC",
                            "loan_token_address": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                            "collateral_token": collateral_token,
                            "collateral_token_address": collateral_address,
                            "loan_decimals": 6,
                            "collateral_decimals": collateral_decimals,
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


def _config_with_pool_id(collateral_token: str, pool_id: str) -> MarketsConfig:
    payload = _config_without_pool_id(collateral_token=collateral_token).model_dump()
    payload["morpho"]["ethereum"]["markets"][0]["defillama_pool_id"] = pool_id
    return MarketsConfig.model_validate(payload)


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

    assert positions[0].supplied_amount == Decimal("300")
    assert positions[0].supplied_usd == Decimal("300")
    assert positions[0].collateral_amount == Decimal("200")
    assert positions[0].collateral_usd == Decimal("200")
    assert positions[0].borrowed_amount == Decimal("100")
    assert positions[0].borrowed_usd == Decimal("100")
    assert positions[0].equity_usd == Decimal("400")
    assert positions[0].supply_apy == Decimal("0.078")
    assert positions[0].borrow_apy == markets[0].borrow_apy
    assert markets[0].supply_apy != Decimal("0.078")


def test_position_supply_apy_uses_avant_api_for_avant_native_collateral() -> None:
    avant_oracle = _StubAvantYieldOracle(Decimal("0.0745"))
    adapter = MorphoAdapter(
        markets_config=_config_without_pool_id(collateral_token="savUSD"),
        rpc_client=_StubRpc(
            collateral_token_address="0xb8d89678e75a973e74698c976716308abb8a46a4",
            collateral_decimals=18,
        ),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.041"))),
        avant_yield_oracle=cast(AvantYieldOracle, avant_oracle),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0xb8d89678e75a973e74698c976716308abb8a46a4"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    assert positions[0].supply_apy == Decimal("0.0745")
    assert avant_oracle.calls == ["savUSD"]
    assert not issues


def test_position_supply_apy_uses_bracket_nav_for_wbravusdc_collateral() -> None:
    bracket_oracle = _StubBracketYieldOracle(Decimal("0.0391930337"))
    adapter = MorphoAdapter(
        markets_config=_config_without_pool_id(collateral_token="wbravUSDC"),
        rpc_client=_StubRpc(
            collateral_token_address="0x7309e1e2e74af170c69bde8fcb30397f8697d5ff",
            collateral_decimals=6,
        ),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.041"))),
        bracket_yield_oracle=cast(BracketNavYieldOracle, bracket_oracle),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x7309e1e2e74af170c69bde8fcb30397f8697d5ff"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    assert positions[0].supply_apy == Decimal("0.0391930337")
    assert bracket_oracle.calls == ["wbravUSDC"]
    assert not issues


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
    assert positions[0].collateral_amount == Decimal("200")
    assert positions[0].collateral_usd == Decimal("200")
    assert positions[0].supply_apy == markets[0].supply_apy
    assert any(issue.error_type == "morpho_collateral_apy_fallback_failed" for issue in issues)


def test_position_supply_apy_falls_back_to_defillama_when_avant_api_fails() -> None:
    adapter = MorphoAdapter(
        markets_config=_config_with_pool_id(
            collateral_token="savUSD",
            pool_id="avant-savusd-pool",
        ),
        rpc_client=_StubRpc(
            collateral_token_address="0xb8d89678e75a973e74698c976716308abb8a46a4",
            collateral_decimals=18,
        ),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.078"))),
        avant_yield_oracle=cast(
            AvantYieldOracle,
            _StubAvantYieldOracle(None, error=RuntimeError("avant unavailable")),
        ),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0xb8d89678e75a973e74698c976716308abb8a46a4"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    assert positions[0].supply_apy == Decimal("0.078")
    assert any(
        issue.error_type == "morpho_collateral_apy_fallback_failed"
        and issue.payload_json
        and issue.payload_json.get("source") == "avant_api"
        for issue in issues
    )


def test_position_emits_dq_when_yield_bearing_collateral_lacks_carry_source() -> None:
    adapter = MorphoAdapter(
        markets_config=_config_without_pool_id(),
        rpc_client=_StubRpc(),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.078"))),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b"): Decimal("1"),
        ("ethereum", "0xb8d89678e75a973e74698c976716308abb8a46a4"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    assert any(issue.error_type == "morpho_collateral_apy_source_missing" for issue in issues)


def test_pt_collateral_does_not_emit_generic_missing_carry_source() -> None:
    adapter = MorphoAdapter(
        markets_config=_config_without_pool_id(collateral_token="PT-savUSD-14MAY2026"),
        rpc_client=_StubRpc(
            collateral_token_address="0x606b5c773dc4d6e625c411cf60565f8c30c467d2",
            collateral_decimals=18,
        ),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.078"))),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x606b5c773dc4d6e625c411cf60565f8c30c467d2"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    assert not any(issue.error_type == "morpho_collateral_apy_source_missing" for issue in issues)


def test_plain_usde_collateral_does_not_emit_missing_carry_source() -> None:
    adapter = MorphoAdapter(
        markets_config=_config_without_pool_id(collateral_token="USDe"),
        rpc_client=_StubRpc(
            collateral_token_address="0x4c9edd5852cd905f086c759e8383e09bff1e68b3",
            collateral_decimals=18,
        ),
        yield_oracle=cast(DefiLlamaYieldOracle, _StubYieldOracle(Decimal("0.078"))),
        avant_yield_oracle=cast(AvantYieldOracle, _StubAvantYieldOracle(None)),
    )

    prices = {
        ("ethereum", "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"): Decimal("1"),
        ("ethereum", "0x4c9edd5852cd905f086c759e8383e09bff1e68b3"): Decimal("1"),
    }
    as_of = datetime(2026, 3, 3, 0, 0, tzinfo=UTC)

    positions, issues = adapter.collect_positions(as_of_ts_utc=as_of, prices_by_token=prices)
    markets, market_issues = adapter.collect_markets(as_of_ts_utc=as_of, prices_by_token=prices)

    assert len(positions) == 1
    assert len(markets) == 1
    assert not market_issues
    assert positions[0].supply_apy == markets[0].supply_apy
    assert not any(issue.error_type == "morpho_collateral_apy_source_missing" for issue in issues)
