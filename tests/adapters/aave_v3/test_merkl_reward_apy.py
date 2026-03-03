"""Merkl reward APY behavior tests for Aave v3 USDe/sUSDe loops."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from adapters.aave_v3.adapter import (
    AaveV3Adapter,
    ReserveCaps,
    ReserveData,
    UserAccountData,
    UserReserveData,
    _MerklRewardContext,
)
from core.config import MarketsConfig, canonical_address

USDE = "0x4444444444444444444444444444444444444444"
SUSDE = "0x5555555555555555555555555555555555555555"
USDT = "0x6666666666666666666666666666666666666666"


@dataclass(frozen=True)
class _StubRpc:
    @staticmethod
    def _to_ray(rate: str) -> int:
        return int((Decimal(rate) * Decimal("1e27")).to_integral_value())

    def close(self) -> None:  # pragma: no cover
        return

    def get_block_number(self, chain_code: str) -> int:
        del chain_code
        return 123

    def get_user_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
        wallet_address: str,
    ) -> UserReserveData:
        del chain_code, pool_data_provider, wallet_address
        market_ref = canonical_address(asset)
        if market_ref == canonical_address(USDE):
            return UserReserveData(
                current_a_token_balance=10_000_000_000_000_000_000,
                current_stable_debt=0,
                current_variable_debt=0,
            )
        if market_ref == canonical_address(SUSDE):
            return UserReserveData(
                current_a_token_balance=20_000_000_000_000_000_000,
                current_stable_debt=0,
                current_variable_debt=0,
            )
        if market_ref == canonical_address(USDT):
            return UserReserveData(
                current_a_token_balance=0,
                current_stable_debt=0,
                current_variable_debt=15_000_000,
            )
        return UserReserveData(
            current_a_token_balance=0,
            current_stable_debt=0,
            current_variable_debt=0,
        )

    def get_user_account_data(
        self,
        chain_code: str,
        pool: str,
        wallet_address: str,
    ) -> UserAccountData:
        del chain_code, pool, wallet_address
        return UserAccountData(
            ltv_bps=7_500,
            health_factor_wad=int(Decimal("1.6") * Decimal("1e18")),
        )

    def get_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveData:
        del chain_code, pool_data_provider
        market_ref = canonical_address(asset)
        liquidity_rate = "0.01"
        if market_ref == canonical_address(SUSDE):
            liquidity_rate = "0.035"
        return ReserveData(
            total_a_token=1_000_000_000_000_000_000_000_000,
            total_stable_debt=0,
            total_variable_debt=500_000_000_000_000_000_000_000,
            liquidity_rate_ray=self._to_ray(liquidity_rate),
            variable_borrow_rate_ray=self._to_ray("0.03"),
        )

    def get_reserve_caps(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveCaps:
        del chain_code, pool_data_provider, asset
        return ReserveCaps(borrow_cap=0, supply_cap=0)


def _markets() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {
                "ethereum": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": ["0x3333333333333333333333333333333333333333"],
                    "markets": [
                        {"symbol": "USDe", "asset": USDE, "decimals": 18},
                        {"symbol": "sUSDe", "asset": SUSDE, "decimals": 18},
                        {"symbol": "USDT", "asset": USDT, "decimals": 6},
                    ],
                }
            },
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def _price_map() -> dict[tuple[str, str], Decimal]:
    return {
        ("ethereum", canonical_address(USDE)): Decimal("1"),
        ("ethereum", canonical_address(SUSDE)): Decimal("1"),
        ("ethereum", canonical_address(USDT)): Decimal("1"),
    }


def _reward_context() -> _MerklRewardContext:
    return _MerklRewardContext(
        reward_apy=Decimal("0.0356"),
        opportunity_id="op_1",
        identifier="id_1",
        name="Lend sUSDe and USDe on Aave (looping required)",
    )


def test_parse_merkl_reward_apy_prefers_max_apr() -> None:
    assert AaveV3Adapter._parse_merkl_reward_apy({"maxApr": 0.0356, "apr": 4.49}) == Decimal(
        "0.0356"
    )


def test_parse_merkl_reward_apy_falls_back_to_apr_percent() -> None:
    assert AaveV3Adapter._parse_merkl_reward_apy({"apr": 4.49}) == Decimal("0.0449")


def test_positions_apply_merkl_reward_to_usde_and_susde(monkeypatch) -> None:
    adapter = AaveV3Adapter(
        markets_config=_markets(),
        rpc_client=_StubRpc(),
        merkl_base_url="https://api.merkl.xyz",
    )
    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _reward_context)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(),
    )

    assert not issues
    assert len(positions) == 3
    by_market = {position.market_ref: position for position in positions}
    usde = by_market[canonical_address(USDE)]
    susde = by_market[canonical_address(SUSDE)]
    usdt = by_market[canonical_address(USDT)]

    assert usde.reward_apy == Decimal("0.0356")
    assert susde.reward_apy < usde.reward_apy
    assert usdt.reward_apy == Decimal("0")
    # Effective total supply yield is intentionally aligned between USDe and sUSDe.
    assert usde.supply_apy + usde.reward_apy == susde.supply_apy + susde.reward_apy


def test_merkl_fetch_failure_records_issue_and_keeps_reward_zero(monkeypatch) -> None:
    adapter = AaveV3Adapter(
        markets_config=_markets(),
        rpc_client=_StubRpc(),
        merkl_base_url="https://api.merkl.xyz",
    )

    def _raise_merkl_error() -> _MerklRewardContext:
        raise RuntimeError("Merkl unavailable")

    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _raise_merkl_error)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(),
    )

    assert any(issue.error_type == "aave_merkl_reward_apy_fetch_failed" for issue in issues)
    rewards_by_market = {position.market_ref: position.reward_apy for position in positions}
    assert rewards_by_market[canonical_address(USDE)] == Decimal("0")
    assert rewards_by_market[canonical_address(SUSDE)] == Decimal("0")


def test_market_metadata_includes_merkl_context_for_usde_and_susde(monkeypatch) -> None:
    adapter = AaveV3Adapter(
        markets_config=_markets(),
        rpc_client=_StubRpc(),
        merkl_base_url="https://api.merkl.xyz",
    )
    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _reward_context)

    snapshots, issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_price_map(),
    )

    assert not issues
    snapshots_by_market = {snapshot.market_ref: snapshot for snapshot in snapshots}

    usde_metadata = snapshots_by_market[canonical_address(USDE)].irm_params_json
    susde_metadata = snapshots_by_market[canonical_address(SUSDE)].irm_params_json
    usdt_metadata = snapshots_by_market[canonical_address(USDT)].irm_params_json

    assert usde_metadata is not None
    assert susde_metadata is not None
    assert usdt_metadata is not None

    assert usde_metadata["merkl_reward_apy"] == "0.0356"
    assert usde_metadata["merkl_reward_source"] == "merkl_api_v4"
    assert usde_metadata["merkl_reward_chain_scope"] == "ethereum_campaign_global_application"
    assert usde_metadata["merkl_opportunity_id"] == "op_1"
    assert usde_metadata["merkl_identifier"] == "id_1"

    usde_total = Decimal(usde_metadata["merkl_target_total_apy"])
    susde_total = Decimal(susde_metadata["merkl_target_total_apy"])
    assert usde_total == susde_total
    assert Decimal(susde_metadata["merkl_reward_apy"]) < Decimal(usde_metadata["merkl_reward_apy"])
    assert susde_metadata["merkl_reward_source"] == "merkl_api_v4"
    assert susde_metadata["merkl_reward_chain_scope"] == "ethereum_campaign_global_application"
    assert susde_metadata["merkl_opportunity_id"] == "op_1"
    assert susde_metadata["merkl_identifier"] == "id_1"

    assert "merkl_reward_apy" not in usdt_metadata
