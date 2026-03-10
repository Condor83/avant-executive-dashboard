"""Merkl reward APY behavior tests for Aave v3 USDe/sUSDe loops."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal

from adapters.aave_v3.adapter import (
    AaveV3Adapter,
    ReserveCaps,
    ReserveData,
    ReserveRiskConfiguration,
    UserAccountData,
    UserReserveData,
    _MerklRewardContext,
    apr_to_apy,
)
from core.config import MarketsConfig, canonical_address

USDE = "0x4444444444444444444444444444444444444444"
SUSDE = "0x5555555555555555555555555555555555555555"
USDT = "0x6666666666666666666666666666666666666666"
MANTLE_WALLET = "0x7777777777777777777777777777777777777777"


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

    def get_reserve_risk_configuration(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveRiskConfiguration:
        del chain_code, pool_data_provider, asset
        return ReserveRiskConfiguration(
            ltv_bps=8_000,
            liquidation_threshold_bps=8_500,
            liquidation_bonus_bps=10_500,
        )

    def get_reserve_optimal_usage_ratio(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> Decimal:
        del chain_code, pool_data_provider, asset
        return Decimal("0.92")


@dataclass(frozen=True)
class _ChainAwareStubRpc(_StubRpc):
    usde_apr_by_chain: dict[str, str]
    susde_apr_by_chain: dict[str, str]

    def get_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveData:
        del pool_data_provider
        market_ref = canonical_address(asset)
        liquidity_rate = "0.01"
        if market_ref == canonical_address(USDE):
            liquidity_rate = self.usde_apr_by_chain.get(chain_code, liquidity_rate)
        elif market_ref == canonical_address(SUSDE):
            liquidity_rate = self.susde_apr_by_chain.get(chain_code, "0.035")
        return ReserveData(
            total_a_token=1_000_000_000_000_000_000_000_000,
            total_stable_debt=0,
            total_variable_debt=500_000_000_000_000_000_000_000,
            liquidity_rate_ray=self._to_ray(liquidity_rate),
            variable_borrow_rate_ray=self._to_ray("0.03"),
        )


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


def _mantle_markets(*, include_reference_market: bool) -> MarketsConfig:
    rate_reference_markets: list[dict[str, object]] = []
    if include_reference_market:
        rate_reference_markets.append({"symbol": "USDe", "asset": USDE, "decimals": 18})

    return MarketsConfig.model_validate(
        {
            "aave_v3": {
                "mantle": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": [MANTLE_WALLET],
                    "markets": [
                        {
                            "symbol": "sUSDe",
                            "asset": SUSDE,
                            "decimals": 18,
                        }
                    ],
                    "rate_reference_markets": rate_reference_markets,
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


def _merkl_chain_markets() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {
                "ethereum": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": [],
                    "markets": [{"symbol": "USDe", "asset": USDE, "decimals": 18}],
                },
                "mantle": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": [],
                    "markets": [{"symbol": "sUSDe", "asset": SUSDE, "decimals": 18}],
                    "rate_reference_markets": [{"symbol": "USDe", "asset": USDE, "decimals": 18}],
                },
                "ink": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": [],
                    "markets": [{"symbol": "sUSDe", "asset": SUSDE, "decimals": 18}],
                    "rate_reference_markets": [{"symbol": "USDe", "asset": USDE, "decimals": 18}],
                },
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


def _mantle_price_map() -> dict[tuple[str, str], Decimal]:
    return {
        ("mantle", canonical_address(SUSDE)): Decimal("1"),
    }


def _reward_context(
    *,
    merkl_chain_id: int = 1,
    reward_apr: str = "0.0356",
) -> _MerklRewardContext:
    return _MerklRewardContext(
        reward_apy=apr_to_apy(Decimal(reward_apr)),
        merkl_chain_id=merkl_chain_id,
        opportunity_id="op_1",
        identifier="id_1",
        name="Lend sUSDe and USDe on Aave (looping required)",
    )


def test_parse_merkl_reward_apy_prefers_apr_and_normalizes_to_apy() -> None:
    assert AaveV3Adapter._parse_merkl_reward_apy({"maxApr": 0.0356, "apr": 4.49}) == apr_to_apy(
        Decimal("0.0449")
    )


def test_parse_merkl_reward_apy_falls_back_to_max_apr_and_normalizes_to_apy() -> None:
    assert AaveV3Adapter._parse_merkl_reward_apy({"maxApr": 0.0356}) == apr_to_apy(
        Decimal("0.0356")
    )


def test_resolve_merkl_reward_context_caches_per_chain(monkeypatch) -> None:
    adapter = AaveV3Adapter(
        markets_config=_merkl_chain_markets(),
        rpc_client=_StubRpc(),
        merkl_base_url="https://api.merkl.xyz",
    )
    calls: list[tuple[str, int]] = []

    def _fetch(*, chain_code: str, merkl_chain_id: int) -> _MerklRewardContext:
        calls.append((chain_code, merkl_chain_id))
        return _reward_context(merkl_chain_id=merkl_chain_id)

    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _fetch)

    mantle_context, mantle_issues = adapter._resolve_merkl_reward_context(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        stage="sync_snapshot",
        chain_code="mantle",
    )
    mantle_cached, mantle_cached_issues = adapter._resolve_merkl_reward_context(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        stage="sync_markets",
        chain_code="mantle",
    )
    ethereum_context, ethereum_issues = adapter._resolve_merkl_reward_context(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        stage="sync_snapshot",
        chain_code="ethereum",
    )
    ink_context, ink_issues = adapter._resolve_merkl_reward_context(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        stage="sync_snapshot",
        chain_code="ink",
    )

    assert not mantle_issues
    assert not mantle_cached_issues
    assert not ethereum_issues
    assert not ink_issues
    assert mantle_context == mantle_cached
    assert mantle_context is mantle_cached
    assert ethereum_context is not None
    assert ink_context is not None
    assert calls == [("mantle", 5000), ("ethereum", 1), ("ink", 57073)]


def test_positions_apply_merkl_reward_to_usde_and_susde(monkeypatch) -> None:
    adapter = AaveV3Adapter(
        markets_config=_markets(),
        rpc_client=_StubRpc(),
        merkl_base_url="https://api.merkl.xyz",
    )

    def _fetch(*, chain_code: str, merkl_chain_id: int) -> _MerklRewardContext:
        del chain_code, merkl_chain_id
        return _reward_context()

    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _fetch)

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

    assert usde.reward_apy == _reward_context().reward_apy
    assert susde.reward_apy < usde.reward_apy
    assert usdt.reward_apy == Decimal("0")
    assert usde.supply_apy + usde.reward_apy == susde.supply_apy + susde.reward_apy


def test_merkl_fetch_failure_records_issue_and_keeps_reward_zero(monkeypatch) -> None:
    adapter = AaveV3Adapter(
        markets_config=_markets(),
        rpc_client=_StubRpc(),
        merkl_base_url="https://api.merkl.xyz",
    )

    def _raise_merkl_error(*, chain_code: str, merkl_chain_id: int) -> _MerklRewardContext:
        del chain_code, merkl_chain_id
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

    def _fetch(*, chain_code: str, merkl_chain_id: int) -> _MerklRewardContext:
        del chain_code, merkl_chain_id
        return _reward_context()

    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _fetch)

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
    assert usde_metadata["optimal_usage_ratio"] == "0.92"
    assert susde_metadata["optimal_usage_ratio"] == "0.92"
    assert usdt_metadata["optimal_usage_ratio"] == "0.92"

    assert usde_metadata["merkl_reward_apy"] == str(_reward_context().reward_apy)
    assert usde_metadata["merkl_reward_source"] == "merkl_api_v4"
    assert usde_metadata["merkl_reward_chain_scope"] == "chain_local_application"
    assert usde_metadata["merkl_chain_id"] == 1
    assert usde_metadata["merkl_opportunity_id"] == "op_1"
    assert usde_metadata["merkl_identifier"] == "id_1"

    usde_total = Decimal(usde_metadata["merkl_target_total_apy"])
    susde_total = Decimal(susde_metadata["merkl_target_total_apy"])
    assert usde_total == susde_total
    assert Decimal(susde_metadata["merkl_reward_apy"]) < Decimal(usde_metadata["merkl_reward_apy"])
    assert susde_metadata["merkl_reward_source"] == "merkl_api_v4"
    assert susde_metadata["merkl_reward_chain_scope"] == "chain_local_application"
    assert susde_metadata["merkl_chain_id"] == 1
    assert susde_metadata["merkl_opportunity_id"] == "op_1"
    assert susde_metadata["merkl_identifier"] == "id_1"

    assert "merkl_reward_apy" not in usdt_metadata


def test_mantle_susde_uses_same_chain_reference_market_without_surfacing_hidden_usde(
    monkeypatch,
) -> None:
    adapter = AaveV3Adapter(
        markets_config=_mantle_markets(include_reference_market=True),
        rpc_client=_ChainAwareStubRpc(
            usde_apr_by_chain={"mantle": "0.015"},
            susde_apr_by_chain={"mantle": "0.03"},
        ),
        merkl_base_url="https://api.merkl.xyz",
    )

    def _fetch(*, chain_code: str, merkl_chain_id: int) -> _MerklRewardContext:
        assert chain_code == "mantle"
        assert merkl_chain_id == 5000
        return _reward_context(merkl_chain_id=5000, reward_apr="0.02")

    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _fetch)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_mantle_price_map(),
    )
    snapshots, market_issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_mantle_price_map(),
    )

    assert not issues
    assert not market_issues
    assert len(positions) == 1
    assert len(snapshots) == 1
    assert positions[0].market_ref == canonical_address(SUSDE)
    assert snapshots[0].market_ref == canonical_address(SUSDE)

    expected_total = apr_to_apy(Decimal("0.015")) + apr_to_apy(Decimal("0.02"))
    expected_reward = expected_total - apr_to_apy(Decimal("0.03"))
    assert positions[0].reward_apy == expected_reward

    snapshot_metadata = snapshots[0].irm_params_json
    assert snapshot_metadata is not None
    assert Decimal(snapshot_metadata["merkl_reward_apy"]) == expected_reward
    assert Decimal(snapshot_metadata["merkl_target_total_apy"]) == expected_total
    assert snapshot_metadata["merkl_chain_id"] == 5000


def test_missing_same_chain_reference_records_issue_and_keeps_susde_reward_zero(
    monkeypatch,
) -> None:
    adapter = AaveV3Adapter(
        markets_config=_mantle_markets(include_reference_market=False),
        rpc_client=_ChainAwareStubRpc(
            usde_apr_by_chain={"mantle": "0.015"},
            susde_apr_by_chain={"mantle": "0.03"},
        ),
        merkl_base_url="https://api.merkl.xyz",
    )

    def _fetch(*, chain_code: str, merkl_chain_id: int) -> _MerklRewardContext:
        assert chain_code == "mantle"
        assert merkl_chain_id == 5000
        return _reward_context(merkl_chain_id=5000, reward_apr="0.02")

    monkeypatch.setattr(adapter, "_fetch_merkl_usde_loop_reward_context", _fetch)

    positions, issues = adapter.collect_positions(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_mantle_price_map(),
    )
    snapshots, market_issues = adapter.collect_markets(
        as_of_ts_utc=datetime(2026, 3, 3, 0, 0, tzinfo=UTC),
        prices_by_token=_mantle_price_map(),
    )

    assert len(positions) == 1
    assert positions[0].reward_apy == Decimal("0")
    assert any(issue.error_type == "aave_usde_reference_supply_apy_missing" for issue in issues)
    assert len(snapshots) == 1
    assert snapshots[0].irm_params_json is not None
    assert snapshots[0].irm_params_json["merkl_reward_apy"] == "0"
    assert any(
        issue.error_type == "aave_usde_reference_supply_apy_missing" for issue in market_issues
    )
