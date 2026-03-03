"""Spark adapter for canonical position and market snapshot ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from adapters.aave_v3.adapter import (
    AaveV3RpcClient,
    EvmRpcAaveV3Client,
    ReserveData,
    apr_to_apy,
    normalize_aave_ray_rate,
    normalize_raw_amount,
)
from core.config import MarketsConfig, SparkChainConfig, SparkMarket, canonical_address
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput
from core.yields import DefiLlamaYieldOracle

WAD = Decimal("1e18")
BPS = Decimal("1e4")
MAX_HEALTH_FACTOR_NUMERIC_ABS = Decimal("1e10")


SparkRpcClient = AaveV3RpcClient


class EvmRpcSparkClient(EvmRpcAaveV3Client):
    """JSON-RPC client for Spark Aave-fork reads against EVM chains."""


@dataclass(frozen=True)
class _ReserveRuntime:
    market: SparkMarket
    reserve_data: ReserveData
    supply_rate_norm: Decimal
    borrow_rate_norm: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    supply_apy_source: str
    supply_apy_fallback_pool_id: str | None = None


class SparkAdapter:
    """Collect canonical Spark positions and market snapshots."""

    protocol_code = "spark"

    def __init__(
        self,
        markets_config: MarketsConfig,
        rpc_client: SparkRpcClient,
        *,
        defillama_timeout_seconds: float = 15.0,
        yield_oracle: DefiLlamaYieldOracle | None = None,
    ) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client
        self.yield_oracle = yield_oracle or DefiLlamaYieldOracle(
            timeout_seconds=defillama_timeout_seconds
        )

    @staticmethod
    def _normalize_health_factor(raw_health_factor_wad: int) -> Decimal:
        return Decimal(raw_health_factor_wad) / WAD

    @staticmethod
    def _sanitize_health_factor(
        health_factor: Decimal | None,
        *,
        has_borrow: bool,
    ) -> Decimal | None:
        if health_factor is None:
            return None
        if not has_borrow:
            return None
        if health_factor < 0 or health_factor >= MAX_HEALTH_FACTOR_NUMERIC_ABS:
            return None
        return health_factor

    @staticmethod
    def _normalize_ltv(raw_ltv_bps: int) -> Decimal:
        return Decimal(raw_ltv_bps) / BPS

    @staticmethod
    def _utilization(total_supply: Decimal, total_borrow: Decimal) -> Decimal:
        if total_supply <= 0:
            return Decimal("0")
        return total_borrow / total_supply

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, market_ref: str) -> str:
        return f"spark:{chain_code}:{wallet_address}:{market_ref}"

    def _fetch_defillama_pool_apy(self, pool_id: str) -> Decimal:
        """Fetch latest pool APY from DefiLlama chart endpoint in 0.0-1.0 units."""

        return self.yield_oracle.get_pool_apy(pool_id)

    def _issue(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        error_type: str,
        error_message: str,
        chain_code: str,
        wallet_address: str | None = None,
        market_ref: str | None = None,
        payload_json: dict[str, object] | None = None,
    ) -> DataQualityIssue:
        return DataQualityIssue(
            as_of_ts_utc=as_of_ts_utc,
            stage=stage,
            error_type=error_type,
            error_message=error_message,
            protocol_code=self.protocol_code,
            chain_code=chain_code,
            wallet_address=wallet_address,
            market_ref=market_ref,
            payload_json=payload_json,
        )

    def _collect_chain_reserves(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
        chain_code: str,
        chain_config: SparkChainConfig,
    ) -> tuple[dict[str, _ReserveRuntime], list[DataQualityIssue], str | None]:
        issues: list[DataQualityIssue] = []
        reserve_map: dict[str, _ReserveRuntime] = {}

        block_number_or_slot: str | None = None
        try:
            block_number_or_slot = str(self.rpc_client.get_block_number(chain_code))
        except Exception as exc:
            issues.append(
                self._issue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage=stage,
                    error_type="spark_block_number_failed",
                    error_message=str(exc),
                    chain_code=chain_code,
                )
            )

        for market in chain_config.markets:
            market_ref = canonical_address(market.asset)
            try:
                reserve_data = self.rpc_client.get_reserve_data(
                    chain_code,
                    chain_config.pool_data_provider,
                    market_ref,
                )
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage=stage,
                        error_type="spark_reserve_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"symbol": market.symbol},
                    )
                )
                continue

            supply_rate_norm = normalize_aave_ray_rate(reserve_data.liquidity_rate_ray)
            borrow_rate_norm = normalize_aave_ray_rate(reserve_data.variable_borrow_rate_ray)
            supply_apy = apr_to_apy(supply_rate_norm)
            supply_apy_source = "protocol_supply_apy"
            fallback_pool_id: str | None = None

            if supply_apy <= 0 and market.supply_apy_fallback_pool_id:
                fallback_pool_id = market.supply_apy_fallback_pool_id
                try:
                    supply_apy = self._fetch_defillama_pool_apy(fallback_pool_id)
                    supply_apy_source = "defillama_pool_fallback"
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage=stage,
                            error_type="spark_supply_apy_fallback_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={
                                "symbol": market.symbol,
                                "pool_id": fallback_pool_id,
                            },
                        )
                    )
                    supply_apy = Decimal("0")
                    supply_apy_source = "protocol_supply_apy"
                    fallback_pool_id = None

            reserve_map[market_ref] = _ReserveRuntime(
                market=market,
                reserve_data=reserve_data,
                supply_rate_norm=supply_rate_norm,
                borrow_rate_norm=borrow_rate_norm,
                supply_apy=supply_apy,
                borrow_apy=apr_to_apy(borrow_rate_norm),
                supply_apy_source=supply_apy_source,
                supply_apy_fallback_pool_id=fallback_pool_id,
            )

        return reserve_map, issues, block_number_or_slot

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        reserve_maps_by_chain: dict[str, dict[str, _ReserveRuntime]] = {}
        block_by_chain: dict[str, str | None] = {}
        for chain_code, chain_config in self.markets_config.spark.items():
            reserve_map, reserve_issues, block_number_or_slot = self._collect_chain_reserves(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_snapshot",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(reserve_issues)
            reserve_maps_by_chain[chain_code] = reserve_map
            block_by_chain[chain_code] = block_number_or_slot

        for chain_code, chain_config in self.markets_config.spark.items():
            reserve_map = reserve_maps_by_chain[chain_code]
            block_number_or_slot = block_by_chain[chain_code]
            for wallet in chain_config.wallets:
                wallet_address = canonical_address(wallet)
                account_data = None
                try:
                    account_data = self.rpc_client.get_user_account_data(
                        chain_code,
                        chain_config.pool,
                        wallet_address,
                    )
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="spark_user_account_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                        )
                    )

                for market in chain_config.markets:
                    market_ref = canonical_address(market.asset)
                    reserve_runtime = reserve_map.get(market_ref)
                    if reserve_runtime is None:
                        continue

                    try:
                        user_reserve = self.rpc_client.get_user_reserve_data(
                            chain_code,
                            chain_config.pool_data_provider,
                            market_ref,
                            wallet_address,
                        )
                    except Exception as exc:
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="spark_user_reserve_read_failed",
                                error_message=str(exc),
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"symbol": market.symbol},
                            )
                        )
                        continue

                    supplied_amount = normalize_raw_amount(
                        user_reserve.current_a_token_balance,
                        market.decimals,
                    )
                    borrowed_raw = (
                        user_reserve.current_stable_debt + user_reserve.current_variable_debt
                    )
                    borrowed_amount = normalize_raw_amount(borrowed_raw, market.decimals)

                    if supplied_amount == 0 and borrowed_amount == 0:
                        continue

                    price_key = (chain_code, market_ref)
                    price_usd = prices_by_token.get(price_key)
                    if price_usd is None:
                        price_usd = Decimal("0")
                        issues.append(
                            self._issue(
                                as_of_ts_utc=as_of_ts_utc,
                                stage="sync_snapshot",
                                error_type="price_missing",
                                error_message="no price available for Spark reserve asset",
                                chain_code=chain_code,
                                wallet_address=wallet_address,
                                market_ref=market_ref,
                                payload_json={"symbol": market.symbol},
                            )
                        )

                    supplied_usd = supplied_amount * price_usd
                    borrowed_usd = borrowed_amount * price_usd
                    equity_usd = supplied_usd - borrowed_usd

                    raw_health_factor = (
                        self._normalize_health_factor(account_data.health_factor_wad)
                        if account_data is not None
                        else None
                    )
                    health_factor = self._sanitize_health_factor(
                        raw_health_factor,
                        has_borrow=borrowed_amount > 0,
                    )
                    ltv = (
                        self._normalize_ltv(account_data.ltv_bps)
                        if account_data is not None
                        else None
                    )

                    positions.append(
                        PositionSnapshotInput(
                            as_of_ts_utc=as_of_ts_utc,
                            protocol_code=self.protocol_code,
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            position_key=self._position_key(chain_code, wallet_address, market_ref),
                            supplied_amount=supplied_amount,
                            supplied_usd=supplied_usd,
                            borrowed_amount=borrowed_amount,
                            borrowed_usd=borrowed_usd,
                            supply_apy=reserve_runtime.supply_apy,
                            borrow_apy=reserve_runtime.borrow_apy,
                            reward_apy=Decimal("0"),
                            equity_usd=equity_usd,
                            source="rpc",
                            block_number_or_slot=block_number_or_slot,
                            health_factor=health_factor,
                            ltv=ltv,
                        )
                    )

        return positions, issues

    def collect_markets(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[MarketSnapshotInput], list[DataQualityIssue]]:
        snapshots: list[MarketSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        reserve_maps_by_chain: dict[str, dict[str, _ReserveRuntime]] = {}
        block_by_chain: dict[str, str | None] = {}
        for chain_code, chain_config in self.markets_config.spark.items():
            reserve_map, reserve_issues, block_number_or_slot = self._collect_chain_reserves(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_markets",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(reserve_issues)
            reserve_maps_by_chain[chain_code] = reserve_map
            block_by_chain[chain_code] = block_number_or_slot

        for chain_code, chain_config in self.markets_config.spark.items():
            reserve_map = reserve_maps_by_chain[chain_code]
            block_number_or_slot = block_by_chain[chain_code]
            for market in chain_config.markets:
                market_ref = canonical_address(market.asset)
                reserve_runtime = reserve_map.get(market_ref)
                if reserve_runtime is None:
                    continue

                price_key = (chain_code, market_ref)
                price_usd = prices_by_token.get(price_key)
                if price_usd is None:
                    price_usd = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="price_missing",
                            error_message="no price available for Spark reserve asset",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": market.symbol},
                        )
                    )

                total_supply_amount = normalize_raw_amount(
                    reserve_runtime.reserve_data.total_a_token,
                    market.decimals,
                )
                total_borrow_raw = (
                    reserve_runtime.reserve_data.total_stable_debt
                    + reserve_runtime.reserve_data.total_variable_debt
                )
                total_borrow_amount = normalize_raw_amount(total_borrow_raw, market.decimals)

                total_supply_usd = total_supply_amount * price_usd
                total_borrow_usd = total_borrow_amount * price_usd
                utilization = self._utilization(total_supply_amount, total_borrow_amount)
                available_liquidity_usd = max(total_supply_usd - total_borrow_usd, Decimal("0"))

                caps_json: dict[str, str] | None = None
                caps = self.rpc_client.get_reserve_caps(
                    chain_code,
                    chain_config.pool_data_provider,
                    market_ref,
                )
                if caps is not None:
                    caps_json = {
                        "borrow_cap": str(caps.borrow_cap),
                        "supply_cap": str(caps.supply_cap),
                    }

                irm_params_json = {
                    "supply_rate": {
                        "raw_ray": str(reserve_runtime.reserve_data.liquidity_rate_ray),
                        "normalized_rate": str(reserve_runtime.supply_rate_norm),
                        "apy_compounded": str(reserve_runtime.supply_apy),
                    },
                    "borrow_rate": {
                        "raw_ray": str(reserve_runtime.reserve_data.variable_borrow_rate_ray),
                        "normalized_rate": str(reserve_runtime.borrow_rate_norm),
                        "apy_compounded": str(reserve_runtime.borrow_apy),
                    },
                    "supply_apy_source": reserve_runtime.supply_apy_source,
                    "supply_apy_fallback_pool_id": reserve_runtime.supply_apy_fallback_pool_id,
                    "includes_rewards": "unknown",
                }

                snapshots.append(
                    MarketSnapshotInput(
                        as_of_ts_utc=as_of_ts_utc,
                        protocol_code=self.protocol_code,
                        chain_code=chain_code,
                        market_ref=market_ref,
                        total_supply_usd=total_supply_usd,
                        total_borrow_usd=total_borrow_usd,
                        utilization=utilization,
                        supply_apy=reserve_runtime.supply_apy,
                        borrow_apy=reserve_runtime.borrow_apy,
                        available_liquidity_usd=available_liquidity_usd,
                        caps_json=caps_json,
                        irm_params_json=irm_params_json,
                        source="rpc",
                        block_number_or_slot=block_number_or_slot,
                    )
                )

        return snapshots, issues
