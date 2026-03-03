"""Aave v3 adapter for canonical position and market snapshot ingestion."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

import httpx

from core.config import AaveChainConfig, AaveMarket, MarketsConfig, canonical_address
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput

RAY = Decimal("1e27")
WAD = Decimal("1e18")
BPS = Decimal("1e4")
SECONDS_PER_YEAR = Decimal("31536000")
SECONDS_PER_YEAR_FLOAT = 31536000.0
PCT_TO_UNIT = Decimal("100")
DEFILLAMA_POOL_CHART_URL = "https://yields.llama.fi/chart"
MERKL_OPPORTUNITIES_PATH = "/v4/opportunities"
MERKL_REWARD_SOURCE = "merkl_api_v4"
MERKL_REWARD_CHAIN_SCOPE = "ethereum_campaign_global_application"
MERKL_CHAIN_ID = 1
MERKL_TARGET_SYMBOLS = frozenset({"usde", "susde"})
MAX_HEALTH_FACTOR_NUMERIC_ABS = Decimal("1e10")

GET_RESERVE_DATA_SELECTOR = "0x35ea6a75"
GET_USER_RESERVE_DATA_SELECTOR = "0x28dd2d01"
GET_USER_ACCOUNT_DATA_SELECTOR = "0xbf92857c"
GET_RESERVE_CAPS_SELECTOR = "0x46fbe558"


def normalize_raw_amount(raw_amount: int, decimals: int) -> Decimal:
    """Convert raw on-chain integer amounts into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def normalize_aave_ray_rate(raw_rate_ray: int) -> Decimal:
    """Normalize an Aave ray-scaled annual rate to 0.0-1.0 decimal units."""

    if raw_rate_ray < 0:
        raise ValueError("raw_rate_ray must be non-negative")
    return Decimal(raw_rate_ray) / RAY


def apr_to_apy(apr: Decimal) -> Decimal:
    """Convert an annual nominal rate to compounded APY in 0.0-1.0 units."""

    apr_float = float(apr)
    if apr_float <= 0.0:
        return Decimal("0")
    apy = math.pow(1.0 + (apr_float / SECONDS_PER_YEAR_FLOAT), SECONDS_PER_YEAR_FLOAT) - 1.0
    if apy <= 0.0:
        return Decimal("0")
    return Decimal(str(apy))


def _strip_0x_hex(value: str) -> str:
    cleaned = value.strip().lower()
    return cleaned[2:] if cleaned.startswith("0x") else cleaned


def _encode_address(value: str) -> str:
    return _strip_0x_hex(value).rjust(64, "0")


def _encode_call_data(selector: str, *address_args: str) -> str:
    if not selector.startswith("0x"):
        raise ValueError("selector must start with 0x")
    encoded = selector[2:] + "".join(_encode_address(arg) for arg in address_args)
    return f"0x{encoded}"


def _decode_uint_words(raw_hex: str) -> list[int]:
    payload = _strip_0x_hex(raw_hex)
    if not payload:
        return []
    if len(payload) % 64 != 0:
        raise ValueError(f"invalid ABI payload length: {len(payload)}")
    words: list[int] = []
    for idx in range(0, len(payload), 64):
        words.append(int(payload[idx : idx + 64], 16))
    return words


@dataclass(frozen=True)
class UserReserveData:
    current_a_token_balance: int
    current_stable_debt: int
    current_variable_debt: int


@dataclass(frozen=True)
class UserAccountData:
    ltv_bps: int
    health_factor_wad: int


@dataclass(frozen=True)
class ReserveData:
    total_a_token: int
    total_stable_debt: int
    total_variable_debt: int
    liquidity_rate_ray: int
    variable_borrow_rate_ray: int


@dataclass(frozen=True)
class ReserveCaps:
    borrow_cap: int
    supply_cap: int


class AaveV3RpcClient(Protocol):
    """Protocol for Aave v3 reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_block_number(self, chain_code: str) -> int:
        """Return latest chain block number."""

    def get_user_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
        wallet_address: str,
    ) -> UserReserveData:
        """Return user-level reserve balances."""

    def get_user_account_data(
        self,
        chain_code: str,
        pool: str,
        wallet_address: str,
    ) -> UserAccountData:
        """Return user-level health and LTV data."""

    def get_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveData:
        """Return reserve totals and rates."""

    def get_reserve_caps(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveCaps | None:
        """Return reserve caps if available."""


class EvmRpcAaveV3Client:
    """JSON-RPC client for Aave v3 calls against EVM chains."""

    def __init__(self, rpc_urls: dict[str, str], timeout_seconds: float = 15.0) -> None:
        self.rpc_urls = {key: value for key, value in rpc_urls.items() if value}
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _rpc(self, chain_code: str, method: str, params: list[object]) -> str:
        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing RPC URL for chain '{chain_code}'")

        response = self._client.post(
            rpc_url,
            json={
                "jsonrpc": "2.0",
                "id": 1,
                "method": method,
                "params": params,
            },
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("error"):
            raise RuntimeError(str(payload["error"]))
        result = payload.get("result")
        if not isinstance(result, str):
            raise RuntimeError(f"unexpected RPC response for {method}: {payload}")
        return result

    def _eth_call(self, chain_code: str, to: str, data: str) -> list[int]:
        raw_hex = self._rpc(chain_code, "eth_call", [{"to": to, "data": data}, "latest"])
        return _decode_uint_words(raw_hex)

    def get_block_number(self, chain_code: str) -> int:
        raw = self._rpc(chain_code, "eth_blockNumber", [])
        return int(raw, 16)

    def get_user_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
        wallet_address: str,
    ) -> UserReserveData:
        words = self._eth_call(
            chain_code,
            pool_data_provider,
            _encode_call_data(GET_USER_RESERVE_DATA_SELECTOR, asset, wallet_address),
        )
        if len(words) < 3:
            raise RuntimeError("Aave getUserReserveData returned insufficient words")
        return UserReserveData(
            current_a_token_balance=words[0],
            current_stable_debt=words[1],
            current_variable_debt=words[2],
        )

    def get_user_account_data(
        self,
        chain_code: str,
        pool: str,
        wallet_address: str,
    ) -> UserAccountData:
        words = self._eth_call(
            chain_code,
            pool,
            _encode_call_data(GET_USER_ACCOUNT_DATA_SELECTOR, wallet_address),
        )
        if len(words) < 6:
            raise RuntimeError("Aave getUserAccountData returned insufficient words")
        return UserAccountData(
            ltv_bps=words[4],
            health_factor_wad=words[5],
        )

    def get_reserve_data(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveData:
        words = self._eth_call(
            chain_code,
            pool_data_provider,
            _encode_call_data(GET_RESERVE_DATA_SELECTOR, asset),
        )
        if len(words) < 7:
            raise RuntimeError("Aave getReserveData returned insufficient words")
        return ReserveData(
            total_a_token=words[2],
            total_stable_debt=words[3],
            total_variable_debt=words[4],
            liquidity_rate_ray=words[5],
            variable_borrow_rate_ray=words[6],
        )

    def get_reserve_caps(
        self,
        chain_code: str,
        pool_data_provider: str,
        asset: str,
    ) -> ReserveCaps | None:
        try:
            words = self._eth_call(
                chain_code,
                pool_data_provider,
                _encode_call_data(GET_RESERVE_CAPS_SELECTOR, asset),
            )
        except Exception:
            return None

        if len(words) < 2:
            return None

        return ReserveCaps(
            borrow_cap=words[0],
            supply_cap=words[1],
        )


@dataclass(frozen=True)
class _ReserveRuntime:
    market: AaveMarket
    reserve_data: ReserveData
    supply_rate_norm: Decimal
    borrow_rate_norm: Decimal
    supply_apy: Decimal
    borrow_apy: Decimal
    supply_apy_source: str
    supply_apy_fallback_pool_id: str | None = None


@dataclass(frozen=True)
class _MerklRewardContext:
    reward_apy: Decimal
    opportunity_id: str | None
    identifier: str | None
    name: str | None


class AaveV3Adapter:
    """Collect canonical Aave v3 positions and market snapshots."""

    protocol_code = "aave_v3"

    def __init__(
        self,
        markets_config: MarketsConfig,
        rpc_client: AaveV3RpcClient,
        *,
        defillama_timeout_seconds: float = 15.0,
        merkl_base_url: str | None = None,
        merkl_timeout_seconds: float = 15.0,
    ) -> None:
        self.markets_config = markets_config
        self.rpc_client = rpc_client
        self.defillama_timeout_seconds = defillama_timeout_seconds
        self._defillama_apy_cache: dict[str, Decimal] = {}
        self.merkl_base_url = merkl_base_url.rstrip("/") if merkl_base_url else None
        self.merkl_timeout_seconds = merkl_timeout_seconds
        self._merkl_reward_context_cache: _MerklRewardContext | None = None

    @staticmethod
    def _normalize_health_factor(raw_health_factor_wad: int) -> Decimal:
        return Decimal(raw_health_factor_wad) / WAD

    @staticmethod
    def _sanitize_health_factor(
        health_factor: Decimal | None,
        *,
        has_borrow: bool,
    ) -> Decimal | None:
        """Drop non-actionable / non-storable health factors."""

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
        return f"aave_v3:{chain_code}:{wallet_address}:{market_ref}"

    @staticmethod
    def _is_merkl_reward_symbol(symbol: str) -> bool:
        return symbol.strip().lower() in MERKL_TARGET_SYMBOLS

    def _merkl_issue_chain_code(self) -> str:
        if "ethereum" in self.markets_config.aave_v3:
            return "ethereum"
        return next(iter(self.markets_config.aave_v3.keys()), "ethereum")

    @staticmethod
    def _select_merkl_usde_loop_opportunity(opportunities: list[object]) -> dict[str, object]:
        candidates: list[dict[str, object]] = []
        for entry in opportunities:
            if not isinstance(entry, dict):
                continue

            name = str(entry.get("name", "")).lower()
            if "aave" in name and "usde" in name and "susde" in name:
                candidates.append(entry)
                continue

            tokens = entry.get("tokens")
            if isinstance(tokens, list):
                token_symbols = {
                    str(token.get("symbol", "")).lower()
                    for token in tokens
                    if isinstance(token, dict)
                }
                if "usde" in token_symbols and "susde" in token_symbols:
                    candidates.append(entry)

        if not candidates:
            raise RuntimeError("Merkl USDe/sUSDe Aave opportunity not found")

        live = [entry for entry in candidates if str(entry.get("status", "")).upper() == "LIVE"]
        pool = live if live else candidates

        def _sort_key(entry: dict[str, object]) -> tuple[int, int]:
            latest_start = entry.get("latestCampaignStart")
            last_created = entry.get("lastCampaignCreatedAt")
            start_val = int(str(latest_start)) if latest_start is not None else 0
            created_val = int(str(last_created)) if last_created is not None else 0
            return (start_val, created_val)

        return max(pool, key=_sort_key)

    @staticmethod
    def _parse_merkl_reward_apy(opportunity: dict[str, object]) -> Decimal:
        max_apr = opportunity.get("maxApr")
        if max_apr is not None:
            try:
                parsed = Decimal(str(max_apr))
            except (InvalidOperation, ValueError, TypeError) as exc:
                raise RuntimeError(f"invalid Merkl maxApr value: {max_apr}") from exc
            if parsed < 0:
                raise RuntimeError(f"Merkl maxApr is negative: {parsed}")
            return parsed

        apr = opportunity.get("apr")
        if apr is None:
            raise RuntimeError("Merkl opportunity missing both maxApr and apr")

        try:
            parsed_apr = Decimal(str(apr))
        except (InvalidOperation, ValueError, TypeError) as exc:
            raise RuntimeError(f"invalid Merkl apr value: {apr}") from exc

        if parsed_apr < 0:
            raise RuntimeError(f"Merkl apr is negative: {parsed_apr}")
        if parsed_apr > Decimal("1"):
            return parsed_apr / PCT_TO_UNIT
        return parsed_apr

    def _fetch_merkl_usde_loop_reward_context(self) -> _MerklRewardContext:
        if not self.merkl_base_url:
            raise RuntimeError("Merkl base URL is not configured")

        response = httpx.get(
            f"{self.merkl_base_url}{MERKL_OPPORTUNITIES_PATH}",
            params={
                "mainProtocolId": "aave",
                "chainId": MERKL_CHAIN_ID,
                "name": "USDe",
                "campaigns": "true",
            },
            timeout=self.merkl_timeout_seconds,
        )
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, list):
            raise RuntimeError(f"unexpected Merkl opportunities payload: {payload}")

        opportunity = self._select_merkl_usde_loop_opportunity(payload)
        reward_apy = self._parse_merkl_reward_apy(opportunity)
        return _MerklRewardContext(
            reward_apy=reward_apy,
            opportunity_id=(
                str(opportunity.get("id")) if opportunity.get("id") is not None else None
            ),
            identifier=(
                str(opportunity.get("identifier"))
                if opportunity.get("identifier") is not None
                else None
            ),
            name=(str(opportunity.get("name")) if opportunity.get("name") is not None else None),
        )

    def _resolve_merkl_reward_context(
        self,
        *,
        as_of_ts_utc: datetime,
        stage: str,
    ) -> tuple[_MerklRewardContext | None, list[DataQualityIssue]]:
        if self._merkl_reward_context_cache is not None:
            return self._merkl_reward_context_cache, []
        if not self.merkl_base_url:
            return None, []

        try:
            context = self._fetch_merkl_usde_loop_reward_context()
        except Exception as exc:
            issue = self._issue(
                as_of_ts_utc=as_of_ts_utc,
                stage=stage,
                error_type="aave_merkl_reward_apy_fetch_failed",
                error_message=str(exc),
                chain_code=self._merkl_issue_chain_code(),
                payload_json={"merkl_base_url": self.merkl_base_url},
            )
            return None, [issue]

        self._merkl_reward_context_cache = context
        return context, []

    @staticmethod
    def _find_chain_usde_supply_apy(reserve_map: dict[str, _ReserveRuntime]) -> Decimal | None:
        for runtime in reserve_map.values():
            if runtime.market.symbol.strip().lower() == "usde":
                return runtime.supply_apy
        return None

    @staticmethod
    def _resolve_usde_supply_apy_by_chain(
        reserve_maps_by_chain: dict[str, dict[str, _ReserveRuntime]],
    ) -> tuple[dict[str, Decimal], Decimal]:
        by_chain: dict[str, Decimal] = {}
        for chain_code, reserve_map in reserve_maps_by_chain.items():
            usde_supply = AaveV3Adapter._find_chain_usde_supply_apy(reserve_map)
            if usde_supply is not None:
                by_chain[chain_code] = usde_supply

        if "ethereum" in by_chain:
            default_supply = by_chain["ethereum"]
        elif by_chain:
            default_supply = next(iter(by_chain.values()))
        else:
            default_supply = Decimal("0")

        return by_chain, default_supply

    @staticmethod
    def _susde_reward_apy_aligned_to_usde(
        *,
        chain_code: str,
        susde_supply_apy: Decimal,
        merkl_reward_apy: Decimal,
        usde_supply_apy_by_chain: dict[str, Decimal],
        default_usde_supply_apy: Decimal,
    ) -> Decimal:
        usde_supply_apy = usde_supply_apy_by_chain.get(chain_code, default_usde_supply_apy)
        target_total_apy = usde_supply_apy + merkl_reward_apy
        adjusted_reward = target_total_apy - susde_supply_apy
        if adjusted_reward < 0:
            return Decimal("0")
        return adjusted_reward

    def _fetch_defillama_pool_apy(self, pool_id: str) -> Decimal:
        """Fetch latest pool APY from DefiLlama chart endpoint in 0.0-1.0 units."""

        cached = self._defillama_apy_cache.get(pool_id)
        if cached is not None:
            return cached

        response = httpx.get(
            f"{DEFILLAMA_POOL_CHART_URL}/{pool_id}",
            timeout=self.defillama_timeout_seconds,
        )
        response.raise_for_status()
        payload = response.json()

        if payload.get("status") != "success":
            raise RuntimeError(f"unexpected DefiLlama response status: {payload}")

        rows = payload.get("data")
        if not isinstance(rows, list) or not rows:
            raise RuntimeError("DefiLlama pool chart response has no rows")

        latest = rows[-1]
        apy_pct = latest.get("apy")
        if apy_pct is None:
            raise RuntimeError("DefiLlama pool chart row missing `apy`")

        apy = Decimal(str(apy_pct)) / PCT_TO_UNIT
        if apy < 0:
            raise RuntimeError(f"DefiLlama pool APY is negative for {pool_id}: {apy}")

        self._defillama_apy_cache[pool_id] = apy
        return apy

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
        chain_config: AaveChainConfig,
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
                    error_type="aave_block_number_failed",
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
                        error_type="aave_reserve_read_failed",
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
                            error_type="aave_supply_apy_fallback_failed",
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
        merkl_context, merkl_issues = self._resolve_merkl_reward_context(
            as_of_ts_utc=as_of_ts_utc,
            stage="sync_snapshot",
        )
        issues.extend(merkl_issues)

        reserve_maps_by_chain: dict[str, dict[str, _ReserveRuntime]] = {}
        block_by_chain: dict[str, str | None] = {}
        for chain_code, chain_config in self.markets_config.aave_v3.items():
            reserve_map, reserve_issues, block_number_or_slot = self._collect_chain_reserves(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_snapshot",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(reserve_issues)
            reserve_maps_by_chain[chain_code] = reserve_map
            block_by_chain[chain_code] = block_number_or_slot

        usde_supply_apy_by_chain, default_usde_supply_apy = self._resolve_usde_supply_apy_by_chain(
            reserve_maps_by_chain
        )

        for chain_code, chain_config in self.markets_config.aave_v3.items():
            reserve_map = reserve_maps_by_chain[chain_code]
            block_number_or_slot = block_by_chain[chain_code]
            for wallet in chain_config.wallets:
                wallet_address = canonical_address(wallet)
                account_data: UserAccountData | None = None
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
                            error_type="aave_user_account_read_failed",
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
                                error_type="aave_user_reserve_read_failed",
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
                                error_message="no price available for Aave reserve asset",
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
                    reward_apy = Decimal("0")
                    symbol_lower = market.symbol.strip().lower()
                    if supplied_amount > 0 and merkl_context is not None:
                        if symbol_lower == "usde":
                            reward_apy = merkl_context.reward_apy
                        elif symbol_lower == "susde":
                            reward_apy = self._susde_reward_apy_aligned_to_usde(
                                chain_code=chain_code,
                                susde_supply_apy=reserve_runtime.supply_apy,
                                merkl_reward_apy=merkl_context.reward_apy,
                                usde_supply_apy_by_chain=usde_supply_apy_by_chain,
                                default_usde_supply_apy=default_usde_supply_apy,
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
                            # Canonical field stores full effective supply APY.
                            supply_apy=reserve_runtime.supply_apy,
                            borrow_apy=reserve_runtime.borrow_apy,
                            reward_apy=reward_apy,
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
        merkl_context, merkl_issues = self._resolve_merkl_reward_context(
            as_of_ts_utc=as_of_ts_utc,
            stage="sync_markets",
        )
        issues.extend(merkl_issues)

        reserve_maps_by_chain: dict[str, dict[str, _ReserveRuntime]] = {}
        block_by_chain: dict[str, str | None] = {}
        for chain_code, chain_config in self.markets_config.aave_v3.items():
            reserve_map, reserve_issues, block_number_or_slot = self._collect_chain_reserves(
                as_of_ts_utc=as_of_ts_utc,
                stage="sync_markets",
                chain_code=chain_code,
                chain_config=chain_config,
            )
            issues.extend(reserve_issues)
            reserve_maps_by_chain[chain_code] = reserve_map
            block_by_chain[chain_code] = block_number_or_slot

        usde_supply_apy_by_chain, default_usde_supply_apy = self._resolve_usde_supply_apy_by_chain(
            reserve_maps_by_chain
        )

        for chain_code, chain_config in self.markets_config.aave_v3.items():
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
                            error_message="no price available for Aave reserve asset",
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
                if self._is_merkl_reward_symbol(market.symbol):
                    symbol_lower = market.symbol.strip().lower()
                    merkl_reward_apy = Decimal("0")
                    if merkl_context is not None:
                        if symbol_lower == "usde":
                            merkl_reward_apy = merkl_context.reward_apy
                        elif symbol_lower == "susde":
                            merkl_reward_apy = self._susde_reward_apy_aligned_to_usde(
                                chain_code=chain_code,
                                susde_supply_apy=reserve_runtime.supply_apy,
                                merkl_reward_apy=merkl_context.reward_apy,
                                usde_supply_apy_by_chain=usde_supply_apy_by_chain,
                                default_usde_supply_apy=default_usde_supply_apy,
                            )
                    target_total_apy = reserve_runtime.supply_apy + merkl_reward_apy
                    irm_params_json["merkl_reward_apy"] = str(merkl_reward_apy)
                    irm_params_json["merkl_target_total_apy"] = str(target_total_apy)
                    irm_params_json["merkl_reward_source"] = (
                        MERKL_REWARD_SOURCE if merkl_context is not None else "unavailable"
                    )
                    irm_params_json["merkl_reward_chain_scope"] = MERKL_REWARD_CHAIN_SCOPE
                    irm_params_json["merkl_opportunity_id"] = (
                        merkl_context.opportunity_id if merkl_context is not None else None
                    )
                    irm_params_json["merkl_identifier"] = (
                        merkl_context.identifier if merkl_context is not None else None
                    )

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
