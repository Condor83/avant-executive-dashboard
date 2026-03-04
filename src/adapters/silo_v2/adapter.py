"""Silo v2 adapter for market health and strategy wallet snapshots."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Protocol

import httpx

from core.config import ConsumerMarket, ConsumerMarketsConfig, MarketsConfig, canonical_address
from core.types import DataQualityIssue, MarketSnapshotInput, PositionSnapshotInput

WAD = Decimal("1000000000000000000")
QueryPrimitive = str | int | float | bool | None
QueryParamValue = QueryPrimitive | Sequence[QueryPrimitive]
QueryParams = Mapping[str, QueryParamValue]


def normalize_raw_amount(raw_amount: int, decimals: int) -> Decimal:
    """Convert raw token balances into decimal token units."""

    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def normalize_rate_to_unit(value: object) -> Decimal:
    """Normalize APR/APY payloads to canonical 0.0-1.0 units."""

    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"invalid rate value: {value}") from None
    if parsed < 0:
        raise ValueError(f"rate cannot be negative: {parsed}")
    if parsed > Decimal("1"):
        if parsed > Decimal("100"):
            return parsed / WAD
        return parsed / Decimal("100")
    return parsed


def _to_decimal(value: object, *, default: Decimal | None = None) -> Decimal:
    if value is None:
        if default is None:
            raise ValueError("missing decimal value")
        return default
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        if default is None:
            raise ValueError(f"invalid decimal value: {value}") from None
        return default


@dataclass(frozen=True)
class SiloMarketHealth:
    """Normalized market health metrics."""

    total_supply_raw: int | None
    total_borrow_raw: int | None
    total_supply_usd: Decimal | None
    total_borrow_usd: Decimal | None
    supply_apy: Decimal
    borrow_apy: Decimal
    utilization: Decimal | None
    block_number_or_slot: str | None = None
    raw_payload: dict[str, object] | None = None


@dataclass(frozen=True)
class SiloHolderPosition:
    """Top-holder snapshot payload for one wallet."""

    wallet_address: str
    supplied_raw: int
    borrowed_raw: int
    raw_payload: dict[str, object] | None = None


@dataclass(frozen=True)
class SiloWalletPosition:
    """Wallet-scoped Silo position payload normalized to raw token units."""

    wallet_address: str
    supplied_raw: int
    borrowed_raw: int
    raw_payload: dict[str, object] | None = None


class SiloTokenMappingError(RuntimeError):
    """Raised when configured tokens cannot be mapped to Silo payload silo legs."""


class SiloClient(Protocol):
    """Protocol for Silo market reads used by the adapter."""

    def close(self) -> None:
        """Close transport resources."""

    def get_market_health(self, *, chain_code: str, market_ref: str) -> SiloMarketHealth:
        """Return normalized market-level health metrics."""

    def get_wallet_position(
        self,
        *,
        chain_code: str,
        market_ref: str,
        wallet_address: str,
        collateral_token_address: str,
        borrow_token_address: str,
    ) -> SiloWalletPosition:
        """Return one wallet position for a configured market."""

    def get_top_holders(
        self, *, chain_code: str, market_ref: str, limit: int
    ) -> list[SiloHolderPosition]:
        """Return top holder positions for a configured market."""


class SiloApiClient:
    """HTTP client for Silo API market-health and wallet position reads."""

    def __init__(
        self,
        base_url: str,
        timeout_seconds: float = 15.0,
        points_base_url: str | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.points_base_url = points_base_url.rstrip("/") if points_base_url else None
        self._client = httpx.Client(timeout=timeout_seconds)
        self._earn_silo_cache: dict[str, dict[str, dict[str, object]]] = {}

    def close(self) -> None:
        """Close HTTP transport resources."""

        self._client.close()

    def _get_json(
        self,
        path: str,
        params: QueryParams | None = None,
        *,
        base_url: str | None = None,
    ) -> object:
        request_base_url = base_url or self.base_url
        response = self._client.get(f"{request_base_url}{path}", params=params)
        response.raise_for_status()
        return response.json()

    def _post_json(
        self,
        path: str,
        payload_json: dict[str, object],
        *,
        base_url: str | None = None,
    ) -> object:
        request_base_url = base_url or self.base_url
        response = self._client.post(f"{request_base_url}{path}", json=payload_json)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _candidate_dicts(payload: object) -> list[dict[str, object]]:
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Silo payload: {payload}")
        candidates = [payload]
        for key in ("data", "market", "result", "stats", "health"):
            nested = payload.get(key)
            if isinstance(nested, dict):
                candidates.append(nested)
        return candidates

    @classmethod
    def _extract_first(cls, payload: object, keys: tuple[str, ...]) -> object:
        for candidate in cls._candidate_dicts(payload):
            for key in keys:
                if key in candidate:
                    return candidate[key]
        return None

    def _get_lending_market_payload(self, *, chain_code: str, market_ref: str) -> dict[str, object]:
        payload = self._get_json(
            f"/api/lending-market/{chain_code}/{market_ref}",
            base_url=self.base_url,
        )
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected Silo lending market payload: {payload}")
        return payload

    @staticmethod
    def _silo_index_value(silo_payload: dict[str, object]) -> str | None:
        silo_index = silo_payload.get("siloIndex")
        if silo_index is None:
            return None
        return str(silo_index)

    @classmethod
    def _pick_debt_silo(cls, market_payload: dict[str, object]) -> dict[str, object]:
        silo1 = market_payload.get("silo1")
        if isinstance(silo1, dict):
            return silo1
        silo0 = market_payload.get("silo0")
        if isinstance(silo0, dict):
            return silo0

        for value in market_payload.values():
            if isinstance(value, dict) and cls._silo_index_value(value) == "1":
                return value
        raise RuntimeError("Silo lending market payload missing silo entries")

    def _load_earn_silo_cache(self, *, chain_code: str) -> dict[str, dict[str, object]]:
        cached = self._earn_silo_cache.get(chain_code)
        if cached is not None:
            return cached

        payload = self._post_json(
            "/api/earn",
            {
                "search": None,
                "chainKeys": [chain_code],
                "type": "silo",
                "sort": None,
                "limit": 500,
                "offset": 0,
            },
            base_url=self.base_url,
        )

        pools: list[object] = []
        if isinstance(payload, dict):
            maybe_pools = payload.get("pools")
            if isinstance(maybe_pools, list):
                pools = maybe_pools

        mapping: dict[str, dict[str, object]] = {}
        for pool_obj in pools:
            if not isinstance(pool_obj, dict):
                continue
            pool_id = pool_obj.get("id")
            if pool_id is not None:
                mapping[str(pool_id)] = pool_obj
            market_id = pool_obj.get("marketId")
            if isinstance(market_id, str):
                mapping[market_id.lower()] = pool_obj
                suffix = market_id.split("-", 1)[-1]
                mapping[suffix.lower()] = pool_obj

        self._earn_silo_cache[chain_code] = mapping
        return mapping

    def _find_earn_silo_pool(
        self,
        *,
        chain_code: str,
        market_ref: str,
        market_payload: dict[str, object],
    ) -> dict[str, object] | None:
        try:
            cache = self._load_earn_silo_cache(chain_code=chain_code)
        except Exception:
            return None

        config_address = market_payload.get("configAddress")
        candidates = [market_ref]
        if isinstance(config_address, str):
            candidates.append(config_address)
        candidates.extend(
            f"{chain_code}-{candidate.lower()}"
            for candidate in [market_ref, config_address]
            if isinstance(candidate, str)
        )
        for candidate in candidates:
            pool = cache.get(candidate.lower() if isinstance(candidate, str) else str(candidate))
            if isinstance(pool, dict):
                return pool
        return None

    def _resolve_market_address(self, *, chain_code: str, market_ref: str) -> str:
        if market_ref.startswith("0x") and len(market_ref) == 42:
            return market_ref
        payload = self._get_lending_market_payload(chain_code=chain_code, market_ref=market_ref)
        config_address = payload.get("configAddress")
        if not isinstance(config_address, str) or not config_address.startswith("0x"):
            raise RuntimeError(f"Silo market {market_ref} missing configAddress")
        return config_address

    @staticmethod
    def _int_value(value: object, *, default: int = 0) -> int:
        if value is None:
            return default
        try:
            return int(str(value))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _find_silo_for_token(
        market_payload: dict[str, object], token_address: str
    ) -> dict[str, object] | None:
        token_key = canonical_address(token_address)
        for silo_key in ("silo0", "silo1"):
            silo_obj = market_payload.get(silo_key)
            if not isinstance(silo_obj, dict):
                continue
            silo_token_address = silo_obj.get("tokenAddress")
            if isinstance(silo_token_address, str) and (
                canonical_address(silo_token_address) == token_key
            ):
                return silo_obj
        return None

    def get_wallet_position(
        self,
        *,
        chain_code: str,
        market_ref: str,
        wallet_address: str,
        collateral_token_address: str,
        borrow_token_address: str,
    ) -> SiloWalletPosition:
        market_payload = self._get_json(
            f"/api/lending-market/{chain_code}/{market_ref}",
            params={"user": wallet_address},
            base_url=self.base_url,
        )
        if not isinstance(market_payload, dict):
            raise RuntimeError(f"unexpected Silo lending market payload: {market_payload}")

        collateral_silo = self._find_silo_for_token(market_payload, collateral_token_address)
        if collateral_silo is None:
            raise SiloTokenMappingError(
                "configured collateral token address missing from Silo market payload"
            )

        borrow_silo = self._find_silo_for_token(market_payload, borrow_token_address)
        if borrow_silo is None:
            raise SiloTokenMappingError(
                "configured borrow token address missing from Silo market payload"
            )

        supplied_raw = self._int_value(collateral_silo.get("collateralBalance")) + self._int_value(
            collateral_silo.get("protectedBalance")
        )
        borrowed_raw = self._int_value(borrow_silo.get("debtBalance"))

        return SiloWalletPosition(
            wallet_address=wallet_address,
            supplied_raw=supplied_raw,
            borrowed_raw=borrowed_raw,
            raw_payload=market_payload,
        )

    def get_market_health(self, *, chain_code: str, market_ref: str) -> SiloMarketHealth:
        market_payload = self._get_lending_market_payload(
            chain_code=chain_code,
            market_ref=market_ref,
        )
        debt_silo = self._pick_debt_silo(market_payload)

        total_supply_raw_obj = debt_silo.get("collateralAccruedAssets")
        if total_supply_raw_obj is None:
            total_supply_raw_obj = debt_silo.get("collateralStoredAssets")
        total_borrow_raw_obj = debt_silo.get("debtAccruedAssets")
        if total_borrow_raw_obj is None:
            total_borrow_raw_obj = debt_silo.get("debtStoredAssets")

        total_supply_raw = (
            int(str(total_supply_raw_obj)) if total_supply_raw_obj is not None else None
        )
        total_borrow_raw = (
            int(str(total_borrow_raw_obj)) if total_borrow_raw_obj is not None else None
        )

        earn_pool = self._find_earn_silo_pool(
            chain_code=chain_code,
            market_ref=market_ref,
            market_payload=market_payload,
        )

        supply_apy_obj = None
        if isinstance(earn_pool, dict):
            supply_apy_obj = earn_pool.get("supplyApr")
        if supply_apy_obj is None:
            supply_apy_obj = debt_silo.get("collateralBaseApr")
        borrow_apy_obj = debt_silo.get("debtBaseApr")

        supply_apy = normalize_rate_to_unit(supply_apy_obj if supply_apy_obj is not None else 0)
        borrow_apy = normalize_rate_to_unit(borrow_apy_obj if borrow_apy_obj is not None else 0)

        utilization_obj = debt_silo.get("utilization")
        utilization = _to_decimal(utilization_obj) if utilization_obj is not None else None
        if utilization is not None and utilization > Decimal("1"):
            utilization = utilization / WAD
            if utilization > Decimal("1"):
                utilization = utilization / Decimal("100")

        return SiloMarketHealth(
            total_supply_raw=total_supply_raw,
            total_borrow_raw=total_borrow_raw,
            total_supply_usd=None,
            total_borrow_usd=None,
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            utilization=utilization,
            block_number_or_slot=None,
            raw_payload=market_payload,
        )

    def get_top_holders(
        self, *, chain_code: str, market_ref: str, limit: int
    ) -> list[SiloHolderPosition]:
        payload: object | None = None
        points_error: Exception | None = None

        if self.points_base_url is not None:
            try:
                market_address = self._resolve_market_address(
                    chain_code=chain_code,
                    market_ref=market_ref,
                )
                payload = self._get_json(
                    "/positions/breakdown-by-markets",
                    params={
                        "market": market_address,
                        "sort": "supplied",
                        "order": "desc",
                        "limit": min(max(limit, 1), 100),
                    },
                    base_url=self.points_base_url,
                )
            except Exception as exc:  # pragma: no cover - network failure surface
                points_error = exc

        if payload is None:
            last_error: Exception | None = points_error
            fallback_endpoints: tuple[tuple[str, QueryParams], ...] = (
                (f"/v1/markets/{market_ref}/holders", {"chain": chain_code, "limit": limit}),
                (f"/markets/{market_ref}/holders", {"chain": chain_code, "limit": limit}),
                (f"/v1/{chain_code}/markets/{market_ref}/holders", {"limit": limit}),
            )
            for path, params in fallback_endpoints:
                try:
                    payload = self._get_json(path, params=params, base_url=self.base_url)
                    break
                except Exception as exc:  # pragma: no cover - network failure surface
                    last_error = exc

            if payload is None:
                assert last_error is not None
                raise RuntimeError(f"Silo holders request failed: {last_error}") from last_error

        rows: list[object]
        if isinstance(payload, list):
            rows = payload
        elif isinstance(payload, dict):
            data = payload.get("data")
            if isinstance(data, list):
                rows = data
            else:
                holder_rows = payload.get("holders")
                if isinstance(holder_rows, list):
                    rows = holder_rows
                else:
                    rows = []
        else:
            rows = []

        holders: list[SiloHolderPosition] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            wallet = (
                row.get("wallet")
                or row.get("address")
                or row.get("holder")
                or row.get("walletAddress")
                or row.get("userAddress")
                or row.get("user")
            )
            if not isinstance(wallet, str):
                continue

            supplied_obj = row.get("suppliedRaw")
            if supplied_obj is None:
                supplied_obj = row.get("supplyRaw")
            if supplied_obj is None:
                supplied_obj = row.get("collateralRaw")
            if supplied_obj is None:
                supplied_obj = row.get("suppliedValue")
            if supplied_obj is None:
                supplied_obj = row.get("supplied")

            borrowed_obj = row.get("borrowedRaw")
            if borrowed_obj is None:
                borrowed_obj = row.get("debtRaw")
            if borrowed_obj is None:
                borrowed_obj = row.get("borrowedValue")
            if borrowed_obj is None:
                borrowed_obj = row.get("borrowed")
            if borrowed_obj is None:
                borrowed_obj = 0

            holders.append(
                SiloHolderPosition(
                    wallet_address=wallet,
                    supplied_raw=int(str(supplied_obj)) if supplied_obj is not None else 0,
                    borrowed_raw=int(str(borrowed_obj)),
                    raw_payload=row,
                )
            )

        return holders


class SiloV2Adapter:
    """Collect canonical Silo v2 market and strategy wallet position snapshots."""

    protocol_code = "silo_v2"

    def __init__(
        self,
        *,
        markets_config: MarketsConfig,
        consumer_markets_config: ConsumerMarketsConfig,
        client: SiloClient,
        top_holders_limit: int = 50,
        include_strategy_wallets: bool = True,
    ) -> None:
        self.markets_config = markets_config
        self.consumer_markets_config = consumer_markets_config
        self.client = client
        self.top_holders_limit = top_holders_limit
        self.include_strategy_wallets = include_strategy_wallets

    @staticmethod
    def _utilization(total_supply: Decimal, total_borrow: Decimal) -> Decimal:
        if total_supply <= 0:
            return Decimal("0")
        return total_borrow / total_supply

    def _strategy_wallets_for_chain(self, chain_code: str) -> set[str]:
        wallets: set[str] = set()
        for section in (
            self.markets_config.aave_v3,
            self.markets_config.spark,
            self.markets_config.morpho,
            self.markets_config.euler_v2,
            self.markets_config.dolomite,
            self.markets_config.kamino,
            self.markets_config.zest,
            self.markets_config.wallet_balances,
            self.markets_config.traderjoe_lp,
            self.markets_config.stakedao,
            self.markets_config.etherex,
        ):
            chain_config = section.get(chain_code)
            if chain_config is None:
                continue
            for wallet in chain_config.wallets:
                wallet_key = canonical_address(wallet)
                if wallet_key.startswith("0x") and len(wallet_key) == 42:
                    wallets.add(wallet_key)
        return wallets

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

    def _silo_markets(self) -> list[ConsumerMarket]:
        return [
            market
            for market in self.consumer_markets_config.markets
            if market.protocol.strip().lower() == self.protocol_code
        ]

    @staticmethod
    def _position_key(chain_code: str, wallet_address: str, market_ref: str) -> str:
        return f"silo_v2:{chain_code}:{wallet_address}:{market_ref}"

    def collect_positions(
        self,
        *,
        as_of_ts_utc: datetime,
        prices_by_token: dict[tuple[str, str], Decimal],
    ) -> tuple[list[PositionSnapshotInput], list[DataQualityIssue]]:
        positions: list[PositionSnapshotInput] = []
        issues: list[DataQualityIssue] = []

        if not self.include_strategy_wallets:
            return positions, issues

        for market in self._silo_markets():
            chain_code = market.chain
            market_ref = market.market_address
            strategy_wallets = self._strategy_wallets_for_chain(chain_code)

            if not strategy_wallets:
                continue

            market_health: SiloMarketHealth | None = None
            try:
                market_health = self.client.get_market_health(
                    chain_code=chain_code,
                    market_ref=market_ref,
                )
            except Exception:
                market_health = None

            collateral_price = prices_by_token.get(
                (chain_code, canonical_address(market.collateral_token.address))
            )
            borrow_price = prices_by_token.get(
                (chain_code, canonical_address(market.borrow_token.address))
            )

            if collateral_price is None:
                collateral_price = Decimal("0")
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="price_missing",
                        error_message="no price available for Silo collateral token",
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"symbol": market.collateral_token.symbol},
                    )
                )
            if borrow_price is None:
                borrow_price = Decimal("0")
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_snapshot",
                        error_type="price_missing",
                        error_message="no price available for Silo borrow token",
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"symbol": market.borrow_token.symbol},
                    )
                )

            supply_apy = market_health.supply_apy if market_health is not None else Decimal("0")
            borrow_apy = market_health.borrow_apy if market_health is not None else Decimal("0")
            block_number_or_slot = (
                market_health.block_number_or_slot if market_health is not None else None
            )

            for wallet_address in sorted(strategy_wallets):
                try:
                    wallet_position = self.client.get_wallet_position(
                        chain_code=chain_code,
                        market_ref=market_ref,
                        wallet_address=wallet_address,
                        collateral_token_address=market.collateral_token.address,
                        borrow_token_address=market.borrow_token.address,
                    )
                except SiloTokenMappingError as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="silo_token_mapping_mismatch",
                            error_message=str(exc),
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            payload_json={"market_name": market.name},
                        )
                    )
                    continue
                except Exception as exc:
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_snapshot",
                            error_type="silo_wallet_position_read_failed",
                            error_message=str(exc),
                            chain_code=chain_code,
                            wallet_address=wallet_address,
                            market_ref=market_ref,
                            payload_json={"market_name": market.name},
                        )
                    )
                    continue

                supplied_amount = normalize_raw_amount(
                    wallet_position.supplied_raw,
                    market.collateral_token.decimals,
                )
                borrowed_amount = normalize_raw_amount(
                    wallet_position.borrowed_raw,
                    market.borrow_token.decimals,
                )
                if supplied_amount == 0 and borrowed_amount == 0:
                    continue

                supplied_usd = supplied_amount * collateral_price
                borrowed_usd = borrowed_amount * borrow_price

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
                        supply_apy=supply_apy,
                        borrow_apy=borrow_apy,
                        reward_apy=Decimal("0"),
                        equity_usd=supplied_usd - borrowed_usd,
                        source="rpc",
                        block_number_or_slot=block_number_or_slot,
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

        for market in self._silo_markets():
            chain_code = market.chain
            market_ref = market.market_address

            try:
                health = self.client.get_market_health(chain_code=chain_code, market_ref=market_ref)
            except Exception as exc:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_markets",
                        error_type="silo_market_health_read_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                        market_ref=market_ref,
                        payload_json={"market_name": market.name},
                    )
                )
                continue

            collateral_price = prices_by_token.get(
                (chain_code, canonical_address(market.collateral_token.address))
            )
            borrow_price = prices_by_token.get(
                (chain_code, canonical_address(market.borrow_token.address))
            )

            total_supply_usd: Decimal
            total_borrow_usd: Decimal
            if health.total_supply_usd is not None and health.total_borrow_usd is not None:
                total_supply_usd = health.total_supply_usd
                total_borrow_usd = health.total_borrow_usd
            elif health.total_supply_raw is not None and health.total_borrow_raw is not None:
                if collateral_price is None:
                    collateral_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="price_missing",
                            error_message="no price available for Silo collateral token",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": market.collateral_token.symbol},
                        )
                    )
                if borrow_price is None:
                    borrow_price = Decimal("0")
                    issues.append(
                        self._issue(
                            as_of_ts_utc=as_of_ts_utc,
                            stage="sync_markets",
                            error_type="price_missing",
                            error_message="no price available for Silo borrow token",
                            chain_code=chain_code,
                            market_ref=market_ref,
                            payload_json={"symbol": market.borrow_token.symbol},
                        )
                    )
                total_supply_usd = (
                    normalize_raw_amount(
                        health.total_supply_raw,
                        market.collateral_token.decimals,
                    )
                    * collateral_price
                )
                total_borrow_usd = (
                    normalize_raw_amount(
                        health.total_borrow_raw,
                        market.borrow_token.decimals,
                    )
                    * borrow_price
                )
            else:
                issues.append(
                    self._issue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_markets",
                        error_type="silo_market_totals_missing",
                        error_message="Silo market health payload missing both USD and raw totals",
                        chain_code=chain_code,
                        market_ref=market_ref,
                    )
                )
                continue

            utilization = (
                health.utilization
                if health.utilization is not None
                else self._utilization(total_supply_usd, total_borrow_usd)
            )

            snapshots.append(
                MarketSnapshotInput(
                    as_of_ts_utc=as_of_ts_utc,
                    protocol_code=self.protocol_code,
                    chain_code=chain_code,
                    market_ref=market_ref,
                    total_supply_usd=max(total_supply_usd, Decimal("0")),
                    total_borrow_usd=max(total_borrow_usd, Decimal("0")),
                    utilization=utilization,
                    supply_apy=max(health.supply_apy, Decimal("0")),
                    borrow_apy=max(health.borrow_apy, Decimal("0")),
                    source="rpc",
                    block_number_or_slot=health.block_number_or_slot,
                    available_liquidity_usd=max(total_supply_usd - total_borrow_usd, Decimal("0")),
                    irm_params_json={
                        "market_name": market.name,
                        "collateral_symbol": market.collateral_token.symbol,
                        "borrow_symbol": market.borrow_token.symbol,
                        "raw_payload_keys": sorted(health.raw_payload.keys())
                        if isinstance(health.raw_payload, dict)
                        else None,
                    },
                )
            )

        return snapshots, issues
