"""Customer cohort discovery, verification, and snapshot config helpers."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

import httpx
import yaml
from eth_utils import keccak
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc
from core.config import (
    AvantToken,
    AvantTokensConfig,
    ConsumerMarketsConfig,
    ConsumerThresholdsConfig,
    HolderExclusionsConfig,
    HolderProtocolMapConfig,
    HolderUniverseConfig,
    MarketsConfig,
    WalletBalanceChainConfig,
    WalletBalanceToken,
    WalletProductsConfig,
    canonical_address,
)
from core.consumer_debank_visibility import is_excluded_visibility_protocol
from core.db.models import (
    ConsumerDebankProtocolDaily,
    ConsumerDebankTokenDaily,
    ConsumerDebankWalletDaily,
    Chain,
    ConsumerCohortDaily,
    ConsumerHolderUniverseDaily,
    ConsumerTokenHolderDaily,
    DataQuality,
    Market,
    PositionSnapshot,
    Price,
    Token,
    Wallet,
)
from core.debank_cloud import DebankCloudClient
from core.debank_coverage import normalize_chain_code, normalize_protocol_code
from core.pricing import PriceOracle
from core.types import DataQualityIssue, PriceRequest

from adapters.silo_v2 import SiloApiClient

ROUTESCAN_BASE_URL = "https://api.routescan.io"
EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ERC20_BALANCE_OF_SELECTOR = "70a08231"
ERC4626_CONVERT_TO_ASSETS_SELECTOR = "07a2d13a"
MORPHO_POSITION_SELECTOR = "93c52062"
DEFAULT_CONSUMER_CHAIN_IDS = {"avalanche": "43114", "ethereum": "1"}
MORPHO_SUPPLY_COLLATERAL_TOPIC = "0x" + keccak(
    text="SupplyCollateral(bytes32,address,address,uint256)"
).hex()
MORPHO_WITHDRAW_COLLATERAL_TOPIC = "0x" + keccak(
    text="WithdrawCollateral(bytes32,address,address,address,uint256)"
).hex()
ZERO = Decimal("0")


@dataclass(frozen=True)
class HolderBalance:
    """Normalized ERC20 holder balance payload."""

    address: str
    balance_raw: int


@dataclass(frozen=True)
class CohortBuildResult:
    """Wallet cohort build output and exclusion counters."""

    wallets: list[HolderBalance]
    fetched_rows: int
    unique_rows: int
    threshold_rows: int
    strategy_excluded: int
    protocol_excluded: int
    contract_excluded: int


class RouteScanClient:
    """Minimal RouteScan top-holder client with pagination support."""

    def __init__(self, base_url: str = ROUTESCAN_BASE_URL, timeout_seconds: float = 20.0) -> None:
        self.base_url = base_url.rstrip("/")
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def get_erc20_holders(
        self,
        *,
        chain_id: str,
        token_address: str,
        limit: int = 200,
        max_rows: int | None = None,
    ) -> list[HolderBalance]:
        """Fetch all holder rows for an ERC20 token across pagination."""

        if limit <= 0:
            raise ValueError("limit must be positive")

        path = (
            f"/v2/network/mainnet/evm/{chain_id}/erc20/{canonical_address(token_address)}/holders"
        )
        holders: list[HolderBalance] = []
        next_token: str | None = None

        while True:
            params: dict[str, str | int] = {"count": "true", "limit": limit}
            if next_token:
                params["next"] = next_token

            response = self._client.get(f"{self.base_url}{path}", params=params)
            response.raise_for_status()
            payload = response.json()

            rows = payload.get("items")
            if not isinstance(rows, list):
                raise RuntimeError(f"unexpected RouteScan payload: {payload}")

            for row in rows:
                if not isinstance(row, dict):
                    continue
                address = row.get("address")
                balance_obj = row.get("balance")
                if not isinstance(address, str) or balance_obj is None:
                    continue
                holders.append(
                    HolderBalance(
                        address=canonical_address(address),
                        balance_raw=max(int(str(balance_obj)), 0),
                    )
                )
                if max_rows is not None and len(holders) >= max_rows:
                    return holders[:max_rows]

            link = payload.get("link")
            next_token = None
            if isinstance(link, dict):
                next_token_obj = link.get("nextToken")
                if isinstance(next_token_obj, str) and next_token_obj:
                    next_token = next_token_obj
            if not next_token:
                break

        return holders


def minimum_balance_raw_for_usd_threshold(
    *,
    threshold_usd: Decimal,
    token_price_usd: Decimal,
    token_decimals: int,
) -> int:
    """Convert USD threshold into minimum token raw units."""

    if threshold_usd < 0:
        raise ValueError("threshold_usd must be non-negative")
    if token_price_usd <= 0:
        raise ValueError("token_price_usd must be positive")
    if token_decimals < 0:
        raise ValueError("token_decimals must be non-negative")

    threshold_tokens = threshold_usd / token_price_usd
    scaled = threshold_tokens * (Decimal(10) ** token_decimals)
    return int(scaled.to_integral_value(rounding=ROUND_CEILING))


def collect_strategy_wallets(
    *,
    markets_config: MarketsConfig,
    wallet_products_config: WalletProductsConfig,
) -> set[str]:
    """Collect all known non-customer strategy/internal wallets from config."""

    strategy_wallets: set[str] = set()

    wallet_groups = (
        markets_config.aave_v3,
        markets_config.spark,
        markets_config.morpho,
        markets_config.euler_v2,
        markets_config.dolomite,
        markets_config.kamino,
        markets_config.pendle,
        markets_config.zest,
        markets_config.wallet_balances,
        markets_config.traderjoe_lp,
        markets_config.stakedao,
        markets_config.etherex,
    )
    for group in wallet_groups:
        for chain_config in group.values():
            for wallet in chain_config.wallets:
                strategy_wallets.add(canonical_address(wallet))

    for assignment in wallet_products_config.assignments:
        if assignment.wallet_type != "customer":
            strategy_wallets.add(canonical_address(assignment.wallet_address))

    return strategy_wallets


def collect_evm_addresses_from_yaml(paths: Iterable[Path]) -> set[str]:
    """Collect every EVM-style address literal appearing in YAML config files."""

    addresses: set[str] = set()

    def walk(value: object) -> None:
        if isinstance(value, dict):
            for nested in value.values():
                walk(nested)
            return
        if isinstance(value, list):
            for nested in value:
                walk(nested)
            return
        if isinstance(value, str):
            cleaned = value.strip()
            if EVM_ADDRESS_RE.fullmatch(cleaned):
                addresses.add(canonical_address(cleaned))

    for path in paths:
        with path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
        if payload is None:
            continue
        walk(payload)

    return addresses


def rpc_contract_addresses(
    *,
    rpc_url: str,
    addresses: Iterable[str],
    timeout_seconds: float = 20.0,
    batch_size: int = 100,
) -> set[str]:
    """Return addresses with non-empty bytecode via batched `eth_getCode`."""

    normalized = [canonical_address(address) for address in addresses]
    if not normalized:
        return set()
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")

    contracts: set[str] = set()
    client = httpx.Client(timeout=timeout_seconds)
    try:
        for offset in range(0, len(normalized), batch_size):
            chunk = normalized[offset : offset + batch_size]
            payload = [
                {
                    "jsonrpc": "2.0",
                    "id": idx,
                    "method": "eth_getCode",
                    "params": [address, "latest"],
                }
                for idx, address in enumerate(chunk)
            ]
            response = client.post(rpc_url, json=payload)
            response.raise_for_status()
            rpc_rows = response.json()
            if isinstance(rpc_rows, dict):
                rpc_rows = [rpc_rows]
            if not isinstance(rpc_rows, list):
                raise RuntimeError(f"unexpected RPC batch response: {rpc_rows}")

            by_id: dict[int, object] = {}
            for row in rpc_rows:
                if not isinstance(row, dict):
                    continue
                row_id = row.get("id")
                if isinstance(row_id, int):
                    by_id[row_id] = row

            for idx, address in enumerate(chunk):
                row = by_id.get(idx)
                if not isinstance(row, dict):
                    raise RuntimeError(f"missing RPC response row for {address}")
                error_obj = row.get("error")
                if error_obj is not None:
                    raise RuntimeError(f"eth_getCode failed for {address}: {error_obj}")
                code = str(row.get("result", "0x")).strip().lower()
                if code not in {"0x", "0x0", "0x00"}:
                    contracts.add(address)
    finally:
        client.close()

    return contracts


def build_customer_wallet_cohort(
    *,
    holders: Iterable[HolderBalance],
    minimum_balance_raw: int,
    strategy_wallets: set[str],
    protocol_wallets: set[str],
    contract_wallets: set[str],
) -> CohortBuildResult:
    """Apply deterministic holder filtering for customer cohort seeds."""

    if minimum_balance_raw < 0:
        raise ValueError("minimum_balance_raw must be non-negative")

    deduped: dict[str, int] = {}
    fetched_rows = 0
    for holder in holders:
        fetched_rows += 1
        address = canonical_address(holder.address)
        balance_raw = max(int(holder.balance_raw), 0)
        current = deduped.get(address)
        if current is None or balance_raw > current:
            deduped[address] = balance_raw

    threshold_balances = {
        address: balance for address, balance in deduped.items() if balance >= minimum_balance_raw
    }

    strategy_excluded_wallets = {
        address for address in threshold_balances if address in strategy_wallets
    }
    after_strategy = {
        address: balance
        for address, balance in threshold_balances.items()
        if address not in strategy_excluded_wallets
    }

    protocol_excluded_wallets = {
        address for address in after_strategy if address in protocol_wallets
    }
    after_protocol = {
        address: balance
        for address, balance in after_strategy.items()
        if address not in protocol_excluded_wallets
    }

    contract_excluded_wallets = {
        address for address in after_protocol if address in contract_wallets
    }
    final_balances = {
        address: balance
        for address, balance in after_protocol.items()
        if address not in contract_excluded_wallets
    }

    wallets = [
        HolderBalance(address=address, balance_raw=balance)
        for address, balance in sorted(
            final_balances.items(),
            key=lambda item: (-item[1], item[0]),
        )
    ]

    return CohortBuildResult(
        wallets=wallets,
        fetched_rows=fetched_rows,
        unique_rows=len(deduped),
        threshold_rows=len(threshold_balances),
        strategy_excluded=len(strategy_excluded_wallets),
        protocol_excluded=len(protocol_excluded_wallets),
        contract_excluded=len(contract_excluded_wallets),
    )


def build_wallet_cohort_config_payload(
    *,
    cohort_name: str,
    chain_code: str,
    chain_id: str,
    token_symbol: str,
    token_address: str,
    token_decimals: int,
    threshold_usd: Decimal,
    token_price_usd: Decimal,
    minimum_balance_raw: int,
    source_url: str,
    result: CohortBuildResult,
) -> dict[str, object]:
    """Build YAML-serializable cohort config payload."""

    scale = Decimal(10) ** token_decimals
    wallets_payload = []
    for wallet in result.wallets:
        balance_tokens = Decimal(wallet.balance_raw) / scale
        wallets_payload.append(
            {
                "address": wallet.address,
                "balance_raw": str(wallet.balance_raw),
                "balance_tokens": f"{balance_tokens:f}",
            }
        )

    return {
        "cohort": {
            "name": cohort_name,
            "chain": chain_code,
            "chain_id": str(chain_id),
            "token_symbol": token_symbol,
            "token_address": canonical_address(token_address),
            "token_decimals": token_decimals,
            "threshold_usd": f"{threshold_usd:f}",
            "token_price_usd_assumption": f"{token_price_usd:f}",
            "minimum_balance_raw": str(minimum_balance_raw),
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "source": {
                "provider": "routescan",
                "url": source_url,
                "fetched_rows": result.fetched_rows,
            },
            "filters": {
                "deduped_unique_rows": result.unique_rows,
                "threshold_rows": result.threshold_rows,
                "strategy_wallets_excluded": result.strategy_excluded,
                "protocol_wallets_excluded": result.protocol_excluded,
                "contract_wallets_excluded": result.contract_excluded,
            },
            "wallet_addresses": [wallet.address for wallet in result.wallets],
            "wallets": wallets_payload,
        }
    }


def _strip_0x_hex(value: str) -> str:
    cleaned = value.strip().lower()
    return cleaned[2:] if cleaned.startswith("0x") else cleaned


def _encode_address(value: str) -> str:
    return _strip_0x_hex(value).rjust(64, "0")


def _encode_uint(value: int) -> str:
    return hex(max(int(value), 0))[2:].rjust(64, "0")


def _encode_bytes32(value: str) -> str:
    return _strip_0x_hex(value).rjust(64, "0")


def _decode_words(raw_hex: str) -> list[int]:
    payload = _strip_0x_hex(raw_hex)
    if not payload:
        return []
    if len(payload) % 64 != 0:
        raise ValueError(f"invalid ABI payload length: {len(payload)}")
    words: list[int] = []
    for idx in range(0, len(payload), 64):
        words.append(int(payload[idx : idx + 64], 16))
    return words


def _decode_topic_address(topic_value: str | None) -> str | None:
    if not topic_value:
        return None
    cleaned = _strip_0x_hex(topic_value)
    if len(cleaned) != 64:
        return None
    return canonical_address(f"0x{cleaned[-40:]}")


@dataclass(frozen=True)
class VerifiedAvantBalance:
    """Verified wallet balance for one Avant token on one chain."""

    wallet_address: str
    chain_code: str
    token_address: str
    symbol: str
    asset_family: str
    wrapper_class: str
    pricing_policy: str
    balance_raw: int
    balance_amount: Decimal
    underlying_amount: Decimal | None
    usd_value: Decimal | None
    resolved: bool
    resolution_source: str | None
    error_type: str | None = None
    error_message: str | None = None


@dataclass(frozen=True)
class VerifiedCustomerWallet:
    """Verified cohort inputs for one wallet on one business date."""

    wallet_address: str
    verified_total_avant_usd: Decimal
    discovery_sources: tuple[str, ...]
    is_signoff_eligible: bool
    exclusion_reason: str | None
    balances: tuple[VerifiedAvantBalance, ...]
    observed_total_avant_usd: Decimal | None = None
    observed_additional_family_usd: dict[str, Decimal] = field(default_factory=dict)

    def family_total_usd(self, family_code: str) -> Decimal:
        return sum(
            (
                balance.usd_value or ZERO
                for balance in self.balances
                if balance.asset_family == family_code and balance.resolved
            ),
            ZERO,
        )

    def wrapper_total_usd(self, wrapper_class: str) -> Decimal:
        return sum(
            (
                balance.usd_value or ZERO
                for balance in self.balances
                if balance.wrapper_class == wrapper_class and balance.resolved
            ),
            ZERO,
        )

    def family_wrapper_total_usd(self, family_code: str, wrapper_class: str) -> Decimal:
        return sum(
            (
                balance.usd_value or ZERO
                for balance in self.balances
                if balance.asset_family == family_code
                and balance.wrapper_class == wrapper_class
                and balance.resolved
            ),
            ZERO,
        )

    def observed_total_usd(self) -> Decimal:
        if self.observed_total_avant_usd is not None:
            return self.observed_total_avant_usd
        return self.verified_total_avant_usd

    def observed_family_total_usd(self, family_code: str) -> Decimal:
        return self.family_total_usd(family_code) + self.observed_additional_family_usd.get(
            family_code,
            ZERO,
        )


@dataclass(frozen=True)
class HolderMarketPositionEvidence:
    """Same-day market-position evidence for one wallet in one configured avAsset market."""

    wallet_address: str
    chain_code: str
    protocol_code: str
    market_ref: str
    collateral_symbol: str
    collateral_token_address: str
    asset_family: str
    wrapper_class: str
    supplied_raw: int
    usd_value: Decimal


@dataclass(frozen=True)
class CustomerCohortSyncSummary:
    """Cohort build summary for CLI output."""

    business_date: date
    as_of_ts_utc: datetime
    candidate_wallet_count: int
    verified_wallet_count: int
    cohort_wallet_count: int
    signoff_eligible_wallet_count: int
    issues_written: int


@dataclass(frozen=True)
class CustomerCandidateDiscovery:
    """Same-day holder discovery output plus raw token holder ledgers."""

    candidate_sources: dict[str, set[str]]
    seed_sources: dict[str, set[str]]
    token_holders_by_token: dict[tuple[str, str, str], list[HolderBalance]]
    market_positions_by_wallet: dict[str, tuple[HolderMarketPositionEvidence, ...]]
    excluded_candidates: dict[str, list[str]]
    missing_market_coverage: list[dict[str, object]]
    issues: list[DataQualityIssue]


@dataclass(frozen=True)
class HolderDiscoveryMarket:
    """Configured avAsset-collateral market surface used for holder discovery."""

    protocol_code: str
    chain_code: str
    market_ref: str
    discovery_ref: str
    collateral_symbol: str
    collateral_token_address: str
    collateral_decimals: int
    borrow_token_address: str | None
    asset_family: str
    wrapper_class: str
    config_sources: tuple[str, ...] = field(default_factory=tuple)
    extra: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class MorphoWalletPosition:
    """Raw Morpho position components for one wallet."""

    supply_shares: int
    borrow_shares: int
    collateral: int


@dataclass(frozen=True)
class DebankHolderWalletSummary:
    """Observed DeBank activity for one candidate holder wallet."""

    fetch_succeeded: bool
    fetch_error_message: str | None
    has_any_activity: bool
    has_any_borrow: bool
    has_configured_surface_activity: bool
    protocol_count: int
    chain_count: int
    configured_protocol_count: int
    total_supply_usd: Decimal
    total_borrow_usd: Decimal
    configured_surface_supply_usd: Decimal
    configured_surface_borrow_usd: Decimal
    avasset_supply_total_usd: Decimal
    avasset_supply_by_family: dict[str, Decimal] = field(default_factory=dict)


class EvmBatchRpcClient:
    """Minimal batched JSON-RPC client for ERC20 balance and share conversion reads."""

    def __init__(self, rpc_urls: dict[str, str], timeout_seconds: float = 20.0) -> None:
        self.rpc_urls = {chain: url for chain, url in rpc_urls.items() if url}
        self._client = httpx.Client(timeout=timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def _rpc_url(self, chain_code: str) -> str:
        rpc_url = self.rpc_urls.get(chain_code)
        if not rpc_url:
            raise ValueError(f"missing RPC URL for chain '{chain_code}'")
        return rpc_url

    def _rpc(self, *, chain_code: str, method: str, params: list[object]) -> object:
        response = self._client.post(
            self._rpc_url(chain_code),
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            raise RuntimeError(f"unexpected RPC response for {method}: {payload}")
        if payload.get("error") is not None:
            raise RuntimeError(str(payload["error"]))
        return payload.get("result")

    def _batched_eth_call(
        self,
        *,
        chain_code: str,
        calls: list[tuple[str, str, str]],
        batch_size: int = 100,
    ) -> dict[str, str | None]:
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")

        rpc_url = self._rpc_url(chain_code)
        results: dict[str, str | None] = {}
        for offset in range(0, len(calls), batch_size):
            chunk = calls[offset : offset + batch_size]
            payload = [
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_call",
                    "params": [{"to": target, "data": data}, "latest"],
                }
                for request_id, target, data in chunk
            ]
            response = self._client.post(rpc_url, json=payload)
            response.raise_for_status()
            rpc_rows = response.json()
            if isinstance(rpc_rows, dict):
                rpc_rows = [rpc_rows]
            if not isinstance(rpc_rows, list):
                raise RuntimeError(f"unexpected RPC batch response: {rpc_rows}")
            by_id = {str(row.get("id")): row for row in rpc_rows if isinstance(row, dict)}
            for request_id, _target, _data in chunk:
                row = by_id.get(request_id)
                if row is None or row.get("error") is not None:
                    results[request_id] = None
                    continue
                result = row.get("result")
                results[request_id] = str(result) if isinstance(result, str) else None
        return results

    def read_erc20_balances(
        self,
        *,
        chain_code: str,
        token_address: str,
        wallet_addresses: list[str],
    ) -> dict[str, int | None]:
        calls = [
            (
                wallet_address,
                canonical_address(token_address),
                f"0x{ERC20_BALANCE_OF_SELECTOR}{_encode_address(wallet_address)}",
            )
            for wallet_address in wallet_addresses
        ]
        raw_results = self._batched_eth_call(chain_code=chain_code, calls=calls)
        return {
            wallet_address: (int(result, 16) if result is not None else None)
            for wallet_address, result in raw_results.items()
        }

    def convert_to_assets(
        self,
        *,
        chain_code: str,
        vault_address: str,
        shares_by_wallet: dict[str, int],
    ) -> dict[str, int | None]:
        calls = [
            (
                wallet_address,
                canonical_address(vault_address),
                f"0x{ERC4626_CONVERT_TO_ASSETS_SELECTOR}{_encode_uint(shares)}",
            )
            for wallet_address, shares in shares_by_wallet.items()
            if shares > 0
        ]
        raw_results = self._batched_eth_call(chain_code=chain_code, calls=calls)
        return {
            wallet_address: (int(result, 16) if result is not None else None)
            for wallet_address, result in raw_results.items()
        }

    def get_block_number(self, *, chain_code: str) -> int:
        result = self._rpc(chain_code=chain_code, method="eth_blockNumber", params=[])
        if not isinstance(result, str):
            raise RuntimeError(f"unexpected eth_blockNumber response: {result}")
        return int(result, 16)

    def get_logs(
        self,
        *,
        chain_code: str,
        address: str,
        topics: list[str | None],
        from_block: int = 0,
        to_block: int | None = None,
        chunk_size: int = 1_000_000,
    ) -> list[dict[str, object]]:
        if from_block < 0:
            raise ValueError("from_block must be >= 0")
        normalized_address = canonical_address(address)
        latest_block = self.get_block_number(chain_code=chain_code) if to_block is None else to_block
        if latest_block < from_block:
            return []

        request = {
            "address": normalized_address,
            "topics": topics,
            "fromBlock": hex(from_block),
            "toBlock": hex(latest_block),
        }
        try:
            result = self._rpc(chain_code=chain_code, method="eth_getLogs", params=[request])
            if not isinstance(result, list):
                raise RuntimeError(f"unexpected eth_getLogs response: {result}")
            return [row for row in result if isinstance(row, dict)]
        except Exception:
            if chunk_size <= 0:
                raise

        rows: list[dict[str, object]] = []
        for start_block in range(from_block, latest_block + 1, chunk_size):
            end_block = min(start_block + chunk_size - 1, latest_block)
            result = self._rpc(
                chain_code=chain_code,
                method="eth_getLogs",
                params=[
                    {
                        "address": normalized_address,
                        "topics": topics,
                        "fromBlock": hex(start_block),
                        "toBlock": hex(end_block),
                    }
                ],
            )
            if not isinstance(result, list):
                raise RuntimeError(f"unexpected eth_getLogs response: {result}")
            rows.extend(row for row in result if isinstance(row, dict))
        return rows

    def read_morpho_positions(
        self,
        *,
        chain_code: str,
        morpho_address: str,
        market_id: str,
        wallet_addresses: list[str],
    ) -> dict[str, MorphoWalletPosition | None]:
        calls = [
            (
                wallet_address,
                canonical_address(morpho_address),
                (
                    f"0x{MORPHO_POSITION_SELECTOR}"
                    f"{_encode_bytes32(market_id)}"
                    f"{_encode_address(wallet_address)}"
                ),
            )
            for wallet_address in wallet_addresses
        ]
        raw_results = self._batched_eth_call(chain_code=chain_code, calls=calls)
        decoded: dict[str, MorphoWalletPosition | None] = {}
        for wallet_address, result in raw_results.items():
            if result is None:
                decoded[wallet_address] = None
                continue
            words = _decode_words(result)
            if len(words) < 3:
                decoded[wallet_address] = None
                continue
            decoded[wallet_address] = MorphoWalletPosition(
                supply_shares=words[0],
                borrow_shares=words[1],
                collateral=words[2],
            )
        return decoded


def active_avant_tokens(
    avant_tokens: AvantTokensConfig,
    *,
    business_date: date,
    chain_scope: set[str] | None = None,
) -> list[AvantToken]:
    """Return registry tokens active for the requested business date."""

    active: list[AvantToken] = []
    for token in avant_tokens.tokens:
        if chain_scope is not None and token.chain_code not in chain_scope:
            continue
        if token.active_from is not None and business_date < token.active_from:
            continue
        if token.active_to is not None and business_date > token.active_to:
            continue
        active.append(token)
    return active


def _avant_symbol_registry(
    *,
    avant_tokens: AvantTokensConfig,
    business_date: date,
) -> tuple[dict[str, tuple[str, str]], list[tuple[str, tuple[str, str]]]]:
    by_symbol: dict[str, tuple[str, str]] = {}
    sorted_symbols: list[tuple[str, tuple[str, str]]] = []
    seen: set[str] = set()
    for token in active_avant_tokens(avant_tokens, business_date=business_date):
        symbol_key = token.symbol.strip().upper()
        if not symbol_key:
            continue
        by_symbol.setdefault(symbol_key, (token.asset_family, token.wrapper_class))
        if symbol_key in seen:
            continue
        seen.add(symbol_key)
        sorted_symbols.append((symbol_key, (token.asset_family, token.wrapper_class)))
    sorted_symbols.sort(key=lambda item: len(item[0]), reverse=True)
    return by_symbol, sorted_symbols


def _family_and_wrapper_for_token_symbol(
    token_symbol: str,
    *,
    registry_by_symbol: dict[str, tuple[str, str]],
    registry_symbols_sorted: list[tuple[str, tuple[str, str]]],
) -> tuple[str | None, str | None]:
    normalized = token_symbol.strip().upper()
    if not normalized:
        return None, None
    direct_match = registry_by_symbol.get(normalized)
    if direct_match is not None:
        return direct_match
    for symbol_key, value in registry_symbols_sorted:
        if symbol_key in normalized:
            return value
    return None, None


def _to_debank_string(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else None
    return str(value)


def _to_debank_decimal(value: object) -> Decimal | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int | float | str):
        try:
            return Decimal(str(value))
        except Exception:
            return None
    return None


def _debank_leg_type_from_detail_key(detail_key: str) -> str | None:
    lower = detail_key.lower()
    if "borrow" in lower:
        return "borrow"
    if "supply" in lower or "deposit" in lower or "collateral" in lower:
        return "supply"
    return None


def load_legacy_customer_seed_wallets(
    config_dir: Path,
    *,
    globs: Iterable[str] | None = None,
    force_include_wallets: Iterable[str] | None = None,
) -> set[str]:
    """Load manually-seeded customer wallets from legacy cohort YAML files."""

    wallets: set[str] = set()
    globs = tuple(globs or ("consumer_wallets_*.yaml",))
    for pattern in globs:
        for path in sorted(config_dir.glob(pattern)):
            with path.open("r", encoding="utf-8") as handle:
                payload = yaml.safe_load(handle) or {}
            if not isinstance(payload, dict):
                continue
            cohort = payload.get("cohort")
            if not isinstance(cohort, dict):
                continue
            wallet_addresses = cohort.get("wallet_addresses")
            if isinstance(wallet_addresses, list):
                for item in wallet_addresses:
                    if isinstance(item, str) and EVM_ADDRESS_RE.fullmatch(item.strip()):
                        wallets.add(canonical_address(item))
            wallet_rows = cohort.get("wallets")
            if isinstance(wallet_rows, list):
                for row in wallet_rows:
                    if not isinstance(row, dict):
                        continue
                    address = row.get("address")
                    if isinstance(address, str) and EVM_ADDRESS_RE.fullmatch(address.strip()):
                        wallets.add(canonical_address(address))

    for wallet in force_include_wallets or ():
        if isinstance(wallet, str) and EVM_ADDRESS_RE.fullmatch(wallet.strip()):
            wallets.add(canonical_address(wallet))

    return wallets


def excluded_holder_wallets(holder_exclusions: HolderExclusionsConfig) -> set[str]:
    """Return explicitly excluded wallets that should not enter monitored holder cohorts."""

    return {
        canonical_address(exclusion.address)
        for exclusion in holder_exclusions.exclusions
        if exclusion.exclude_from_monitoring
    }


def _holder_exclusion_lookup(
    holder_exclusions: HolderExclusionsConfig,
) -> dict[tuple[str | None, str], object]:
    return {
        (exclusion.chain_code, canonical_address(exclusion.address)): exclusion
        for exclusion in holder_exclusions.exclusions
    }


def write_consumer_token_holder_daily(
    *,
    session: Session,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_ids: dict[str, int],
    token_holders_by_token: dict[tuple[str, str, str], list[HolderBalance]],
    price_map: dict[tuple[str, str], Decimal],
    avant_tokens: AvantTokensConfig,
    strategy_wallets: set[str],
    protocol_wallets: set[str],
    contract_wallets: set[str],
    holder_exclusions: HolderExclusionsConfig,
    holder_universe: HolderUniverseConfig,
) -> int:
    """Replace persisted raw holder-ledger rows for all active holder-universe tokens."""

    session.execute(
        delete(ConsumerTokenHolderDaily).where(
            ConsumerTokenHolderDaily.business_date == business_date
        )
    )
    if not token_holders_by_token:
        return 0

    exclusion_lookup = _holder_exclusion_lookup(holder_exclusions)
    internal_wallets = {
        canonical_address(address)
        for address in session.scalars(
            select(Wallet.address).where(Wallet.wallet_type == "internal")
        ).all()
    }
    token_registry = {
        (token.chain_code, canonical_address(token.token_address)): token
        for token in active_avant_tokens(
            avant_tokens,
            business_date=business_date,
            chain_scope=set(holder_universe.chain_id_map()),
        )
    }
    rows: list[dict[str, object]] = []
    for (chain_code, token_address, token_symbol), holders in token_holders_by_token.items():
        token = token_registry.get((chain_code, token_address))
        if token is None:
            continue
        token_price = price_map.get((chain_code, token_address), ZERO)
        scale = Decimal(10) ** token.decimals
        for holder in holders:
            wallet_address = canonical_address(holder.address)
            wallet_id = wallet_ids.get(wallet_address)
            if wallet_id is None:
                continue

            classification = "customer"
            exclude_from_monitoring = False
            exclude_from_customer_float = False

            if wallet_address in strategy_wallets:
                classification = "strategy"
                exclude_from_monitoring = True
                exclude_from_customer_float = True
            elif wallet_address in internal_wallets:
                classification = "internal"
                exclude_from_monitoring = True
                exclude_from_customer_float = True
            else:
                explicit = exclusion_lookup.get(
                    (chain_code, wallet_address)
                ) or exclusion_lookup.get((None, wallet_address))
                if explicit is not None:
                    classification = explicit.classification
                    exclude_from_monitoring = explicit.exclude_from_monitoring
                    exclude_from_customer_float = explicit.exclude_from_customer_float
                elif wallet_address in protocol_wallets:
                    classification = "protocol"
                    exclude_from_monitoring = holder_universe.exclude_protocol_wallets_by_default
                    exclude_from_customer_float = False
                elif wallet_address in contract_wallets:
                    classification = "protocol"
                    exclude_from_monitoring = holder_universe.exclude_contract_wallets_by_default
                    exclude_from_customer_float = False

            balance_tokens = Decimal(holder.balance_raw) / scale
            rows.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": as_of_ts_utc,
                    "chain_code": chain_code,
                    "token_symbol": token_symbol,
                    "token_address": token_address,
                    "wallet_id": wallet_id,
                    "wallet_address": wallet_address,
                    "balance_tokens": balance_tokens,
                    "usd_value": balance_tokens * token_price,
                    "holder_class": classification,
                    "exclude_from_monitoring": exclude_from_monitoring,
                    "exclude_from_customer_float": exclude_from_customer_float,
                    "source_provider": "routescan",
                }
            )

    if not rows:
        return 0
    session.execute(insert(ConsumerTokenHolderDaily).values(rows))
    return len(rows)


def _add_candidate_sources(
    candidate_sources: dict[str, set[str]],
    wallet_address: str,
    *sources: str,
) -> None:
    normalized = canonical_address(wallet_address)
    bucket = candidate_sources[normalized]
    for source in sources:
        if source:
            bucket.add(source)


def _chain_codes_from_discovery_sources(sources: Iterable[str]) -> set[str]:
    chain_codes: set[str] = set()
    for source in sources:
        parts = source.split(":")
        if len(parts) >= 2 and parts[0] == "routescan_holder":
            chain_codes.add(parts[1])
        elif len(parts) >= 3 and parts[0] == "market_position":
            chain_codes.add(parts[2])
    return chain_codes


def _active_avant_token_lookups(
    *,
    avant_tokens: AvantTokensConfig,
    business_date: date,
) -> tuple[dict[tuple[str, str], AvantToken], dict[tuple[str, str], AvantToken]]:
    by_address: dict[tuple[str, str], AvantToken] = {}
    by_symbol: dict[tuple[str, str], AvantToken] = {}
    for token in active_avant_tokens(avant_tokens, business_date=business_date):
        by_address[(token.chain_code, canonical_address(token.token_address))] = token
        by_symbol[(token.chain_code, token.symbol.strip().upper())] = token
    return by_address, by_symbol


def _matching_active_avant_token(
    *,
    chain_code: str,
    token_address: str | None,
    token_symbol: str | None,
    registry_by_address: dict[tuple[str, str], AvantToken],
    registry_by_symbol: dict[tuple[str, str], AvantToken],
) -> AvantToken | None:
    if token_address:
        normalized = token_address.strip()
        if EVM_ADDRESS_RE.fullmatch(normalized):
            match = registry_by_address.get((chain_code, canonical_address(normalized)))
            if match is not None:
                return match
    if token_symbol:
        return registry_by_symbol.get((chain_code, token_symbol.strip().upper()))
    return None


def _build_holder_seed_sources(
    *,
    session: Session,
    business_date: date,
    manual_seed_wallets: set[str],
) -> dict[str, set[str]]:
    seed_sources: dict[str, set[str]] = defaultdict(set)

    previous_date = session.scalar(
        select(func.max(ConsumerHolderUniverseDaily.business_date)).where(
            ConsumerHolderUniverseDaily.business_date < business_date
        )
    )
    if previous_date is not None:
        for wallet_address in session.scalars(
            select(ConsumerHolderUniverseDaily.wallet_address).where(
                ConsumerHolderUniverseDaily.business_date == previous_date
            )
        ).all():
            _add_candidate_sources(seed_sources, wallet_address, "prior_surface")

    for wallet_address in manual_seed_wallets:
        _add_candidate_sources(seed_sources, wallet_address, "legacy_seed")

    _, end_utc = denver_business_bounds_utc(business_date)
    for wallet_address in session.scalars(
        select(Wallet.address)
        .join(PositionSnapshot, PositionSnapshot.wallet_id == Wallet.wallet_id)
        .join(Market, Market.market_id == PositionSnapshot.market_id)
        .where(
            Market.market_kind == "consumer_market",
            PositionSnapshot.as_of_ts_utc <= end_utc,
        )
        .distinct()
    ).all():
        _add_candidate_sources(seed_sources, wallet_address, "canonical_activity")

    return seed_sources


def _discover_direct_holder_wallets(
    *,
    business_date: date,
    as_of_ts_utc: datetime,
    avant_tokens: AvantTokensConfig,
    holder_universe: HolderUniverseConfig,
    routescan_client: RouteScanClient,
    holder_limit_per_token: int,
) -> tuple[
    dict[str, set[str]],
    dict[tuple[str, str, str], list[HolderBalance]],
    list[DataQualityIssue],
]:
    candidate_sources: dict[str, set[str]] = defaultdict(set)
    token_holders_by_token: dict[tuple[str, str, str], list[HolderBalance]] = {}
    issues: list[DataQualityIssue] = []
    chain_id_map = holder_universe.chain_id_map()

    for token in active_avant_tokens(
        avant_tokens,
        business_date=business_date,
        chain_scope=set(chain_id_map),
    ):
        chain_id = chain_id_map.get(token.chain_code)
        if chain_id is None:
            continue
        try:
            holders = routescan_client.get_erc20_holders(
                chain_id=chain_id,
                token_address=token.token_address,
                limit=holder_limit_per_token,
                max_rows=None,
            )
        except Exception as exc:
            issues.append(
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_consumer_cohort",
                    error_type="holder_discovery_failed",
                    error_message=str(exc),
                    chain_code=token.chain_code,
                    market_ref=token.token_address,
                    payload_json={"symbol": token.symbol},
                )
            )
            continue

        token_holders_by_token[
            (token.chain_code, canonical_address(token.token_address), token.symbol)
        ] = holders
        for holder in holders:
            _add_candidate_sources(
                candidate_sources,
                holder.address,
                "routescan_holder",
                f"routescan_holder:{token.chain_code}:{token.symbol}",
            )

    return candidate_sources, token_holders_by_token, issues


def _register_holder_discovery_market(
    registry: dict[tuple[str, str, str], dict[str, object]],
    *,
    protocol_code: str,
    chain_code: str,
    market_ref: str,
    discovery_ref: str,
    collateral_symbol: str,
    collateral_token_address: str,
    collateral_decimals: int,
    borrow_token_address: str | None,
    asset_family: str,
    wrapper_class: str,
    config_source: str,
    extra: dict[str, object] | None = None,
) -> None:
    key = (protocol_code, chain_code, market_ref)
    current = registry.get(key)
    if current is None:
        registry[key] = {
            "protocol_code": protocol_code,
            "chain_code": chain_code,
            "market_ref": market_ref,
            "discovery_ref": discovery_ref,
            "collateral_symbol": collateral_symbol,
            "collateral_token_address": collateral_token_address,
            "collateral_decimals": collateral_decimals,
            "borrow_token_address": borrow_token_address,
            "asset_family": asset_family,
            "wrapper_class": wrapper_class,
            "config_sources": {config_source},
            "extra": dict(extra or {}),
        }
        return

    current["config_sources"].add(config_source)
    if extra:
        current_extra = current.setdefault("extra", {})
        if isinstance(current_extra, dict):
            current_extra.update(extra)


def _build_holder_discovery_markets(
    *,
    business_date: date,
    avant_tokens: AvantTokensConfig,
    consumer_markets_config: ConsumerMarketsConfig,
    markets_config: MarketsConfig,
) -> list[HolderDiscoveryMarket]:
    registry_by_address, registry_by_symbol = _active_avant_token_lookups(
        avant_tokens=avant_tokens,
        business_date=business_date,
    )
    market_rows: dict[tuple[str, str, str], dict[str, object]] = {}

    for market in consumer_markets_config.markets:
        chain_code = market.chain.strip().lower()
        protocol_code = market.protocol.strip().lower()
        token = _matching_active_avant_token(
            chain_code=chain_code,
            token_address=market.collateral_token.address,
            token_symbol=market.collateral_token.symbol,
            registry_by_address=registry_by_address,
            registry_by_symbol=registry_by_symbol,
        )
        if token is None:
            continue
        discovery_ref = market.market_address.split("/", 1)[0] if protocol_code == "euler_v2" else market.market_address
        _register_holder_discovery_market(
            market_rows,
            protocol_code=protocol_code,
            chain_code=chain_code,
            market_ref=market.market_address,
            discovery_ref=discovery_ref,
            collateral_symbol=token.symbol,
            collateral_token_address=canonical_address(token.token_address),
            collateral_decimals=token.decimals,
            borrow_token_address=(
                canonical_address(market.borrow_token.address)
                if EVM_ADDRESS_RE.fullmatch(market.borrow_token.address)
                else None
            ),
            asset_family=token.asset_family,
            wrapper_class=token.wrapper_class,
            config_source="consumer_markets",
        )

    for chain_code, chain_config in markets_config.morpho.items():
        for market in chain_config.markets:
            token = _matching_active_avant_token(
                chain_code=chain_code,
                token_address=market.collateral_token_address,
                token_symbol=market.collateral_token,
                registry_by_address=registry_by_address,
                registry_by_symbol=registry_by_symbol,
            )
            if token is None:
                continue
            _register_holder_discovery_market(
                market_rows,
                protocol_code="morpho",
                chain_code=chain_code,
                market_ref=market.id,
                discovery_ref=market.id,
                collateral_symbol=token.symbol,
                collateral_token_address=canonical_address(token.token_address),
                collateral_decimals=token.decimals,
                borrow_token_address=(
                    canonical_address(market.loan_token_address)
                    if market.loan_token_address is not None
                    else None
                ),
                asset_family=token.asset_family,
                wrapper_class=token.wrapper_class,
                config_source="markets",
                extra={"morpho_address": canonical_address(chain_config.morpho)},
            )

    for chain_code, chain_config in markets_config.euler_v2.items():
        for vault in chain_config.vaults:
            token = _matching_active_avant_token(
                chain_code=chain_code,
                token_address=vault.asset_address,
                token_symbol=vault.asset_symbol,
                registry_by_address=registry_by_address,
                registry_by_symbol=registry_by_symbol,
            )
            if token is None:
                continue
            _register_holder_discovery_market(
                market_rows,
                protocol_code="euler_v2",
                chain_code=chain_code,
                market_ref=vault.address,
                discovery_ref=vault.address,
                collateral_symbol=token.symbol,
                collateral_token_address=canonical_address(token.token_address),
                collateral_decimals=token.decimals,
                borrow_token_address=None,
                asset_family=token.asset_family,
                wrapper_class=token.wrapper_class,
                config_source="markets",
            )

    for chain_code, chain_config in markets_config.dolomite.items():
        for market in chain_config.markets:
            token = _matching_active_avant_token(
                chain_code=chain_code,
                token_address=market.token_address,
                token_symbol=market.symbol,
                registry_by_address=registry_by_address,
                registry_by_symbol=registry_by_symbol,
            )
            if token is None:
                continue
            _register_holder_discovery_market(
                market_rows,
                protocol_code="dolomite",
                chain_code=chain_code,
                market_ref=str(market.id),
                discovery_ref=str(market.id),
                collateral_symbol=token.symbol,
                collateral_token_address=canonical_address(token.token_address),
                collateral_decimals=token.decimals,
                borrow_token_address=None,
                asset_family=token.asset_family,
                wrapper_class=token.wrapper_class,
                config_source="markets",
                extra={"token_address": canonical_address(token.token_address)},
            )

    markets: list[HolderDiscoveryMarket] = []
    for row in market_rows.values():
        markets.append(
            HolderDiscoveryMarket(
                protocol_code=str(row["protocol_code"]),
                chain_code=str(row["chain_code"]),
                market_ref=str(row["market_ref"]),
                discovery_ref=str(row["discovery_ref"]),
                collateral_symbol=str(row["collateral_symbol"]),
                collateral_token_address=str(row["collateral_token_address"]),
                collateral_decimals=int(row["collateral_decimals"]),
                borrow_token_address=(
                    str(row["borrow_token_address"])
                    if row["borrow_token_address"] is not None
                    else None
                ),
                asset_family=str(row["asset_family"]),
                wrapper_class=str(row["wrapper_class"]),
                config_sources=tuple(sorted(row["config_sources"])),
                extra=dict(row.get("extra", {})),
            )
        )
    markets.sort(key=lambda row: (row.protocol_code, row.chain_code, row.market_ref))
    return markets


def _record_market_position_evidence(
    positions_by_wallet: dict[str, list[HolderMarketPositionEvidence]],
    candidate_sources: dict[str, set[str]],
    seen_keys: set[tuple[str, str, str, str]],
    *,
    wallet_address: str,
    market: HolderDiscoveryMarket,
    supplied_raw: int,
    usd_value: Decimal,
) -> None:
    if supplied_raw <= 0 or usd_value <= ZERO:
        return
    normalized_wallet = canonical_address(wallet_address)
    evidence_key = (
        normalized_wallet,
        market.protocol_code,
        market.chain_code,
        market.market_ref,
    )
    if evidence_key in seen_keys:
        return
    seen_keys.add(evidence_key)
    positions_by_wallet[normalized_wallet].append(
        HolderMarketPositionEvidence(
            wallet_address=normalized_wallet,
            chain_code=market.chain_code,
            protocol_code=market.protocol_code,
            market_ref=market.market_ref,
            collateral_symbol=market.collateral_symbol,
            collateral_token_address=market.collateral_token_address,
            asset_family=market.asset_family,
            wrapper_class=market.wrapper_class,
            supplied_raw=supplied_raw,
            usd_value=usd_value,
        )
    )
    _add_candidate_sources(
        candidate_sources,
        normalized_wallet,
        "market_position",
        f"market_position:{market.protocol_code}:{market.chain_code}:{market.market_ref}",
    )


def _discover_silo_market_positions(
    *,
    as_of_ts_utc: datetime,
    markets: list[HolderDiscoveryMarket],
    seed_wallets: set[str],
    price_map: dict[tuple[str, str], Decimal],
    holder_universe: HolderUniverseConfig,
    silo_client: SiloApiClient | None,
    top_holders_limit: int,
) -> tuple[dict[str, list[HolderMarketPositionEvidence]], dict[str, set[str]], list[DataQualityIssue]]:
    positions_by_wallet: dict[str, list[HolderMarketPositionEvidence]] = defaultdict(list)
    candidate_sources: dict[str, set[str]] = defaultdict(set)
    issues: list[DataQualityIssue] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    if silo_client is None:
        return positions_by_wallet, candidate_sources, issues

    for market in markets:
        price = price_map.get((market.chain_code, market.collateral_token_address))
        if price is None:
            issues.append(
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_consumer_cohort",
                    error_type="market_position_price_missing",
                    error_message="no price available for holder market discovery",
                    chain_code=market.chain_code,
                    market_ref=market.market_ref,
                    payload_json={"protocol_code": market.protocol_code},
                )
            )
            continue

        try:
            top_holders = silo_client.get_top_holders(
                chain_code=market.chain_code,
                market_ref=market.market_ref,
                limit=top_holders_limit,
            )
        except Exception as exc:
            issues.append(
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_consumer_cohort",
                    error_type="market_position_discovery_failed",
                    error_message=str(exc),
                    chain_code=market.chain_code,
                    market_ref=market.market_ref,
                    payload_json={"protocol_code": market.protocol_code},
                )
            )
            top_holders = []

        scale = Decimal(10) ** market.collateral_decimals
        for holder in top_holders:
            usd_value = (Decimal(holder.supplied_raw) / scale) * price
            if usd_value < holder_universe.raw_holder_threshold_usd:
                continue
            _record_market_position_evidence(
                positions_by_wallet,
                candidate_sources,
                seen_keys,
                wallet_address=holder.wallet_address,
                market=market,
                supplied_raw=holder.supplied_raw,
                usd_value=usd_value,
            )

        if market.borrow_token_address is None:
            continue
        for wallet_address in sorted(seed_wallets):
            try:
                wallet_position = silo_client.get_wallet_position(
                    chain_code=market.chain_code,
                    market_ref=market.market_ref,
                    wallet_address=wallet_address,
                    collateral_token_address=market.collateral_token_address,
                    borrow_token_address=market.borrow_token_address,
                )
            except Exception:
                continue
            if wallet_position.supplied_raw <= 0:
                continue
            usd_value = (Decimal(wallet_position.supplied_raw) / scale) * price
            if usd_value < holder_universe.raw_holder_threshold_usd:
                continue
            _record_market_position_evidence(
                positions_by_wallet,
                candidate_sources,
                seen_keys,
                wallet_address=wallet_address,
                market=market,
                supplied_raw=wallet_position.supplied_raw,
                usd_value=usd_value,
            )

    return positions_by_wallet, candidate_sources, issues


def _discover_euler_market_positions(
    *,
    as_of_ts_utc: datetime,
    markets: list[HolderDiscoveryMarket],
    routescan_client: RouteScanClient,
    rpc_client: EvmBatchRpcClient,
    price_map: dict[tuple[str, str], Decimal],
    holder_universe: HolderUniverseConfig,
    holder_limit_per_token: int,
) -> tuple[dict[str, list[HolderMarketPositionEvidence]], dict[str, set[str]], list[DataQualityIssue]]:
    positions_by_wallet: dict[str, list[HolderMarketPositionEvidence]] = defaultdict(list)
    candidate_sources: dict[str, set[str]] = defaultdict(set)
    issues: list[DataQualityIssue] = []
    seen_keys: set[tuple[str, str, str, str]] = set()
    chain_id_map = holder_universe.chain_id_map()

    for market in markets:
        chain_id = chain_id_map.get(market.chain_code)
        price = price_map.get((market.chain_code, market.collateral_token_address))
        if chain_id is None or price is None:
            continue
        try:
            holders = routescan_client.get_erc20_holders(
                chain_id=chain_id,
                token_address=market.discovery_ref,
                limit=holder_limit_per_token,
                max_rows=None,
            )
        except Exception as exc:
            issues.append(
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_consumer_cohort",
                    error_type="market_position_discovery_failed",
                    error_message=str(exc),
                    chain_code=market.chain_code,
                    market_ref=market.market_ref,
                    payload_json={"protocol_code": market.protocol_code},
                )
            )
            continue
        shares_by_wallet = {
            canonical_address(holder.address): holder.balance_raw
            for holder in holders
            if holder.balance_raw > 0
        }
        if not shares_by_wallet:
            continue
        converted = rpc_client.convert_to_assets(
            chain_code=market.chain_code,
            vault_address=market.discovery_ref,
            shares_by_wallet=shares_by_wallet,
        )
        scale = Decimal(10) ** market.collateral_decimals
        for wallet_address, supplied_raw in converted.items():
            if supplied_raw is None or supplied_raw <= 0:
                continue
            usd_value = (Decimal(supplied_raw) / scale) * price
            if usd_value < holder_universe.raw_holder_threshold_usd:
                continue
            _record_market_position_evidence(
                positions_by_wallet,
                candidate_sources,
                seen_keys,
                wallet_address=wallet_address,
                market=market,
                supplied_raw=supplied_raw,
                usd_value=usd_value,
            )

    return positions_by_wallet, candidate_sources, issues


def _discover_morpho_market_positions(
    *,
    as_of_ts_utc: datetime,
    markets: list[HolderDiscoveryMarket],
    rpc_client: EvmBatchRpcClient,
    price_map: dict[tuple[str, str], Decimal],
    holder_universe: HolderUniverseConfig,
) -> tuple[dict[str, list[HolderMarketPositionEvidence]], dict[str, set[str]], list[DataQualityIssue]]:
    positions_by_wallet: dict[str, list[HolderMarketPositionEvidence]] = defaultdict(list)
    candidate_sources: dict[str, set[str]] = defaultdict(set)
    issues: list[DataQualityIssue] = []
    seen_keys: set[tuple[str, str, str, str]] = set()

    for market in markets:
        morpho_address_obj = market.extra.get("morpho_address")
        morpho_address = (
            canonical_address(str(morpho_address_obj))
            if morpho_address_obj is not None
            else None
        )
        price = price_map.get((market.chain_code, market.collateral_token_address))
        if morpho_address is None or price is None:
            continue

        try:
            supply_logs = rpc_client.get_logs(
                chain_code=market.chain_code,
                address=morpho_address,
                topics=[
                    MORPHO_SUPPLY_COLLATERAL_TOPIC,
                    "0x" + _encode_bytes32(market.discovery_ref),
                    None,
                    None,
                ],
            )
            withdraw_logs = rpc_client.get_logs(
                chain_code=market.chain_code,
                address=morpho_address,
                topics=[
                    MORPHO_WITHDRAW_COLLATERAL_TOPIC,
                    "0x" + _encode_bytes32(market.discovery_ref),
                    None,
                    None,
                ],
            )
        except Exception as exc:
            issues.append(
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_consumer_cohort",
                    error_type="market_position_discovery_failed",
                    error_message=str(exc),
                    chain_code=market.chain_code,
                    market_ref=market.market_ref,
                    payload_json={"protocol_code": market.protocol_code},
                )
            )
            continue

        candidate_wallets: set[str] = set()
        for log_row in supply_logs:
            topics = log_row.get("topics")
            if isinstance(topics, list) and len(topics) >= 4:
                wallet_address = _decode_topic_address(str(topics[3]))
                if wallet_address is not None:
                    candidate_wallets.add(wallet_address)
        for log_row in withdraw_logs:
            topics = log_row.get("topics")
            if isinstance(topics, list) and len(topics) >= 3:
                wallet_address = _decode_topic_address(str(topics[2]))
                if wallet_address is not None:
                    candidate_wallets.add(wallet_address)

        if not candidate_wallets:
            continue

        positions = rpc_client.read_morpho_positions(
            chain_code=market.chain_code,
            morpho_address=morpho_address,
            market_id=market.discovery_ref,
            wallet_addresses=sorted(candidate_wallets),
        )
        scale = Decimal(10) ** market.collateral_decimals
        for wallet_address, position in positions.items():
            if position is None or position.collateral <= 0:
                continue
            usd_value = (Decimal(position.collateral) / scale) * price
            if usd_value < holder_universe.raw_holder_threshold_usd:
                continue
            _record_market_position_evidence(
                positions_by_wallet,
                candidate_sources,
                seen_keys,
                wallet_address=wallet_address,
                market=market,
                supplied_raw=position.collateral,
                usd_value=usd_value,
            )

    return positions_by_wallet, candidate_sources, issues


def _discover_market_position_evidence(
    *,
    as_of_ts_utc: datetime,
    business_date: date,
    avant_tokens: AvantTokensConfig,
    consumer_markets_config: ConsumerMarketsConfig,
    markets_config: MarketsConfig,
    routescan_client: RouteScanClient,
    rpc_client: EvmBatchRpcClient,
    price_map: dict[tuple[str, str], Decimal],
    holder_universe: HolderUniverseConfig,
    seed_sources: dict[str, set[str]],
    silo_client: SiloApiClient | None,
    holder_limit_per_token: int,
    silo_top_holders_limit: int,
) -> tuple[
    dict[str, tuple[HolderMarketPositionEvidence, ...]],
    dict[str, set[str]],
    list[dict[str, object]],
    list[DataQualityIssue],
]:
    configured_markets = _build_holder_discovery_markets(
        business_date=business_date,
        avant_tokens=avant_tokens,
        consumer_markets_config=consumer_markets_config,
        markets_config=markets_config,
    )
    by_protocol: dict[str, list[HolderDiscoveryMarket]] = defaultdict(list)
    for market in configured_markets:
        by_protocol[market.protocol_code].append(market)

    missing_market_coverage: list[dict[str, object]] = []
    positions_by_wallet: dict[str, list[HolderMarketPositionEvidence]] = defaultdict(list)
    candidate_sources: dict[str, set[str]] = defaultdict(set)
    issues: list[DataQualityIssue] = []
    seed_wallets = set(seed_sources)

    for protocol_code, markets in by_protocol.items():
        if protocol_code == "silo_v2":
            positions, sources, protocol_issues = _discover_silo_market_positions(
                as_of_ts_utc=as_of_ts_utc,
                markets=markets,
                seed_wallets=seed_wallets,
                price_map=price_map,
                holder_universe=holder_universe,
                silo_client=silo_client,
                top_holders_limit=silo_top_holders_limit,
            )
        elif protocol_code == "euler_v2":
            positions, sources, protocol_issues = _discover_euler_market_positions(
                as_of_ts_utc=as_of_ts_utc,
                markets=markets,
                routescan_client=routescan_client,
                rpc_client=rpc_client,
                price_map=price_map,
                holder_universe=holder_universe,
                holder_limit_per_token=holder_limit_per_token,
            )
        elif protocol_code == "morpho":
            positions, sources, protocol_issues = _discover_morpho_market_positions(
                as_of_ts_utc=as_of_ts_utc,
                markets=markets,
                rpc_client=rpc_client,
                price_map=price_map,
                holder_universe=holder_universe,
            )
        else:
            for market in markets:
                missing_market_coverage.append(
                    {
                        "protocol_code": market.protocol_code,
                        "chain_code": market.chain_code,
                        "market_ref": market.market_ref,
                        "config_sources": list(market.config_sources),
                        "reason": "discovery_hook_missing",
                    }
                )
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_cohort",
                        error_type="holder_market_discovery_missing",
                        error_message="configured holder market lacks a discovery hook",
                        chain_code=market.chain_code,
                        market_ref=market.market_ref,
                        payload_json={
                            "protocol_code": market.protocol_code,
                            "config_sources": list(market.config_sources),
                        },
                    )
                )
            continue

        issues.extend(protocol_issues)
        for wallet_address, rows in positions.items():
            positions_by_wallet[wallet_address].extend(rows)
        for wallet_address, sources_for_wallet in sources.items():
            candidate_sources[wallet_address].update(sources_for_wallet)

    return (
        {
            wallet_address: tuple(
                sorted(
                    rows,
                    key=lambda row: (
                        row.protocol_code,
                        row.chain_code,
                        row.market_ref,
                        row.wallet_address,
                    ),
                )
            )
            for wallet_address, rows in positions_by_wallet.items()
        },
        candidate_sources,
        missing_market_coverage,
        issues,
    )


def _filter_holder_candidate_sources(
    *,
    candidate_sources: dict[str, set[str]],
    strategy_wallets: set[str],
    protocol_wallets: set[str],
    holder_exclusions: HolderExclusionsConfig,
    holder_universe: HolderUniverseConfig,
    rpc_urls: dict[str, str] | None,
    as_of_ts_utc: datetime,
) -> tuple[dict[str, set[str]], dict[str, list[str]], set[str], list[DataQualityIssue]]:
    filtered = {wallet_address: set(sources) for wallet_address, sources in candidate_sources.items()}
    excluded_candidates: dict[str, list[str]] = {
        "strategy": [],
        "protocol": [],
        "explicit": [],
        "contract": [],
    }
    issues: list[DataQualityIssue] = []

    strategy_removed = sorted(wallet for wallet in filtered if wallet in strategy_wallets)
    for wallet in strategy_removed:
        filtered.pop(wallet, None)
    excluded_candidates["strategy"] = strategy_removed

    if holder_universe.exclude_protocol_wallets_by_default:
        protocol_removed = sorted(wallet for wallet in filtered if wallet in protocol_wallets)
        for wallet in protocol_removed:
            filtered.pop(wallet, None)
        excluded_candidates["protocol"] = protocol_removed

    explicit_removed = sorted(wallet for wallet in filtered if wallet in excluded_holder_wallets(holder_exclusions))
    for wallet in explicit_removed:
        filtered.pop(wallet, None)
    excluded_candidates["explicit"] = explicit_removed

    contract_wallets: set[str] = set()
    if rpc_urls:
        wallets_by_chain: dict[str, set[str]] = defaultdict(set)
        for wallet_address, sources in filtered.items():
            for chain_code in _chain_codes_from_discovery_sources(sources):
                if chain_code in rpc_urls:
                    wallets_by_chain[chain_code].add(wallet_address)
        for chain_code, wallet_addresses in wallets_by_chain.items():
            try:
                contract_wallets.update(
                    rpc_contract_addresses(
                        rpc_url=rpc_urls[chain_code],
                        addresses=sorted(wallet_addresses),
                    )
                )
            except Exception as exc:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_cohort",
                        error_type="contract_filter_failed",
                        error_message=str(exc),
                        chain_code=chain_code,
                    )
                )

    if holder_universe.exclude_contract_wallets_by_default:
        contract_removed = sorted(wallet for wallet in filtered if wallet in contract_wallets)
        for wallet in contract_removed:
            filtered.pop(wallet, None)
        excluded_candidates["contract"] = contract_removed

    return filtered, excluded_candidates, contract_wallets, issues


def discover_customer_candidate_wallets(
    *,
    session: Session,
    business_date: date,
    markets_config: MarketsConfig,
    consumer_markets_config: ConsumerMarketsConfig,
    avant_tokens: AvantTokensConfig,
    holder_universe: HolderUniverseConfig,
    routescan_client: RouteScanClient,
    rpc_client: EvmBatchRpcClient,
    price_map: dict[tuple[str, str], Decimal],
    manual_seed_wallets: set[str],
    strategy_wallets: set[str],
    protocol_wallets: set[str],
    holder_exclusions: HolderExclusionsConfig,
    silo_client: SiloApiClient | None = None,
    holder_limit_per_token: int = 200,
    silo_top_holders_limit: int = 200,
    rpc_urls: dict[str, str] | None = None,
) -> CustomerCandidateDiscovery:
    """Build the same-day holder universe candidate set from direct holders and live positions."""

    _, as_of_ts_utc = denver_business_bounds_utc(business_date)
    seed_sources = _build_holder_seed_sources(
        session=session,
        business_date=business_date,
        manual_seed_wallets=manual_seed_wallets,
    )
    direct_sources, token_holders_by_token, issues = _discover_direct_holder_wallets(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        avant_tokens=avant_tokens,
        holder_universe=holder_universe,
        routescan_client=routescan_client,
        holder_limit_per_token=holder_limit_per_token,
    )
    market_positions_by_wallet, market_sources, missing_market_coverage, market_issues = (
        _discover_market_position_evidence(
            as_of_ts_utc=as_of_ts_utc,
            business_date=business_date,
            avant_tokens=avant_tokens,
            consumer_markets_config=consumer_markets_config,
            markets_config=markets_config,
            routescan_client=routescan_client,
            rpc_client=rpc_client,
            price_map=price_map,
            holder_universe=holder_universe,
            seed_sources=seed_sources,
            silo_client=silo_client,
            holder_limit_per_token=holder_limit_per_token,
            silo_top_holders_limit=silo_top_holders_limit,
        )
    )
    issues.extend(market_issues)

    same_day_sources: dict[str, set[str]] = defaultdict(set)
    for wallet_address, sources in direct_sources.items():
        same_day_sources[wallet_address].update(sources)
    for wallet_address, sources in market_sources.items():
        same_day_sources[wallet_address].update(sources)

    filtered_sources, excluded_candidates, _contract_wallets, filter_issues = (
        _filter_holder_candidate_sources(
            candidate_sources=same_day_sources,
            strategy_wallets=strategy_wallets,
            protocol_wallets=protocol_wallets,
            holder_exclusions=holder_exclusions,
            holder_universe=holder_universe,
            rpc_urls=rpc_urls,
            as_of_ts_utc=as_of_ts_utc,
        )
    )
    issues.extend(filter_issues)

    return CustomerCandidateDiscovery(
        candidate_sources=filtered_sources,
        seed_sources=seed_sources,
        token_holders_by_token=token_holders_by_token,
        market_positions_by_wallet=market_positions_by_wallet,
        excluded_candidates=excluded_candidates,
        missing_market_coverage=missing_market_coverage,
        issues=issues,
    )


def upsert_customer_wallets(session: Session, wallet_addresses: Iterable[str]) -> dict[str, int]:
    """Insert discovered customer wallets without affecting existing strategy mappings."""

    normalized = sorted({canonical_address(address) for address in wallet_addresses})
    if normalized:
        wallet_values = [
            {"address": address, "wallet_type": "customer", "label": None} for address in normalized
        ]
        stmt = insert(Wallet).values(wallet_values)
        stmt = stmt.on_conflict_do_nothing(index_elements=[Wallet.address])
        session.execute(stmt)

    rows = session.execute(
        select(Wallet.address, Wallet.wallet_id).where(Wallet.address.in_(normalized))
    ).all()
    return {canonical_address(address): int(wallet_id) for address, wallet_id in rows}


def _write_data_quality_issues(session: Session, issues: list[DataQualityIssue]) -> int:
    if not issues:
        return 0
    stmt = insert(DataQuality).values(
        [
            {
                "as_of_ts_utc": issue.as_of_ts_utc,
                "stage": issue.stage,
                "protocol_code": issue.protocol_code,
                "chain_code": issue.chain_code,
                "wallet_address": issue.wallet_address,
                "market_ref": issue.market_ref,
                "error_type": issue.error_type,
                "error_message": issue.error_message,
                "payload_json": issue.payload_json,
            }
            for issue in issues
        ]
    )
    session.execute(stmt)
    return len(issues)


def _insert_rows_batched(
    session: Session,
    table_model,
    rows: list[dict[str, object]],
    *,
    chunk_size: int = 1000,
) -> int:
    """Insert rows in bounded chunks to avoid PostgreSQL bind-parameter limits."""

    if not rows:
        return 0
    if chunk_size < 1:
        raise ValueError("chunk_size must be >= 1")

    written = 0
    for offset in range(0, len(rows), chunk_size):
        chunk = rows[offset : offset + chunk_size]
        session.execute(insert(table_model).values(chunk))
        written += len(chunk)
    return written


def _persist_price_quotes(
    session: Session,
    *,
    as_of_ts_utc: datetime,
    quotes: list[tuple[int, str, str, Decimal]],
) -> None:
    if not quotes:
        return
    stmt = insert(Price).values(
        [
            {
                "ts_utc": as_of_ts_utc,
                "token_id": token_id,
                "price_usd": price_usd,
                "source": "defillama",
                "confidence": None,
            }
            for token_id, _chain_code, _address, price_usd in quotes
        ]
    )
    stmt = stmt.on_conflict_do_update(
        index_elements=[Price.ts_utc, Price.token_id, Price.source],
        set_={"price_usd": stmt.excluded.price_usd, "confidence": stmt.excluded.confidence},
    )
    session.execute(stmt)


def fetch_consumer_price_map(
    *,
    session: Session,
    as_of_ts_utc: datetime,
    avant_tokens: AvantTokensConfig,
    business_date: date,
    price_oracle: PriceOracle,
    chain_scope: set[str] | None = None,
) -> tuple[dict[tuple[str, str], Decimal], list[DataQualityIssue]]:
    """Fetch and persist prices needed for customer balance verification."""

    token_rows = session.execute(
        select(Chain.chain_code, Token.address_or_mint, Token.token_id, Token.symbol).join(
            Chain, Chain.chain_id == Token.chain_id
        )
    ).all()
    token_by_key = {
        (chain_code, canonical_address(address_or_mint)): (int(token_id), str(symbol))
        for chain_code, address_or_mint, token_id, symbol in token_rows
    }

    active_tokens = active_avant_tokens(
        avant_tokens,
        business_date=business_date,
        chain_scope=chain_scope,
    )
    request_targets: set[tuple[str, str]] = set()
    for token in active_tokens:
        request_targets.add((token.chain_code, canonical_address(token.token_address)))
        if token.underlying_token_address is not None:
            request_targets.add(
                (token.chain_code, canonical_address(token.underlying_token_address))
            )

    requests: list[PriceRequest] = []
    for chain_code, address in sorted(request_targets):
        token_row = token_by_key.get((chain_code, address))
        if token_row is None:
            continue
        token_id, symbol = token_row
        requests.append(
            PriceRequest(
                token_id=token_id,
                chain_code=chain_code,
                address_or_mint=address,
                symbol=symbol,
            )
        )

    result = price_oracle.fetch_prices(requests, as_of_ts_utc=as_of_ts_utc)
    _persist_price_quotes(
        session,
        as_of_ts_utc=as_of_ts_utc,
        quotes=[
            (quote.token_id, quote.chain_code, quote.address_or_mint, quote.price_usd)
            for quote in result.quotes
        ],
    )

    price_map = {
        (quote.chain_code, canonical_address(quote.address_or_mint)): quote.price_usd
        for quote in result.quotes
    }
    return price_map, result.issues


def resolve_token_decimals_map(session: Session) -> dict[tuple[str, str], int]:
    """Return token decimals keyed by normalized chain/address."""

    rows = session.execute(
        select(Chain.chain_code, Token.address_or_mint, Token.decimals).join(
            Chain, Chain.chain_id == Token.chain_id
        )
    ).all()
    return {
        (str(chain_code), canonical_address(str(address_or_mint))): int(decimals)
        for chain_code, address_or_mint, decimals in rows
    }


def _verify_customer_wallet_candidates(
    *,
    business_date: date,
    as_of_ts_utc: datetime,
    candidate_sources: dict[str, set[str]],
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    rpc_client: EvmBatchRpcClient,
    price_map: dict[tuple[str, str], Decimal],
    token_decimals_by_key: dict[tuple[str, str], int],
    chain_scope: set[str] | None = None,
) -> tuple[list[VerifiedCustomerWallet], list[DataQualityIssue]]:
    """Verify all candidate wallet balances against the active Avant registry."""

    active_tokens = active_avant_tokens(
        avant_tokens,
        business_date=business_date,
        chain_scope=chain_scope,
    )
    wallet_addresses = sorted(candidate_sources)
    balances_by_wallet: dict[str, list[VerifiedAvantBalance]] = defaultdict(list)
    issues: list[DataQualityIssue] = []

    for token in active_tokens:
        try:
            raw_balances = rpc_client.read_erc20_balances(
                chain_code=token.chain_code,
                token_address=token.token_address,
                wallet_addresses=wallet_addresses,
            )
        except Exception as exc:
            issues.append(
                DataQualityIssue(
                    as_of_ts_utc=as_of_ts_utc,
                    stage="sync_consumer_cohort",
                    error_type="wallet_balance_read_failed",
                    error_message=str(exc),
                    chain_code=token.chain_code,
                    market_ref=token.token_address,
                    payload_json={"symbol": token.symbol},
                )
            )
            continue

        shares_to_convert: dict[str, int] = {}
        for wallet_address, balance_raw in raw_balances.items():
            if balance_raw is None:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_cohort",
                        error_type="wallet_balance_read_failed",
                        error_message="eth_call balanceOf returned no result",
                        chain_code=token.chain_code,
                        wallet_address=wallet_address,
                        market_ref=token.token_address,
                        payload_json={"symbol": token.symbol},
                    )
                )
                continue
            if balance_raw <= 0:
                continue
            if token.pricing_policy in {"convert_to_assets", "direct_or_convert_to_assets"}:
                shares_to_convert[wallet_address] = balance_raw

        converted_to_assets: dict[str, int | None] = {}
        if shares_to_convert:
            target_contract = (
                token.nav_contract or token.exchange_rate_contract or token.token_address
            )
            try:
                converted_to_assets = rpc_client.convert_to_assets(
                    chain_code=token.chain_code,
                    vault_address=target_contract,
                    shares_by_wallet=shares_to_convert,
                )
            except Exception as exc:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_cohort",
                        error_type="share_conversion_failed",
                        error_message=str(exc),
                        chain_code=token.chain_code,
                        market_ref=target_contract,
                        payload_json={"symbol": token.symbol},
                    )
                )

        direct_price = price_map.get((token.chain_code, canonical_address(token.token_address)))
        underlying_price = (
            price_map.get((token.chain_code, canonical_address(token.underlying_token_address)))
            if token.underlying_token_address is not None
            else None
        )

        for wallet_address, balance_raw in raw_balances.items():
            if balance_raw is None or balance_raw <= 0:
                continue
            balance_amount = Decimal(balance_raw) / (Decimal(10) ** token.decimals)
            usd_value: Decimal | None = None
            underlying_amount: Decimal | None = None
            resolved = False
            resolution_source: str | None = None
            error_type: str | None = None
            error_message: str | None = None

            if token.pricing_policy == "direct_price":
                if direct_price is not None:
                    usd_value = balance_amount * direct_price
                    resolved = True
                    resolution_source = "direct_price"
                else:
                    error_type = "price_missing"
                    error_message = "direct token price is unavailable"
            else:
                converted_raw = converted_to_assets.get(wallet_address)
                if (
                    token.pricing_policy == "direct_or_convert_to_assets"
                    and direct_price is not None
                ):
                    usd_value = balance_amount * direct_price
                    resolved = True
                    resolution_source = "direct_price"
                elif converted_raw is None:
                    error_type = "share_conversion_failed"
                    error_message = "convertToAssets returned no result"
                elif underlying_price is None:
                    error_type = "underlying_price_missing"
                    error_message = "underlying token price is unavailable"
                else:
                    underlying_decimals = token_decimals_by_key.get(
                        (token.chain_code, canonical_address(token.underlying_token_address or ""))
                    )
                    if underlying_decimals is None:
                        error_type = "underlying_decimals_missing"
                        error_message = "underlying token decimals are unavailable"
                    else:
                        underlying_amount = Decimal(converted_raw) / (
                            Decimal(10) ** underlying_decimals
                        )
                        usd_value = underlying_amount * underlying_price
                        resolved = True
                        resolution_source = "convert_to_assets"
                if (
                    underlying_amount is None
                    and converted_raw is not None
                    and token.underlying_token_address is not None
                    and error_type is not None
                ):
                    underlying_decimals = token_decimals_by_key.get(
                        (
                            token.chain_code,
                            canonical_address(token.underlying_token_address),
                        )
                    )
                    if underlying_decimals is not None:
                        underlying_amount = Decimal(converted_raw) / (
                            Decimal(10) ** underlying_decimals
                        )

            if not resolved and error_type is not None:
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_cohort",
                        error_type=error_type,
                        error_message=error_message or error_type,
                        chain_code=token.chain_code,
                        wallet_address=wallet_address,
                        market_ref=token.token_address,
                        payload_json={
                            "symbol": token.symbol,
                            "pricing_policy": token.pricing_policy,
                            "balance_raw": str(balance_raw),
                        },
                    )
                )

            balances_by_wallet[wallet_address].append(
                VerifiedAvantBalance(
                    wallet_address=wallet_address,
                    chain_code=token.chain_code,
                    token_address=canonical_address(token.token_address),
                    symbol=token.symbol,
                    asset_family=token.asset_family,
                    wrapper_class=token.wrapper_class,
                    pricing_policy=token.pricing_policy,
                    balance_raw=balance_raw,
                    balance_amount=balance_amount,
                    underlying_amount=underlying_amount,
                    usd_value=usd_value,
                    resolved=resolved,
                    resolution_source=resolution_source,
                    error_type=error_type,
                    error_message=error_message,
                )
            )

    verified_wallets: list[VerifiedCustomerWallet] = []
    for wallet_address in wallet_addresses:
        balance_rows = tuple(
            sorted(
                balances_by_wallet.get(wallet_address, []),
                key=lambda row: (row.chain_code, row.symbol, row.token_address),
            )
        )
        verified_total = sum(
            (row.usd_value or Decimal("0") for row in balance_rows if row.resolved),
            Decimal("0"),
        )
        unresolved_positive = [
            row for row in balance_rows if not row.resolved and row.balance_raw > 0
        ]
        is_signoff_eligible = len(unresolved_positive) == 0
        exclusion_reason = unresolved_positive[0].error_type if unresolved_positive else None
        verified_wallets.append(
            VerifiedCustomerWallet(
                wallet_address=wallet_address,
                verified_total_avant_usd=verified_total,
                discovery_sources=tuple(sorted(candidate_sources.get(wallet_address, set()))),
                is_signoff_eligible=is_signoff_eligible,
                exclusion_reason=exclusion_reason,
                balances=balance_rows,
            )
        )

    verified_wallets.sort(key=lambda row: (-row.verified_total_avant_usd, row.wallet_address))
    return verified_wallets, issues


def verify_customer_wallet_balances(
    *,
    business_date: date,
    as_of_ts_utc: datetime,
    candidate_sources: dict[str, set[str]],
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    rpc_client: EvmBatchRpcClient,
    price_map: dict[tuple[str, str], Decimal],
    token_decimals_by_key: dict[tuple[str, str], int],
) -> tuple[list[VerifiedCustomerWallet], list[DataQualityIssue]]:
    """Verify candidate wallet balances against the active Avant registry."""

    all_wallets, issues = _verify_customer_wallet_candidates(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        candidate_sources=candidate_sources,
        avant_tokens=avant_tokens,
        thresholds=thresholds,
        rpc_client=rpc_client,
        price_map=price_map,
        token_decimals_by_key=token_decimals_by_key,
    )
    return (
        [
            wallet
            for wallet in all_wallets
            if wallet.verified_total_avant_usd >= thresholds.verified_min_total_avant_usd
        ],
        issues,
    )


def _wallet_has_family_exposure(wallet: VerifiedCustomerWallet, family_code: str) -> bool:
    return wallet.observed_family_total_usd(family_code) > ZERO or any(
        balance.asset_family == family_code
        and balance.balance_raw > 0
        and balance.resolved
        and (balance.usd_value or Decimal("0")) > Decimal("0")
        for balance in wallet.balances
    )


def scan_holder_candidate_debank_activity(
    *,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_ids: dict[str, int],
    candidate_sources: dict[str, set[str]],
    avant_tokens: AvantTokensConfig,
    holder_protocol_map: HolderProtocolMapConfig,
    debank_client: DebankCloudClient,
    min_leg_usd: Decimal,
    max_concurrency: int = 6,
) -> tuple[
    dict[str, DebankHolderWalletSummary],
    list[dict[str, object]],
    list[dict[str, object]],
    list[DataQualityIssue],
]:
    """Fetch DeBank protocol/token activity for holder candidates using raw avAsset symbols."""

    if min_leg_usd < 0:
        raise ValueError("min_leg_usd must be non-negative")
    if max_concurrency < 1:
        raise ValueError("max_concurrency must be >= 1")

    protocol_map = holder_protocol_map.by_protocol_code()
    registry_by_symbol, registry_symbols_sorted = _avant_symbol_registry(
        avant_tokens=avant_tokens,
        business_date=business_date,
    )

    wallet_summaries: dict[str, DebankHolderWalletSummary] = {}
    protocol_rows_out: list[dict[str, object]] = []
    token_rows_out: list[dict[str, object]] = []
    issues: list[DataQualityIssue] = []

    wallet_addresses = sorted(candidate_sources)

    def _scan_wallet(wallet_address: str) -> tuple[str, list[dict[str, object]], str | None]:
        try:
            payload = debank_client.get_user_complex_protocols(wallet_address)
        except Exception as exc:  # pragma: no cover - network failures
            return wallet_address, [], str(exc)
        return wallet_address, payload, None

    with ThreadPoolExecutor(max_workers=max_concurrency) as pool:
        futures = [pool.submit(_scan_wallet, wallet_address) for wallet_address in wallet_addresses]
        for future in as_completed(futures):
            wallet_address, payload, fetch_error = future.result()
            wallet_id = wallet_ids[wallet_address]

            if fetch_error is not None:
                wallet_summaries[wallet_address] = DebankHolderWalletSummary(
                    fetch_succeeded=False,
                    fetch_error_message=fetch_error,
                    has_any_activity=False,
                    has_any_borrow=False,
                    has_configured_surface_activity=False,
                    protocol_count=0,
                    chain_count=0,
                    configured_protocol_count=0,
                    total_supply_usd=ZERO,
                    total_borrow_usd=ZERO,
                    configured_surface_supply_usd=ZERO,
                    configured_surface_borrow_usd=ZERO,
                    avasset_supply_total_usd=ZERO,
                    avasset_supply_by_family={},
                )
                issues.append(
                    DataQualityIssue(
                        as_of_ts_utc=as_of_ts_utc,
                        stage="sync_consumer_cohort",
                        error_type="debank_wallet_fetch_failed",
                        error_message=fetch_error,
                        wallet_address=wallet_address,
                    )
                )
                continue

            protocol_totals: dict[tuple[str, str], dict[str, object]] = defaultdict(
                lambda: {
                    "supply_usd": ZERO,
                    "borrow_usd": ZERO,
                    "in_config_surface": False,
                }
            )
            token_totals: dict[tuple[str, str, str, str], dict[str, object]] = defaultdict(
                lambda: {
                    "in_config_surface": False,
                    "usd_value": ZERO,
                }
            )
            avasset_supply_by_family: dict[str, Decimal] = defaultdict(lambda: ZERO)

            for protocol_payload in payload:
                if not isinstance(protocol_payload, dict):
                    continue
                chain_id = _to_debank_string(protocol_payload.get("chain"))
                if not chain_id:
                    continue
                chain_code = normalize_chain_code(chain_id)

                protocol_id = _to_debank_string(protocol_payload.get("id")) or "unknown"
                protocol_code = normalize_protocol_code(protocol_id)
                if is_excluded_visibility_protocol(protocol_code):
                    continue

                protocol_entry = protocol_map.get(protocol_code)
                in_config_surface = (
                    protocol_entry.surface == "canonical_supported"
                    if protocol_entry is not None
                    else holder_protocol_map.defaults.surface == "canonical_supported"
                )

                portfolio_items = protocol_payload.get("portfolio_item_list")
                if not isinstance(portfolio_items, list):
                    continue

                for item in portfolio_items:
                    if not isinstance(item, dict):
                        continue
                    detail = item.get("detail")
                    if not isinstance(detail, dict):
                        continue
                    for detail_key, detail_value in detail.items():
                        if not detail_key.endswith("_token_list") or not isinstance(detail_value, list):
                            continue
                        leg_type = _debank_leg_type_from_detail_key(detail_key)
                        if leg_type is None:
                            continue
                        for token in detail_value:
                            if not isinstance(token, dict):
                                continue
                            token_symbol = _to_debank_string(
                                token.get("optimized_symbol")
                            ) or _to_debank_string(token.get("symbol"))
                            if not token_symbol:
                                continue
                            usd_value = _to_debank_decimal(token.get("usd_value"))
                            if usd_value is None:
                                amount = _to_debank_decimal(token.get("amount"))
                                price = _to_debank_decimal(token.get("price"))
                                if amount is not None and price is not None:
                                    usd_value = amount * price
                            if usd_value is None:
                                continue
                            usd_abs = abs(usd_value)
                            if usd_abs < min_leg_usd:
                                continue

                            protocol_bucket = protocol_totals[(chain_code, protocol_code)]
                            if leg_type == "borrow":
                                protocol_bucket["borrow_usd"] = (
                                    Decimal(str(protocol_bucket["borrow_usd"])) + usd_abs
                                )
                            else:
                                protocol_bucket["supply_usd"] = (
                                    Decimal(str(protocol_bucket["supply_usd"])) + usd_abs
                                )
                            protocol_bucket["in_config_surface"] = bool(
                                protocol_bucket["in_config_surface"]
                            ) or in_config_surface

                            token_key = (
                                chain_code,
                                protocol_code,
                                token_symbol.strip(),
                                leg_type,
                            )
                            token_bucket = token_totals[token_key]
                            token_bucket["in_config_surface"] = bool(
                                token_bucket["in_config_surface"]
                            ) or in_config_surface
                            token_bucket["usd_value"] = (
                                Decimal(str(token_bucket["usd_value"])) + usd_abs
                            )

                            if leg_type != "borrow":
                                family_code, _wrapper_class = _family_and_wrapper_for_token_symbol(
                                    token_symbol,
                                    registry_by_symbol=registry_by_symbol,
                                    registry_symbols_sorted=registry_symbols_sorted,
                                )
                                if family_code is not None:
                                    avasset_supply_by_family[family_code] += usd_abs

            total_supply_usd = sum(
                (Decimal(str(row["supply_usd"])) for row in protocol_totals.values()),
                ZERO,
            )
            total_borrow_usd = sum(
                (Decimal(str(row["borrow_usd"])) for row in protocol_totals.values()),
                ZERO,
            )
            configured_surface_supply_usd = sum(
                (
                    Decimal(str(row["supply_usd"]))
                    for row in protocol_totals.values()
                    if bool(row["in_config_surface"])
                ),
                ZERO,
            )
            configured_surface_borrow_usd = sum(
                (
                    Decimal(str(row["borrow_usd"]))
                    for row in protocol_totals.values()
                    if bool(row["in_config_surface"])
                ),
                ZERO,
            )
            configured_protocols_seen = {
                protocol_code
                for (_chain_code, protocol_code), row in protocol_totals.items()
                if bool(row["in_config_surface"])
            }
            for (chain_code, protocol_code), row in protocol_totals.items():
                protocol_rows_out.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": as_of_ts_utc,
                        "wallet_id": wallet_id,
                        "wallet_address": wallet_address,
                        "chain_code": chain_code,
                        "protocol_code": protocol_code,
                        "in_config_surface": bool(row["in_config_surface"]),
                        "supply_usd": Decimal(str(row["supply_usd"])),
                        "borrow_usd": Decimal(str(row["borrow_usd"])),
                    }
                )
            for (chain_code, protocol_code, token_symbol, leg_type), row in token_totals.items():
                token_rows_out.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": as_of_ts_utc,
                        "wallet_id": wallet_id,
                        "wallet_address": wallet_address,
                        "chain_code": chain_code,
                        "protocol_code": protocol_code,
                        "token_symbol": token_symbol,
                        "leg_type": leg_type,
                        "in_config_surface": bool(row["in_config_surface"]),
                        "usd_value": Decimal(str(row["usd_value"])),
                    }
                )

            wallet_summaries[wallet_address] = DebankHolderWalletSummary(
                fetch_succeeded=True,
                fetch_error_message=None,
                has_any_activity=bool(protocol_totals),
                has_any_borrow=total_borrow_usd > ZERO,
                has_configured_surface_activity=(
                    configured_surface_supply_usd > ZERO or configured_surface_borrow_usd > ZERO
                ),
                protocol_count=len({protocol_code for _chain_code, protocol_code in protocol_totals}),
                chain_count=len({chain_code for chain_code, _protocol_code in protocol_totals}),
                configured_protocol_count=len(configured_protocols_seen),
                total_supply_usd=total_supply_usd,
                total_borrow_usd=total_borrow_usd,
                configured_surface_supply_usd=configured_surface_supply_usd,
                configured_surface_borrow_usd=configured_surface_borrow_usd,
                avasset_supply_total_usd=sum(avasset_supply_by_family.values(), ZERO),
                avasset_supply_by_family=dict(avasset_supply_by_family),
            )

    return wallet_summaries, protocol_rows_out, token_rows_out, issues


def write_consumer_debank_visibility_daily(
    *,
    session: Session,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_ids: dict[str, int],
    candidate_sources: dict[str, set[str]],
    holder_wallets: list[VerifiedCustomerWallet],
    cohort_wallets: list[VerifiedCustomerWallet],
    wallet_summaries: dict[str, DebankHolderWalletSummary],
    protocol_rows: list[dict[str, object]],
    token_rows: list[dict[str, object]],
) -> tuple[int, int, int]:
    """Replace persisted holder-facing DeBank visibility rows for one business date."""

    session.execute(
        delete(ConsumerDebankWalletDaily).where(ConsumerDebankWalletDaily.business_date == business_date)
    )
    session.execute(
        delete(ConsumerDebankProtocolDaily).where(
            ConsumerDebankProtocolDaily.business_date == business_date
        )
    )
    session.execute(
        delete(ConsumerDebankTokenDaily).where(ConsumerDebankTokenDaily.business_date == business_date)
    )

    holder_wallets_by_address = {wallet.wallet_address: wallet for wallet in holder_wallets}
    cohort_wallets_by_address = {wallet.wallet_address: wallet for wallet in cohort_wallets}
    wallet_rows: list[dict[str, object]] = []
    for wallet_address, summary in wallet_summaries.items():
        discovery_sources = set(candidate_sources.get(wallet_address, set()))
        if summary.avasset_supply_total_usd > ZERO:
            discovery_sources.add("debank_avasset_activity")
        holder_wallet = holder_wallets_by_address.get(wallet_address)
        cohort_wallet = cohort_wallets_by_address.get(wallet_address)
        wallet_rows.append(
            {
                "business_date": business_date,
                "as_of_ts_utc": as_of_ts_utc,
                "wallet_id": wallet_ids[wallet_address],
                "wallet_address": wallet_address,
                "in_seed_set": "legacy_seed" in discovery_sources,
                "in_verified_cohort": holder_wallet is not None,
                "in_signoff_cohort": cohort_wallet is not None and cohort_wallet.is_signoff_eligible,
                "seed_sources_json": ["legacy_seed"] if "legacy_seed" in discovery_sources else None,
                "discovery_sources_json": sorted(discovery_sources) or None,
                "fetch_succeeded": summary.fetch_succeeded,
                "fetch_error_message": summary.fetch_error_message,
                "has_any_activity": summary.has_any_activity,
                "has_any_borrow": summary.has_any_borrow,
                "has_configured_surface_activity": summary.has_configured_surface_activity,
                "protocol_count": summary.protocol_count,
                "chain_count": summary.chain_count,
                "configured_protocol_count": summary.configured_protocol_count,
                "total_supply_usd": summary.total_supply_usd,
                "total_borrow_usd": summary.total_borrow_usd,
                "configured_surface_supply_usd": summary.configured_surface_supply_usd,
                "configured_surface_borrow_usd": summary.configured_surface_borrow_usd,
            }
        )

    wallet_count = _insert_rows_batched(session, ConsumerDebankWalletDaily, wallet_rows)
    protocol_count = _insert_rows_batched(session, ConsumerDebankProtocolDaily, protocol_rows)
    token_count = _insert_rows_batched(session, ConsumerDebankTokenDaily, token_rows)
    return wallet_count, protocol_count, token_count


def build_holder_protocol_wallet_scopes(
    *,
    session: Session,
    business_date: date,
    wallet_addresses: Iterable[str],
    holder_protocol_map: HolderProtocolMapConfig,
) -> dict[str, dict[str, list[str]]]:
    """Return per-protocol chain wallet scopes for canonical-supported holder positions."""

    holder_wallets = {canonical_address(wallet_address) for wallet_address in wallet_addresses}
    if not holder_wallets:
        return {}

    protocol_map = holder_protocol_map.by_protocol_code()
    wallet_scope: dict[str, dict[str, set[str]]] = defaultdict(lambda: defaultdict(set))
    rows = session.scalars(
        select(ConsumerHolderUniverseDaily).where(
            ConsumerHolderUniverseDaily.business_date == business_date,
            ConsumerHolderUniverseDaily.wallet_address.in_(sorted(holder_wallets)),
        )
    ).all()
    for row in rows:
        sources_obj = row.discovery_sources_json or {}
        raw_sources = sources_obj.get("sources") if isinstance(sources_obj, dict) else None
        if not isinstance(raw_sources, list):
            continue
        for source in raw_sources:
            if not isinstance(source, str) or not source.startswith("market_position:"):
                continue
            parts = source.split(":", 3)
            if len(parts) != 4:
                continue
            _tag, protocol_code, chain_code, _market_ref = parts
            entry = protocol_map.get(protocol_code)
            surface = entry.surface if entry is not None else holder_protocol_map.defaults.surface
            if surface != "canonical_supported":
                continue
            canonical_protocol_code = (
                entry.canonical_protocol_code if entry is not None else None
            ) or protocol_code
            wallet_scope[canonical_protocol_code][chain_code].add(
                canonical_address(str(row.wallet_address))
            )

    return {
        protocol_code: {
            chain_code: sorted(wallets)
            for chain_code, wallets in chains.items()
            if wallets
        }
        for protocol_code, chains in wallet_scope.items()
        if any(wallets for wallets in chains.values())
    }


def write_consumer_holder_universe_daily(
    *,
    session: Session,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_ids: dict[str, int],
    verified_wallets: list[VerifiedCustomerWallet],
) -> int:
    """Replace persisted verified holder-universe rows for one business date."""

    session.execute(
        delete(ConsumerHolderUniverseDaily).where(
            ConsumerHolderUniverseDaily.business_date == business_date
        )
    )
    if not verified_wallets:
        return 0

    rows = [
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of_ts_utc,
            "wallet_id": wallet_ids[wallet.wallet_address],
            "wallet_address": wallet.wallet_address,
            "verified_total_avant_usd": wallet.verified_total_avant_usd,
            "verified_family_usd_usd": wallet.family_total_usd("usd"),
            "verified_family_btc_usd": wallet.family_total_usd("btc"),
            "verified_family_eth_usd": wallet.family_total_usd("eth"),
            "verified_base_usd": wallet.wrapper_total_usd("base"),
            "verified_staked_usd": wallet.wrapper_total_usd("staked"),
            "verified_boosted_usd": wallet.wrapper_total_usd("boosted"),
            "verified_staked_usd_usd": wallet.family_wrapper_total_usd("usd", "staked"),
            "verified_staked_eth_usd": wallet.family_wrapper_total_usd("eth", "staked"),
            "verified_staked_btc_usd": wallet.family_wrapper_total_usd("btc", "staked"),
            "discovery_sources_json": {"sources": list(wallet.discovery_sources)},
            "is_signoff_eligible": wallet.is_signoff_eligible,
            "exclusion_reason": wallet.exclusion_reason,
            "has_usd_exposure": wallet.observed_family_total_usd("usd") > ZERO
            or _wallet_has_family_exposure(wallet, "usd"),
            "has_eth_exposure": wallet.observed_family_total_usd("eth") > ZERO
            or _wallet_has_family_exposure(wallet, "eth"),
            "has_btc_exposure": wallet.observed_family_total_usd("btc") > ZERO
            or _wallet_has_family_exposure(wallet, "btc"),
        }
        for wallet in verified_wallets
        if wallet.wallet_address in wallet_ids
    ]
    session.execute(insert(ConsumerHolderUniverseDaily).values(rows))
    return len(rows)


def write_consumer_cohort_daily(
    *,
    session: Session,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_ids: dict[str, int],
    verified_wallets: list[VerifiedCustomerWallet],
    thresholds: ConsumerThresholdsConfig,
) -> int:
    """Replace persisted core cohort rows for one business date."""

    session.execute(
        delete(ConsumerCohortDaily).where(ConsumerCohortDaily.business_date == business_date)
    )
    cohort_wallets = [
        wallet
        for wallet in verified_wallets
        if wallet.observed_total_usd() >= thresholds.cohort_min_total_avant_usd
    ]
    if not cohort_wallets:
        return 0

    rows = [
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of_ts_utc,
            "wallet_id": wallet_ids[wallet.wallet_address],
            "wallet_address": wallet.wallet_address,
            "verified_total_avant_usd": wallet.observed_total_usd(),
            "discovery_sources_json": {"sources": list(wallet.discovery_sources)},
            "is_signoff_eligible": wallet.is_signoff_eligible,
            "exclusion_reason": wallet.exclusion_reason,
        }
        for wallet in cohort_wallets
        if wallet.wallet_address in wallet_ids
    ]
    session.execute(insert(ConsumerCohortDaily).values(rows))
    return len(rows)


def build_verified_customer_cohort(
    *,
    session: Session,
    business_date: date,
    markets_config: MarketsConfig,
    consumer_markets_config: ConsumerMarketsConfig,
    wallet_products_config: WalletProductsConfig,
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    holder_universe: HolderUniverseConfig,
    holder_exclusions: HolderExclusionsConfig,
    holder_protocol_map: HolderProtocolMapConfig | None,
    routescan_client: RouteScanClient,
    rpc_client: EvmBatchRpcClient,
    price_oracle: PriceOracle,
    debank_client: DebankCloudClient | None = None,
    silo_client: SiloApiClient | None = None,
    config_dir: Path = Path("config"),
    holder_limit_per_token: int = 200,
    silo_top_holders_limit: int = 200,
    debank_max_concurrency: int = 6,
    rpc_urls: dict[str, str] | None = None,
) -> CustomerCohortSyncSummary:
    """Discover, verify, and persist the tracked customer cohort for one day."""

    _, as_of_ts_utc = denver_business_bounds_utc(business_date)
    manual_seed_wallets = load_legacy_customer_seed_wallets(
        config_dir,
        globs=holder_universe.legacy_seed_globs,
        force_include_wallets=holder_universe.force_include_wallets,
    )
    strategy_wallets = collect_strategy_wallets(
        markets_config=markets_config,
        wallet_products_config=wallet_products_config,
    )
    protocol_wallets = collect_evm_addresses_from_yaml(
        [
            config_dir / "markets.yaml",
            config_dir / "consumer_markets.yaml",
            config_dir / "wallet_products.yaml",
        ]
    )
    chain_scope = set(holder_universe.chain_id_map())
    price_map, price_issues = fetch_consumer_price_map(
        session=session,
        as_of_ts_utc=as_of_ts_utc,
        avant_tokens=avant_tokens,
        business_date=business_date,
        price_oracle=price_oracle,
        chain_scope=chain_scope,
    )
    token_decimals_by_key = resolve_token_decimals_map(session)
    discovery = discover_customer_candidate_wallets(
        session=session,
        business_date=business_date,
        markets_config=markets_config,
        consumer_markets_config=consumer_markets_config,
        avant_tokens=avant_tokens,
        holder_universe=holder_universe,
        routescan_client=routescan_client,
        rpc_client=rpc_client,
        price_map=price_map,
        manual_seed_wallets=manual_seed_wallets,
        strategy_wallets=strategy_wallets,
        protocol_wallets=protocol_wallets,
        holder_exclusions=holder_exclusions,
        silo_client=silo_client,
        holder_limit_per_token=holder_limit_per_token,
        silo_top_holders_limit=silo_top_holders_limit,
        rpc_urls=rpc_urls,
    )
    filtered_candidates = discovery.candidate_sources
    issues = list(price_issues) + list(discovery.issues)
    direct_holder_wallets = {
        canonical_address(holder.address)
        for holders in discovery.token_holders_by_token.values()
        for holder in holders
    }
    contract_wallets = {
        canonical_address(wallet_address)
        for wallet_address in discovery.excluded_candidates.get("contract", [])
    }
    wallet_ids = upsert_customer_wallets(
        session,
        direct_holder_wallets
        | set(filtered_candidates)
        | {
            canonical_address(wallet_address)
            for wallets in discovery.excluded_candidates.values()
            for wallet_address in wallets
        },
    )
    direct_wallets, verification_issues = _verify_customer_wallet_candidates(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        candidate_sources=filtered_candidates,
        avant_tokens=avant_tokens,
        thresholds=thresholds,
        rpc_client=rpc_client,
        price_map=price_map,
        token_decimals_by_key=token_decimals_by_key,
        chain_scope=chain_scope,
    )
    issues.extend(verification_issues)
    direct_wallets_by_address = {wallet.wallet_address: wallet for wallet in direct_wallets}
    market_positions_by_wallet = discovery.market_positions_by_wallet
    market_total_by_wallet: dict[str, Decimal] = defaultdict(lambda: ZERO)
    market_family_by_wallet: dict[str, dict[str, Decimal]] = defaultdict(lambda: defaultdict(lambda: ZERO))
    for wallet_address, rows in market_positions_by_wallet.items():
        for row in rows:
            market_total_by_wallet[wallet_address] += row.usd_value
            market_family_by_wallet[wallet_address][row.asset_family] += row.usd_value

    holder_candidate_sources: dict[str, set[str]] = {}
    holder_wallets: list[VerifiedCustomerWallet] = []
    for wallet_address in sorted(filtered_candidates):
        direct_wallet = direct_wallets_by_address[wallet_address]
        market_total = market_total_by_wallet.get(wallet_address, ZERO)
        if (
            direct_wallet.verified_total_avant_usd < holder_universe.raw_holder_threshold_usd
            and market_total < holder_universe.raw_holder_threshold_usd
        ):
            continue
        holder_candidate_sources[wallet_address] = set(filtered_candidates[wallet_address])
        holder_wallets.append(
            VerifiedCustomerWallet(
                wallet_address=direct_wallet.wallet_address,
                verified_total_avant_usd=direct_wallet.verified_total_avant_usd,
                discovery_sources=tuple(sorted(filtered_candidates[wallet_address])),
                is_signoff_eligible=direct_wallet.is_signoff_eligible,
                exclusion_reason=direct_wallet.exclusion_reason,
                balances=direct_wallet.balances,
                observed_total_avant_usd=direct_wallet.verified_total_avant_usd + market_total,
                observed_additional_family_usd=dict(market_family_by_wallet.get(wallet_address, {})),
            )
        )

    debank_wallet_summaries: dict[str, DebankHolderWalletSummary] = {}
    debank_protocol_rows: list[dict[str, object]] = []
    debank_token_rows: list[dict[str, object]] = []
    if debank_client is not None and holder_protocol_map is not None and holder_candidate_sources:
        (
            debank_wallet_summaries,
            debank_protocol_rows,
            debank_token_rows,
            debank_issues,
        ) = scan_holder_candidate_debank_activity(
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
            wallet_ids=wallet_ids,
            candidate_sources=holder_candidate_sources,
            avant_tokens=avant_tokens,
            holder_protocol_map=holder_protocol_map,
            debank_client=debank_client,
            min_leg_usd=min(
                thresholds.verified_min_total_avant_usd,
                thresholds.classification_dust_floor_usd,
            ),
            max_concurrency=debank_max_concurrency,
        )
        issues.extend(debank_issues)

    enriched_holder_wallets: list[VerifiedCustomerWallet] = []
    for wallet in holder_wallets:
        wallet_address = wallet.wallet_address
        debank_summary = debank_wallet_summaries.get(wallet_address)
        observed_external_total = (
            debank_summary.avasset_supply_total_usd if debank_summary is not None else ZERO
        )
        signoff_eligible = wallet.is_signoff_eligible and (
            debank_summary.fetch_succeeded if debank_summary is not None else True
        )
        exclusion_reason = wallet.exclusion_reason
        if exclusion_reason is None and debank_summary is not None and not debank_summary.fetch_succeeded:
            exclusion_reason = "debank_wallet_fetch_failed"

        additional_family_usd = dict(wallet.observed_additional_family_usd)
        if debank_summary is not None:
            for family_code, usd_value in debank_summary.avasset_supply_by_family.items():
                additional_family_usd[family_code] = additional_family_usd.get(family_code, ZERO) + usd_value

        enriched_holder_wallets.append(
            VerifiedCustomerWallet(
                wallet_address=wallet.wallet_address,
                verified_total_avant_usd=wallet.verified_total_avant_usd,
                discovery_sources=wallet.discovery_sources,
                is_signoff_eligible=signoff_eligible,
                exclusion_reason=exclusion_reason,
                balances=wallet.balances,
                observed_total_avant_usd=(
                    wallet.observed_total_usd() + observed_external_total
                ),
                observed_additional_family_usd=additional_family_usd,
            )
        )
    holder_wallets = enriched_holder_wallets
    holder_wallets.sort(
        key=lambda row: (-row.observed_total_usd(), -row.verified_total_avant_usd, row.wallet_address)
    )
    cohort_wallets = [
        wallet
        for wallet in holder_wallets
        if wallet.observed_total_usd() >= thresholds.cohort_min_total_avant_usd
    ]

    write_consumer_token_holder_daily(
        session=session,
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_ids=wallet_ids,
        token_holders_by_token=discovery.token_holders_by_token,
        price_map=price_map,
        avant_tokens=avant_tokens,
        strategy_wallets=strategy_wallets,
        protocol_wallets=protocol_wallets,
        contract_wallets=contract_wallets,
        holder_exclusions=holder_exclusions,
        holder_universe=holder_universe,
    )
    verified_rows_written = write_consumer_holder_universe_daily(
        session=session,
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_ids=wallet_ids,
        verified_wallets=holder_wallets,
    )
    cohort_rows_written = write_consumer_cohort_daily(
        session=session,
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_ids=wallet_ids,
        verified_wallets=holder_wallets,
        thresholds=thresholds,
    )
    write_consumer_debank_visibility_daily(
        session=session,
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_ids=wallet_ids,
        candidate_sources=filtered_candidates,
        holder_wallets=holder_wallets,
        cohort_wallets=cohort_wallets,
        wallet_summaries=debank_wallet_summaries,
        protocol_rows=debank_protocol_rows,
        token_rows=debank_token_rows,
    )
    issues_written = _write_data_quality_issues(session, issues)

    return CustomerCohortSyncSummary(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        candidate_wallet_count=len(filtered_candidates),
        verified_wallet_count=verified_rows_written,
        cohort_wallet_count=cohort_rows_written,
        signoff_eligible_wallet_count=sum(
            1 for wallet in cohort_wallets if wallet.is_signoff_eligible
        ),
        issues_written=issues_written,
    )


def build_holder_discovery_report_payload(
    *,
    session: Session,
    business_date: date,
    markets_config: MarketsConfig,
    consumer_markets_config: ConsumerMarketsConfig,
    wallet_products_config: WalletProductsConfig,
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    holder_universe: HolderUniverseConfig,
    holder_exclusions: HolderExclusionsConfig,
    routescan_client: RouteScanClient,
    rpc_client: EvmBatchRpcClient,
    price_oracle: PriceOracle,
    silo_client: SiloApiClient | None = None,
    config_dir: Path = Path("config"),
    holder_limit_per_token: int = 200,
    silo_top_holders_limit: int = 200,
    rpc_urls: dict[str, str] | None = None,
) -> dict[str, object]:
    """Return a non-mutating report for same-day holder discovery and admission."""

    _, as_of_ts_utc = denver_business_bounds_utc(business_date)
    manual_seed_wallets = load_legacy_customer_seed_wallets(
        config_dir,
        globs=holder_universe.legacy_seed_globs,
        force_include_wallets=holder_universe.force_include_wallets,
    )
    strategy_wallets = collect_strategy_wallets(
        markets_config=markets_config,
        wallet_products_config=wallet_products_config,
    )
    protocol_wallets = collect_evm_addresses_from_yaml(
        [
            config_dir / "markets.yaml",
            config_dir / "consumer_markets.yaml",
            config_dir / "wallet_products.yaml",
        ]
    )
    chain_scope = set(holder_universe.chain_id_map())
    price_map, price_issues = fetch_consumer_price_map(
        session=session,
        as_of_ts_utc=as_of_ts_utc,
        avant_tokens=avant_tokens,
        business_date=business_date,
        price_oracle=price_oracle,
        chain_scope=chain_scope,
    )
    token_decimals_by_key = resolve_token_decimals_map(session)
    discovery = discover_customer_candidate_wallets(
        session=session,
        business_date=business_date,
        markets_config=markets_config,
        consumer_markets_config=consumer_markets_config,
        avant_tokens=avant_tokens,
        holder_universe=holder_universe,
        routescan_client=routescan_client,
        rpc_client=rpc_client,
        price_map=price_map,
        manual_seed_wallets=manual_seed_wallets,
        strategy_wallets=strategy_wallets,
        protocol_wallets=protocol_wallets,
        holder_exclusions=holder_exclusions,
        silo_client=silo_client,
        holder_limit_per_token=holder_limit_per_token,
        silo_top_holders_limit=silo_top_holders_limit,
        rpc_urls=rpc_urls,
    )
    direct_wallets, verification_issues = _verify_customer_wallet_candidates(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        candidate_sources=discovery.candidate_sources,
        avant_tokens=avant_tokens,
        thresholds=thresholds,
        rpc_client=rpc_client,
        price_map=price_map,
        token_decimals_by_key=token_decimals_by_key,
        chain_scope=chain_scope,
    )
    direct_wallets_by_address = {wallet.wallet_address: wallet for wallet in direct_wallets}

    market_total_by_wallet: dict[str, Decimal] = defaultdict(lambda: ZERO)
    for wallet_address, rows in discovery.market_positions_by_wallet.items():
        for row in rows:
            market_total_by_wallet[wallet_address] += row.usd_value

    monitored_wallets = sorted(
        wallet_address
        for wallet_address in discovery.candidate_sources
        if (
            direct_wallets_by_address[wallet_address].verified_total_avant_usd
            >= holder_universe.raw_holder_threshold_usd
            or market_total_by_wallet.get(wallet_address, ZERO)
            >= holder_universe.raw_holder_threshold_usd
        )
    )

    previous_date = session.scalar(
        select(func.max(ConsumerHolderUniverseDaily.business_date)).where(
            ConsumerHolderUniverseDaily.business_date < business_date
        )
    )
    prior_wallets = {
        canonical_address(wallet_address)
        for wallet_address in session.scalars(
            select(ConsumerHolderUniverseDaily.wallet_address).where(
                ConsumerHolderUniverseDaily.business_date == previous_date
            )
        ).all()
    } if previous_date is not None else set()

    per_asset_detections = [
        {
            "chain_code": chain_code,
            "token_symbol": token_symbol,
            "wallet_count": len({canonical_address(holder.address) for holder in holders}),
        }
        for (chain_code, _token_address, token_symbol), holders in sorted(
            discovery.token_holders_by_token.items(),
            key=lambda item: (item[0][0], item[0][2]),
        )
    ]
    position_sourced_wallets = sorted(
        wallet_address
        for wallet_address in monitored_wallets
        if market_total_by_wallet.get(wallet_address, ZERO) >= holder_universe.raw_holder_threshold_usd
        and direct_wallets_by_address[wallet_address].verified_total_avant_usd
        < holder_universe.raw_holder_threshold_usd
    )

    return {
        "business_date": business_date.isoformat(),
        "as_of_ts_utc": as_of_ts_utc.isoformat(),
        "candidate_wallet_count": len(discovery.candidate_sources),
        "monitored_wallet_count": len(monitored_wallets),
        "new_wallet_addresses": sorted(set(monitored_wallets) - prior_wallets),
        "removed_wallet_addresses": sorted(prior_wallets - set(monitored_wallets)),
        "per_asset_detections": per_asset_detections,
        "position_sourced_wallet_count": len(position_sourced_wallets),
        "position_sourced_wallet_addresses": position_sourced_wallets,
        "excluded_candidates": {
            bucket: sorted({canonical_address(wallet) for wallet in wallets})
            for bucket, wallets in discovery.excluded_candidates.items()
        },
        "missing_market_coverage": discovery.missing_market_coverage,
        "issue_count": len(price_issues) + len(discovery.issues) + len(verification_issues),
    }


def build_customer_snapshot_markets_config(
    *,
    markets_config: MarketsConfig,
    avant_tokens: AvantTokensConfig,
    business_date: date,
    wallet_addresses: Iterable[str],
    wallet_balance_wallets: Iterable[str] | None = None,
    protocol_wallets_by_adapter: dict[str, dict[str, list[str]]] | None = None,
) -> MarketsConfig:
    """Build a reduced MarketsConfig for customer snapshot ingestion."""

    normalized_wallets = sorted({canonical_address(wallet) for wallet in wallet_addresses})
    protocol_wallets_by_adapter = protocol_wallets_by_adapter or {}
    wallet_balance_wallets = (
        normalized_wallets
        if wallet_balance_wallets is None
        else sorted({canonical_address(wallet) for wallet in wallet_balance_wallets})
    )

    def _scoped_wallets(protocol_code: str, chain_code: str) -> list[str]:
        protocol_scope = protocol_wallets_by_adapter.get(protocol_code)
        if protocol_wallets_by_adapter and protocol_scope is None:
            return []
        if protocol_scope is None:
            return normalized_wallets
        return sorted({canonical_address(wallet) for wallet in protocol_scope.get(chain_code, [])})

    wallet_balances: dict[str, WalletBalanceChainConfig] = {}
    for chain_code in sorted(set(DEFAULT_CONSUMER_CHAIN_IDS)):
        tokens = [
            WalletBalanceToken(
                symbol=token.symbol,
                address=token.token_address,
                decimals=token.decimals,
            )
            for token in active_avant_tokens(
                avant_tokens,
                business_date=business_date,
                chain_scope={chain_code},
            )
        ]
        if tokens:
            wallet_balances[chain_code] = WalletBalanceChainConfig(
                wallets=wallet_balance_wallets,
                tokens=tokens,
            )

    def _clone_chain_configs(section: dict[str, object], protocol_code: str) -> dict[str, object]:
        scoped_configs: dict[str, object] = {}
        for chain_code, chain_config in section.items():
            if chain_code not in DEFAULT_CONSUMER_CHAIN_IDS:
                continue
            wallets = _scoped_wallets(protocol_code, chain_code)
            if protocol_wallets_by_adapter and not wallets:
                continue
            scoped_configs[chain_code] = chain_config.model_copy(update={"wallets": wallets})
        return scoped_configs

    return MarketsConfig(
        aave_v3=_clone_chain_configs(markets_config.aave_v3, "aave_v3"),
        spark=_clone_chain_configs(markets_config.spark, "spark"),
        morpho=_clone_chain_configs(markets_config.morpho, "morpho"),
        euler_v2=_clone_chain_configs(markets_config.euler_v2, "euler_v2"),
        dolomite=_clone_chain_configs(markets_config.dolomite, "dolomite"),
        kamino=_clone_chain_configs(markets_config.kamino, "kamino"),
        pendle=_clone_chain_configs(markets_config.pendle, "pendle"),
        zest=_clone_chain_configs(markets_config.zest, "zest"),
        wallet_balances=wallet_balances,
        traderjoe_lp={},
        stakedao={},
        etherex={},
    )
