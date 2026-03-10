"""Customer cohort discovery, verification, and snapshot config helpers."""

from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

import httpx
import yaml
from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc
from core.config import (
    AvantToken,
    AvantTokensConfig,
    ConsumerThresholdsConfig,
    HolderExclusionsConfig,
    HolderUniverseConfig,
    MarketsConfig,
    WalletBalanceChainConfig,
    WalletBalanceToken,
    WalletProductsConfig,
    canonical_address,
)
from core.db.models import (
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
from core.pricing import PriceOracle
from core.types import DataQualityIssue, PriceRequest

ROUTESCAN_BASE_URL = "https://api.routescan.io"
EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
ERC20_BALANCE_OF_SELECTOR = "70a08231"
ERC4626_CONVERT_TO_ASSETS_SELECTOR = "07a2d13a"
DEFAULT_CONSUMER_CHAIN_IDS = {"avalanche": "43114", "ethereum": "1"}
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
    """Candidate discovery output plus raw token holder ledgers."""

    candidate_sources: dict[str, set[str]]
    token_holders_by_token: dict[tuple[str, str, str], list[HolderBalance]]
    issues: list[DataQualityIssue]


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


def discover_customer_candidate_wallets(
    *,
    session: Session,
    business_date: date,
    avant_tokens: AvantTokensConfig,
    holder_universe: HolderUniverseConfig,
    routescan_client: RouteScanClient,
    manual_seed_wallets: set[str],
    holder_limit_per_token: int = 200,
) -> CustomerCandidateDiscovery:
    """Build the daily candidate wallet set from deterministic discovery surfaces."""

    candidates: dict[str, set[str]] = defaultdict(set)
    token_holders_by_token: dict[tuple[str, str, str], list[HolderBalance]] = {}
    issues: list[DataQualityIssue] = []

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
            candidates[canonical_address(wallet_address)].add("prior_cohort")

    for wallet_address in manual_seed_wallets:
        candidates[canonical_address(wallet_address)].add("legacy_seed")

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
        candidates[canonical_address(wallet_address)].add("consumer_market_activity")

    as_of_ts_utc = end_utc
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
            candidates[canonical_address(holder.address)].add(
                f"routescan:{token.chain_code}:{token.symbol}"
            )

    return CustomerCandidateDiscovery(
        candidate_sources=candidates,
        token_holders_by_token=token_holders_by_token,
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
        chain_scope=set(DEFAULT_CONSUMER_CHAIN_IDS),
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

    active_tokens = active_avant_tokens(
        avant_tokens,
        business_date=business_date,
        chain_scope=set(DEFAULT_CONSUMER_CHAIN_IDS),
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
        if verified_total < thresholds.verified_min_total_avant_usd:
            continue
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


def _wallet_has_family_exposure(wallet: VerifiedCustomerWallet, family_code: str) -> bool:
    return any(
        balance.asset_family == family_code
        and balance.balance_raw > 0
        and balance.resolved
        and (balance.usd_value or Decimal("0")) > Decimal("0")
        for balance in wallet.balances
    )


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
            "discovery_sources_json": {"sources": list(wallet.discovery_sources)},
            "is_signoff_eligible": wallet.is_signoff_eligible,
            "exclusion_reason": wallet.exclusion_reason,
            "has_usd_exposure": _wallet_has_family_exposure(wallet, "usd"),
            "has_eth_exposure": _wallet_has_family_exposure(wallet, "eth"),
            "has_btc_exposure": _wallet_has_family_exposure(wallet, "btc"),
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
        if wallet.verified_total_avant_usd >= thresholds.cohort_min_total_avant_usd
    ]
    if not cohort_wallets:
        return 0

    rows = [
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of_ts_utc,
            "wallet_id": wallet_ids[wallet.wallet_address],
            "wallet_address": wallet.wallet_address,
            "verified_total_avant_usd": wallet.verified_total_avant_usd,
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
    wallet_products_config: WalletProductsConfig,
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
    holder_universe: HolderUniverseConfig,
    holder_exclusions: HolderExclusionsConfig,
    routescan_client: RouteScanClient,
    rpc_client: EvmBatchRpcClient,
    price_oracle: PriceOracle,
    config_dir: Path = Path("config"),
    holder_limit_per_token: int = 200,
    rpc_urls: dict[str, str] | None = None,
) -> CustomerCohortSyncSummary:
    """Discover, verify, and persist the tracked customer cohort for one day."""

    _, as_of_ts_utc = denver_business_bounds_utc(business_date)
    manual_seed_wallets = load_legacy_customer_seed_wallets(
        config_dir,
        globs=holder_universe.legacy_seed_globs,
        force_include_wallets=holder_universe.force_include_wallets,
    )
    discovery = discover_customer_candidate_wallets(
        session=session,
        business_date=business_date,
        avant_tokens=avant_tokens,
        holder_universe=holder_universe,
        routescan_client=routescan_client,
        manual_seed_wallets=manual_seed_wallets,
        holder_limit_per_token=holder_limit_per_token,
    )
    candidates = discovery.candidate_sources
    issues = list(discovery.issues)

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

    filtered_candidates = dict(candidates)
    if holder_universe.exclude_protocol_wallets_by_default:
        filtered_candidates = {
            wallet_address: sources
            for wallet_address, sources in filtered_candidates.items()
            if wallet_address not in strategy_wallets and wallet_address not in protocol_wallets
        }
    else:
        filtered_candidates = {
            wallet_address: sources
            for wallet_address, sources in filtered_candidates.items()
            if wallet_address not in strategy_wallets
        }
    explicit_excluded_wallets = excluded_holder_wallets(holder_exclusions)
    filtered_candidates = {
        wallet_address: sources
        for wallet_address, sources in filtered_candidates.items()
        if wallet_address not in explicit_excluded_wallets
    }

    contract_wallets: set[str] = set()
    if rpc_urls:
        for chain_code in sorted(set(DEFAULT_CONSUMER_CHAIN_IDS) & set(rpc_urls)):
            try:
                contract_wallets.update(
                    rpc_contract_addresses(
                        rpc_url=rpc_urls[chain_code],
                        addresses=filtered_candidates,
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
        filtered_candidates = {
            wallet_address: sources
            for wallet_address, sources in filtered_candidates.items()
            if wallet_address not in contract_wallets
        }

    wallet_ids = upsert_customer_wallets(
        session,
        set(discovery.candidate_sources) | set(filtered_candidates),
    )
    price_map, price_issues = fetch_consumer_price_map(
        session=session,
        as_of_ts_utc=as_of_ts_utc,
        avant_tokens=avant_tokens,
        business_date=business_date,
        price_oracle=price_oracle,
    )
    issues.extend(price_issues)
    token_decimals_by_key = resolve_token_decimals_map(session)
    verified_wallets, verification_issues = verify_customer_wallet_balances(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        candidate_sources=filtered_candidates,
        avant_tokens=avant_tokens,
        thresholds=thresholds,
        rpc_client=rpc_client,
        price_map=price_map,
        token_decimals_by_key=token_decimals_by_key,
    )
    issues.extend(verification_issues)
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
        verified_wallets=verified_wallets,
    )
    cohort_rows_written = write_consumer_cohort_daily(
        session=session,
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_ids=wallet_ids,
        verified_wallets=verified_wallets,
        thresholds=thresholds,
    )
    issues_written = _write_data_quality_issues(session, issues)

    return CustomerCohortSyncSummary(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        candidate_wallet_count=len(filtered_candidates),
        verified_wallet_count=verified_rows_written,
        cohort_wallet_count=cohort_rows_written,
        signoff_eligible_wallet_count=sum(
            1
            for wallet in verified_wallets
            if wallet.verified_total_avant_usd >= thresholds.cohort_min_total_avant_usd
            and wallet.is_signoff_eligible
        ),
        issues_written=issues_written,
    )


def build_customer_snapshot_markets_config(
    *,
    markets_config: MarketsConfig,
    avant_tokens: AvantTokensConfig,
    business_date: date,
    wallet_addresses: Iterable[str],
) -> MarketsConfig:
    """Build a reduced MarketsConfig for customer snapshot ingestion."""

    normalized_wallets = sorted({canonical_address(wallet) for wallet in wallet_addresses})
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
                wallets=normalized_wallets,
                tokens=tokens,
            )

    morpho = {
        chain_code: chain_config.model_copy(update={"wallets": normalized_wallets})
        for chain_code, chain_config in markets_config.morpho.items()
        if chain_code in DEFAULT_CONSUMER_CHAIN_IDS
    }
    euler_v2 = {
        chain_code: chain_config.model_copy(update={"wallets": normalized_wallets})
        for chain_code, chain_config in markets_config.euler_v2.items()
        if chain_code in DEFAULT_CONSUMER_CHAIN_IDS
    }

    return MarketsConfig(
        aave_v3={},
        spark={},
        morpho=morpho,
        euler_v2=euler_v2,
        dolomite={},
        kamino={},
        zest={},
        wallet_balances=wallet_balances,
        traderjoe_lp={},
        stakedao={},
        etherex={},
    )
