"""Customer cohort seed generation helpers for Sprint 08 prep."""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from pathlib import Path

import httpx
import yaml

from core.config import MarketsConfig, WalletProductsConfig, canonical_address

ROUTESCAN_BASE_URL = "https://api.routescan.io"
EVM_ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


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
