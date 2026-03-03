"""Helpers for read-only Dolomite wallet/account discovery scans."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Protocol

import yaml

from core.config import (
    DolomiteChainConfig,
    DolomiteMarket,
    WalletProductAssignment,
    canonical_address,
)

WAD = Decimal("1e18")


@dataclass(frozen=True)
class DolomiteDiscoveryRow:
    wallet_address: str
    account_number: int
    market_id: int
    symbol: str
    token_address: str
    supplied_usd: Decimal
    borrowed_usd: Decimal
    net_usd: Decimal
    abs_exposure_usd: Decimal


@dataclass(frozen=True)
class _MarketMeta:
    market_id: int
    symbol: str
    token_address: str
    decimals: int
    price_usd: Decimal


@dataclass(frozen=True)
class DolomiteDiscoveryResult:
    rows: list[DolomiteDiscoveryRow]
    warnings: list[str]
    market_ids_scanned: list[int]


class DolomiteDiscoveryRpcClient(Protocol):
    """Read-only Dolomite RPC surface needed for wallet/account discovery."""

    def get_num_markets(self, chain_code: str, margin_address: str) -> int:
        """Return number of market ids available on Dolomite margin."""

    def get_market_token_address(self, chain_code: str, margin_address: str, market_id: int) -> str:
        """Return market token address by market id."""

    def get_market_price(self, chain_code: str, margin_address: str, market_id: int) -> int:
        """Return market price raw value for a market id."""

    def get_account_wei(
        self,
        chain_code: str,
        margin_address: str,
        wallet_address: str,
        account_number: int,
        market_id: int,
    ) -> object:
        """Return signed market balance for wallet/account/market."""

    def get_erc20_decimals(self, chain_code: str, token_address: str) -> int:
        """Return ERC20 decimals for token address."""


def _normalize_price(price_raw: int, decimals: int) -> Decimal:
    exponent = 36 - decimals
    if exponent <= 0:
        return Decimal(price_raw)
    scale = Decimal(10) ** Decimal(exponent)
    return Decimal(price_raw) / scale


def _normalize_amount(raw_amount: int, decimals: int) -> Decimal:
    if decimals < 0:
        raise ValueError("decimals must be non-negative")
    return Decimal(raw_amount) / (Decimal(10) ** Decimal(decimals))


def _load_wallet_products_raw(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise ValueError("wallet products config must be a top-level YAML mapping")
    return loaded


def _legacy_wallets_for_groups(
    raw_wallet_products: dict[str, object],
    groups: list[str],
) -> tuple[set[str], list[str]]:
    warnings: list[str] = []
    wallets: set[str] = set()

    strategy_wallets = raw_wallet_products.get("STRATEGY_WALLETS")
    if not isinstance(strategy_wallets, dict):
        return wallets, warnings

    missing_groups: list[str] = []
    for group in groups:
        entries = strategy_wallets.get(group)
        if not isinstance(entries, list):
            missing_groups.append(group)
            continue
        for wallet in entries:
            if isinstance(wallet, str):
                wallets.add(canonical_address(wallet))

    if missing_groups:
        warnings.append(
            "wallet groups missing in STRATEGY_WALLETS: " + ", ".join(sorted(missing_groups))
        )

    return wallets, warnings


def _wallets_from_assignments(
    assignments: list[WalletProductAssignment],
    groups: list[str],
) -> tuple[set[str], list[str]]:
    mapping: dict[str, tuple[str, str]] = {
        "avUSD": ("stablecoin", "senior"),
        "avUSDx": ("stablecoin", "junior"),
        "avBTC": ("btc", "senior"),
        "avBTCx": ("btc", "junior"),
        "avETH": ("eth", "senior"),
        "avETHx": ("eth", "junior"),
    }

    requested_pairs: set[tuple[str, str]] = set()
    unsupported: list[str] = []
    for group in groups:
        pair = mapping.get(group)
        if pair is None:
            unsupported.append(group)
            continue
        requested_pairs.add(pair)

    wallets = {
        canonical_address(assignment.wallet_address)
        for assignment in assignments
        if assignment.wallet_type == "strategy"
        and (assignment.product_family, assignment.tranche) in requested_pairs
    }

    warnings: list[str] = []
    if unsupported:
        warnings.append("unsupported wallet group labels: " + ", ".join(sorted(unsupported)))

    return wallets, warnings


def wallet_candidates_for_groups(
    *,
    wallet_products_path: Path,
    wallet_groups: list[str],
    assignments: list[WalletProductAssignment],
) -> tuple[list[str], list[str]]:
    """Resolve candidate strategy wallets for requested wallet groups."""

    normalized_groups = [group.strip() for group in wallet_groups if group.strip()]
    if not normalized_groups:
        raise ValueError("wallet_groups must contain at least one non-empty group")

    warnings: list[str] = []
    legacy_wallets: set[str] = set()
    try:
        raw = _load_wallet_products_raw(wallet_products_path)
        legacy_wallets, legacy_warnings = _legacy_wallets_for_groups(raw, normalized_groups)
        warnings.extend(legacy_warnings)
    except Exception as exc:
        warnings.append(f"failed reading raw wallet products file: {exc}")

    if legacy_wallets:
        return sorted(legacy_wallets), warnings

    assignment_wallets, assignment_warnings = _wallets_from_assignments(
        assignments, normalized_groups
    )
    warnings.extend(assignment_warnings)
    if not assignment_wallets:
        warnings.append("no candidate wallets resolved from wallet assignments")
    return sorted(assignment_wallets), warnings


def _discover_market_ids(
    *,
    chain_code: str,
    chain_config: DolomiteChainConfig,
    rpc_client: DolomiteDiscoveryRpcClient,
    fallback_probe_max_market_id: int,
) -> tuple[list[int], list[str]]:
    warnings: list[str] = []
    margin_address = canonical_address(chain_config.margin)

    configured_ids = {market.id for market in chain_config.markets}

    try:
        num_markets = rpc_client.get_num_markets(chain_code, margin_address)
        if num_markets <= 0:
            raise RuntimeError("get_num_markets returned non-positive value")

        discovered_ids = sorted(set(range(num_markets)))
        return discovered_ids, warnings
    except Exception as exc:
        warnings.append(f"get_num_markets failed; using fallback ids: {exc}")

    fallback_ids = set(configured_ids)
    if fallback_probe_max_market_id >= 0:
        fallback_ids.update(range(fallback_probe_max_market_id + 1))
    return sorted(fallback_ids), warnings


def discover_wallet_positions(
    *,
    chain_code: str,
    chain_config: DolomiteChainConfig,
    wallets: list[str],
    rpc_client: DolomiteDiscoveryRpcClient,
    max_account_number: int,
    min_abs_exposure_usd: Decimal,
    fallback_probe_max_market_id: int = 63,
) -> DolomiteDiscoveryResult:
    """Scan candidate wallets/accounts/markets and return non-zero Dolomite exposures."""

    if max_account_number < 0:
        raise ValueError("max_account_number must be non-negative")
    if min_abs_exposure_usd < 0:
        raise ValueError("min_abs_exposure_usd must be non-negative")

    warnings: list[str] = []
    normalized_wallets = [canonical_address(wallet) for wallet in wallets]
    configured_market_by_id: dict[int, DolomiteMarket] = {
        market.id: market for market in chain_config.markets
    }

    market_ids, market_warnings = _discover_market_ids(
        chain_code=chain_code,
        chain_config=chain_config,
        rpc_client=rpc_client,
        fallback_probe_max_market_id=fallback_probe_max_market_id,
    )
    warnings.extend(market_warnings)

    margin_address = canonical_address(chain_config.margin)
    market_meta_by_id: dict[int, _MarketMeta] = {}
    for market_id in market_ids:
        configured = configured_market_by_id.get(market_id)
        symbol = configured.symbol if configured is not None else f"market_{market_id}"
        decimals = configured.decimals if configured is not None else None

        try:
            token_address = canonical_address(
                rpc_client.get_market_token_address(chain_code, margin_address, market_id)
            )
            price_raw = rpc_client.get_market_price(chain_code, margin_address, market_id)
            if decimals is None:
                decimals = rpc_client.get_erc20_decimals(chain_code, token_address)
            price_usd = _normalize_price(price_raw, decimals)
        except Exception as exc:
            warnings.append(f"skipping market {market_id}: {exc}")
            continue

        market_meta_by_id[market_id] = _MarketMeta(
            market_id=market_id,
            symbol=symbol,
            token_address=token_address,
            decimals=decimals,
            price_usd=price_usd,
        )

    rows: list[DolomiteDiscoveryRow] = []
    account_numbers = range(max_account_number + 1)
    for wallet in normalized_wallets:
        for account_number in account_numbers:
            for market_id, meta in market_meta_by_id.items():
                try:
                    account_wei = rpc_client.get_account_wei(
                        chain_code,
                        margin_address,
                        wallet,
                        account_number,
                        market_id,
                    )
                except Exception:
                    continue

                value = int(getattr(account_wei, "value", 0))
                is_positive = bool(getattr(account_wei, "is_positive", False))
                if value == 0:
                    continue

                amount = _normalize_amount(value, meta.decimals)
                supplied_usd = amount * meta.price_usd if is_positive else Decimal("0")
                borrowed_usd = amount * meta.price_usd if not is_positive else Decimal("0")
                net_usd = supplied_usd - borrowed_usd
                abs_exposure_usd = supplied_usd + borrowed_usd
                if abs_exposure_usd < min_abs_exposure_usd:
                    continue

                rows.append(
                    DolomiteDiscoveryRow(
                        wallet_address=wallet,
                        account_number=account_number,
                        market_id=market_id,
                        symbol=meta.symbol,
                        token_address=meta.token_address,
                        supplied_usd=supplied_usd,
                        borrowed_usd=borrowed_usd,
                        net_usd=net_usd,
                        abs_exposure_usd=abs_exposure_usd,
                    )
                )

    rows.sort(key=lambda row: row.abs_exposure_usd, reverse=True)
    return DolomiteDiscoveryResult(
        rows=rows,
        warnings=warnings,
        market_ids_scanned=sorted(market_meta_by_id.keys()),
    )
