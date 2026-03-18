"""Persisted holder dashboard rollups and shared wallet/product helpers."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, timedelta
from decimal import Decimal
from statistics import median

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.holder_behavior_engine import HolderBehaviorEngine
from core.config import (
    AvantTokensConfig,
    ConsumerThresholdsConfig,
    HolderProtocolMapConfig,
)
from core.consumer_debank_visibility import is_excluded_visibility_protocol
from core.db.models import (
    ConsumerDebankTokenDaily,
    ConsumerDebankWalletDaily,
    ConsumerHolderUniverseDaily,
    HolderBehaviorDaily,
    HolderProductSegmentDaily,
    HolderProtocolDeployDaily,
    HolderWalletProductDaily,
)

ZERO = Decimal("0")
DEBANK_LOOKBACK_DAYS = 7
PRODUCT_SCOPES = ("all", "avusd", "aveth", "avbtc")
PRODUCT_SCOPE_LABELS = {
    "all": "All Products",
    "avusd": "avUSD",
    "aveth": "avETH",
    "avbtc": "avBTC",
}
PRODUCT_SCOPE_TO_FAMILY = {
    "avusd": "usd",
    "aveth": "eth",
    "avbtc": "btc",
}
COHORT_SEGMENTS = ("all", "verified", "core", "whale")


@dataclass(frozen=True)
class HolderDashboardBuildSummary:
    business_date: date
    segment_rows_written: int
    protocol_rows_written: int


@dataclass
class WalletDashboardMetrics:
    wallet_id: int
    wallet_address: str
    is_signoff_eligible: bool
    has_any_activity: bool
    has_any_borrow: bool
    multi_asset_flag: bool
    risk_band: str
    health_factor_min: Decimal | None
    leverage_ratio: Decimal | None
    age_days: int | None
    observed_by_scope: dict[str, Decimal] = field(default_factory=dict)
    idle_by_scope: dict[str, Decimal] = field(default_factory=dict)
    collateral_by_scope: dict[str, Decimal] = field(default_factory=dict)
    fixed_yield_by_scope: dict[str, Decimal] = field(default_factory=dict)
    yield_token_by_scope: dict[str, Decimal] = field(default_factory=dict)
    other_defi_by_scope: dict[str, Decimal] = field(default_factory=dict)
    base_by_scope: dict[str, Decimal] = field(default_factory=dict)
    staked_by_scope: dict[str, Decimal] = field(default_factory=dict)
    boosted_by_scope: dict[str, Decimal] = field(default_factory=dict)
    borrowed_by_scope: dict[str, Decimal] = field(default_factory=dict)
    asset_symbols_by_scope: dict[str, tuple[str, ...]] = field(default_factory=dict)


@dataclass(frozen=True)
class WalletProductMetrics:
    wallet_id: int
    wallet_address: str
    product_scope: str
    monitored_presence_usd: Decimal
    observed_exposure_usd: Decimal
    wallet_held_usd: Decimal
    canonical_deployed_usd: Decimal
    external_fixed_yield_pt_usd: Decimal
    external_yield_token_yt_usd: Decimal
    external_other_defi_usd: Decimal
    has_any_defi_activity: bool
    has_any_defi_borrow: bool
    has_canonical_activity: bool
    segment: str | None
    is_attributed: bool
    asset_symbols: tuple[str, ...]
    borrowed_usd: Decimal
    leverage_ratio: Decimal | None
    health_factor_min: Decimal | None
    risk_band: str
    age_days: int | None
    multi_asset_flag: bool
    aum_delta_7d_usd: Decimal
    aum_delta_7d_pct: Decimal | None
    staked_usd: Decimal


@dataclass(frozen=True)
class HolderDashboardContext:
    business_date: date
    as_of_ts_utc: object
    wallets: list[WalletDashboardMetrics]
    prior_exposure_by_wallet_scope: dict[tuple[int, str], Decimal]


def _share(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator <= ZERO:
        return None
    return numerator / denominator


def _pct(count: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(count) / Decimal(denominator)


def family_for_scope(product_scope: str) -> str | None:
    return PRODUCT_SCOPE_TO_FAMILY.get(product_scope)


def segment_for_exposure(
    exposure_usd: Decimal,
    *,
    thresholds: ConsumerThresholdsConfig,
) -> str | None:
    if exposure_usd < thresholds.verified_min_total_avant_usd:
        return None
    if exposure_usd < thresholds.cohort_min_total_avant_usd:
        return "verified"
    if exposure_usd < thresholds.whales.wallet_usd_threshold:
        return "core"
    return "whale"


def family_matches_token_symbol(family_code: str, token_symbol: str) -> bool:
    normalized = token_symbol.upper()
    if family_code == "usd":
        return "SAVUSD" in normalized or "AVUSD" in normalized
    if family_code == "eth":
        return "SAVETH" in normalized or "AVETH" in normalized
    if family_code == "btc":
        return "SAVBTC" in normalized or "AVBTC" in normalized
    return False


def family_for_token_symbol(token_symbol: str) -> str | None:
    for family_code in ("usd", "eth", "btc"):
        if family_matches_token_symbol(family_code, token_symbol):
            return family_code
    return None


def _avant_symbol_registry(
    avant_tokens: AvantTokensConfig,
    *,
    business_date: date,
) -> tuple[dict[str, tuple[str, str]], list[tuple[str, tuple[str, str]]]]:
    by_symbol: dict[str, tuple[str, str]] = {}
    symbols_sorted: list[tuple[str, tuple[str, str]]] = []
    seen: set[str] = set()
    for token in avant_tokens.tokens:
        if token.active_from is not None and business_date < token.active_from:
            continue
        if token.active_to is not None and business_date > token.active_to:
            continue
        symbol_key = token.symbol.strip().upper()
        if not symbol_key:
            continue
        by_symbol.setdefault(symbol_key, (token.asset_family, token.wrapper_class))
        if symbol_key in seen:
            continue
        seen.add(symbol_key)
        symbols_sorted.append((symbol_key, (token.asset_family, token.wrapper_class)))
    symbols_sorted.sort(key=lambda item: len(item[0]), reverse=True)
    return by_symbol, symbols_sorted


def _family_and_wrapper_for_token_symbol(
    token_symbol: str,
    *,
    registry_by_symbol: dict[str, tuple[str, str]],
    registry_symbols_sorted: list[tuple[str, tuple[str, str]]],
) -> tuple[str | None, str | None]:
    normalized = token_symbol.strip().upper()
    if not normalized:
        return None, None
    if normalized.startswith("PT-") or normalized.startswith("YT-"):
        return family_for_token_symbol(token_symbol), None
    direct_match = registry_by_symbol.get(normalized)
    if direct_match is not None:
        return direct_match
    for symbol_key, value in registry_symbols_sorted:
        if symbol_key in normalized:
            return value
    return family_for_token_symbol(token_symbol), None


def is_pendle_pt(protocol_code: str, token_symbol: str) -> bool:
    return "pendle" in protocol_code.lower() and token_symbol.upper().startswith("PT-")


def is_pendle_yt(protocol_code: str, token_symbol: str) -> bool:
    return "pendle" in protocol_code.lower() and token_symbol.upper().startswith("YT-")


def _protocol_use_category(
    *,
    protocol_code: str,
    token_symbol: str,
    in_config_surface: bool,
    mapped_primary_use: str | None = None,
) -> str:
    if in_config_surface:
        return "collateral"
    if mapped_primary_use is not None and mapped_primary_use not in {"other", "other_defi"}:
        return mapped_primary_use

    normalized_protocol = protocol_code.lower()
    normalized_symbol = token_symbol.upper()
    if is_pendle_pt(normalized_protocol, normalized_symbol):
        return "fixed_yield"
    if is_pendle_yt(normalized_protocol, normalized_symbol):
        return "yield_token"
    if (
        "eigen" in normalized_protocol
        or "symbiotic" in normalized_protocol
        or "renzo" in normalized_protocol
        or "kelp" in normalized_protocol
        or "restake" in normalized_symbol
    ):
        return "restaking"
    if (
        "vault" in normalized_protocol
        or "yearn" in normalized_protocol
        or "beefy" in normalized_protocol
        or "sommelier" in normalized_protocol
        or "vault" in normalized_symbol
    ):
        return "vault"
    if (
        "traderjoe" in normalized_protocol
        or "trader_joe" in normalized_protocol
        or "uniswap" in normalized_protocol
        or "curve" in normalized_protocol
        or "camelot" in normalized_protocol
        or "balancer" in normalized_protocol
        or "etherex" in normalized_protocol
        or normalized_symbol.startswith("LP-")
        or normalized_symbol.endswith("-LP")
    ):
        return "lp"
    return "other_defi"


def _primary_use_label(category: str) -> str:
    return {
        "collateral": "Collateral",
        "fixed_yield": "Fixed yield",
        "yield_token": "Yield token",
        "restaking": "Restaking",
        "vault": "Vault",
        "lp": "LP",
        "other": "Other DeFi",
        "other_defi": "Other DeFi",
    }.get(category, "Other DeFi")


def _exposure_by_scope(
    *,
    wallet_usd: dict[str, Decimal],
    deployed_usd: dict[str, Decimal],
    fixed_yield_usd: dict[str, Decimal],
    yield_token_usd: dict[str, Decimal],
    other_defi_usd: dict[str, Decimal],
) -> dict[str, Decimal]:
    family_scopes = {
        "avusd": (
            wallet_usd.get("usd", ZERO)
            + deployed_usd.get("usd", ZERO)
            + fixed_yield_usd.get("usd", ZERO)
            + yield_token_usd.get("usd", ZERO)
            + other_defi_usd.get("usd", ZERO)
        ),
        "aveth": (
            wallet_usd.get("eth", ZERO)
            + deployed_usd.get("eth", ZERO)
            + fixed_yield_usd.get("eth", ZERO)
            + yield_token_usd.get("eth", ZERO)
            + other_defi_usd.get("eth", ZERO)
        ),
        "avbtc": (
            wallet_usd.get("btc", ZERO)
            + deployed_usd.get("btc", ZERO)
            + fixed_yield_usd.get("btc", ZERO)
            + yield_token_usd.get("btc", ZERO)
            + other_defi_usd.get("btc", ZERO)
        ),
    }
    family_scopes["all"] = sum(family_scopes.values(), ZERO)
    return family_scopes


def _resolve_debank_date(session: Session, target_date: date) -> date | None:
    """Return *target_date* if DeBank wallet rows exist, else the most recent
    earlier date within DEBANK_LOOKBACK_DAYS.  Returns None when no data at all."""
    has_exact = session.scalar(
        select(func.count())
        .select_from(ConsumerDebankWalletDaily)
        .where(ConsumerDebankWalletDaily.business_date == target_date)
    )
    if has_exact:
        return target_date
    fallback = session.scalar(
        select(func.max(ConsumerDebankWalletDaily.business_date)).where(
            ConsumerDebankWalletDaily.business_date
            >= target_date - timedelta(days=DEBANK_LOOKBACK_DAYS),
            ConsumerDebankWalletDaily.business_date < target_date,
        )
    )
    return fallback


def _build_wallet_metrics(
    *,
    business_date: date,
    holder_rows: list[HolderBehaviorDaily],
    visibility_rows: list[ConsumerDebankWalletDaily],
    token_rows: list[ConsumerDebankTokenDaily],
    balance_rows,
    consumer_rows,
    first_seen_by_wallet: dict[int, date],
    registry_by_symbol: dict[str, tuple[str, str]],
    registry_symbols_sorted: list[tuple[str, tuple[str, str]]],
) -> list[WalletDashboardMetrics]:
    visibility_by_wallet = {row.wallet_id: row for row in visibility_rows}
    token_rows_by_wallet: dict[int, list[ConsumerDebankTokenDaily]] = defaultdict(list)
    for row in token_rows:
        token_rows_by_wallet[row.wallet_id].append(row)

    staked_by_wallet_family: dict[int, dict[str, Decimal]] = {}
    for row in holder_rows:
        staked_by_wallet_family[row.wallet_id] = {
            "usd": row.wallet_staked_usd_usd + row.deployed_staked_usd_usd,
            "eth": row.wallet_staked_eth_usd + row.deployed_staked_eth_usd,
            "btc": row.wallet_staked_btc_usd + row.deployed_staked_btc_usd,
        }
    canonical_wrapper_by_wallet_family: dict[int, dict[str, dict[str, Decimal]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: ZERO))
    )
    asset_symbols_by_wallet_family: dict[int, dict[str, set[str]]] = defaultdict(
        lambda: defaultdict(set)
    )
    for row in balance_rows:
        asset_symbols_by_wallet_family[row.wallet_id][row.asset_family].add(row.symbol)
        canonical_wrapper_by_wallet_family[row.wallet_id][row.asset_family][
            row.wrapper_class
        ] += row.usd_value
    for row in consumer_rows:
        if row.collateral_symbol != "unknown":
            asset_symbols_by_wallet_family[row.wallet_id][row.collateral_family].add(
                row.collateral_symbol
            )
        if row.collateral_family != "unknown":
            canonical_wrapper_by_wallet_family[row.wallet_id][row.collateral_family][
                row.collateral_wrapper_class
            ] += row.collateral_usd

    wallet_metrics: list[WalletDashboardMetrics] = []
    for row in holder_rows:
        visibility_row = visibility_by_wallet.get(row.wallet_id)
        wallet_usd = {
            "usd": row.wallet_family_usd_usd,
            "eth": row.wallet_family_eth_usd,
            "btc": row.wallet_family_btc_usd,
        }
        deployed_usd = {
            "usd": row.deployed_family_usd_usd,
            "eth": row.deployed_family_eth_usd,
            "btc": row.deployed_family_btc_usd,
        }
        fixed_yield_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        yield_token_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        other_defi_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        external_base_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        external_staked_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        external_boosted_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        asset_symbols_family = asset_symbols_by_wallet_family[row.wallet_id]
        for token_row in token_rows_by_wallet.get(row.wallet_id, []):
            if token_row.leg_type == "borrow" or token_row.in_config_surface:
                continue
            if is_excluded_visibility_protocol(token_row.protocol_code):
                continue
            family_code, wrapper_class = _family_and_wrapper_for_token_symbol(
                token_row.token_symbol,
                registry_by_symbol=registry_by_symbol,
                registry_symbols_sorted=registry_symbols_sorted,
            )
            if family_code is None:
                continue
            asset_symbols_family[family_code].add(token_row.token_symbol)
            if is_pendle_pt(token_row.protocol_code, token_row.token_symbol):
                fixed_yield_usd[family_code] += token_row.usd_value
            elif is_pendle_yt(token_row.protocol_code, token_row.token_symbol):
                yield_token_usd[family_code] += token_row.usd_value
            else:
                other_defi_usd[family_code] += token_row.usd_value
            if wrapper_class == "base":
                external_base_usd[family_code] += token_row.usd_value
            elif wrapper_class == "staked":
                external_staked_usd[family_code] += token_row.usd_value
            elif wrapper_class == "boosted":
                external_boosted_usd[family_code] += token_row.usd_value

        observed_by_scope = _exposure_by_scope(
            wallet_usd=wallet_usd,
            deployed_usd=deployed_usd,
            fixed_yield_usd=fixed_yield_usd,
            yield_token_usd=yield_token_usd,
            other_defi_usd=other_defi_usd,
        )
        idle_by_scope = {
            "avusd": wallet_usd.get("usd", ZERO),
            "aveth": wallet_usd.get("eth", ZERO),
            "avbtc": wallet_usd.get("btc", ZERO),
        }
        idle_by_scope["all"] = sum(idle_by_scope.values(), ZERO)
        collateral_by_scope = {
            "avusd": deployed_usd.get("usd", ZERO),
            "aveth": deployed_usd.get("eth", ZERO),
            "avbtc": deployed_usd.get("btc", ZERO),
        }
        collateral_by_scope["all"] = sum(collateral_by_scope.values(), ZERO)
        fixed_yield_by_scope = {
            "avusd": fixed_yield_usd.get("usd", ZERO),
            "aveth": fixed_yield_usd.get("eth", ZERO),
            "avbtc": fixed_yield_usd.get("btc", ZERO),
        }
        fixed_yield_by_scope["all"] = sum(fixed_yield_by_scope.values(), ZERO)
        yield_token_by_scope = {
            "avusd": yield_token_usd.get("usd", ZERO),
            "aveth": yield_token_usd.get("eth", ZERO),
            "avbtc": yield_token_usd.get("btc", ZERO),
        }
        yield_token_by_scope["all"] = sum(yield_token_by_scope.values(), ZERO)
        other_defi_by_scope = {
            "avusd": other_defi_usd.get("usd", ZERO),
            "aveth": other_defi_usd.get("eth", ZERO),
            "avbtc": other_defi_usd.get("btc", ZERO),
        }
        other_defi_by_scope["all"] = sum(other_defi_by_scope.values(), ZERO)
        canonical_wrapper_family = canonical_wrapper_by_wallet_family[row.wallet_id]
        base_by_scope = {
            "avusd": canonical_wrapper_family["usd"].get("base", ZERO)
            + external_base_usd.get("usd", ZERO),
            "aveth": canonical_wrapper_family["eth"].get("base", ZERO)
            + external_base_usd.get("eth", ZERO),
            "avbtc": canonical_wrapper_family["btc"].get("base", ZERO)
            + external_base_usd.get("btc", ZERO),
        }
        base_by_scope["all"] = row.total_base_usd + sum(external_base_usd.values(), ZERO)
        staked_by_scope = {
            "avusd": staked_by_wallet_family[row.wallet_id].get("usd", ZERO),
            "aveth": staked_by_wallet_family[row.wallet_id].get("eth", ZERO),
            "avbtc": staked_by_wallet_family[row.wallet_id].get("btc", ZERO),
        }
        staked_by_scope["avusd"] += external_staked_usd.get("usd", ZERO)
        staked_by_scope["aveth"] += external_staked_usd.get("eth", ZERO)
        staked_by_scope["avbtc"] += external_staked_usd.get("btc", ZERO)
        staked_by_scope["all"] = row.total_staked_usd + sum(external_staked_usd.values(), ZERO)
        boosted_by_scope = {
            "avusd": canonical_wrapper_family["usd"].get("boosted", ZERO)
            + external_boosted_usd.get("usd", ZERO),
            "aveth": canonical_wrapper_family["eth"].get("boosted", ZERO)
            + external_boosted_usd.get("eth", ZERO),
            "avbtc": canonical_wrapper_family["btc"].get("boosted", ZERO)
            + external_boosted_usd.get("btc", ZERO),
        }
        boosted_by_scope["all"] = row.total_boosted_usd + sum(
            external_boosted_usd.values(),
            ZERO,
        )
        borrowed_by_scope = {
            "avusd": row.borrowed_usd if deployed_usd.get("usd", ZERO) > ZERO else ZERO,
            "aveth": row.borrowed_usd if deployed_usd.get("eth", ZERO) > ZERO else ZERO,
            "avbtc": row.borrowed_usd if deployed_usd.get("btc", ZERO) > ZERO else ZERO,
            "all": row.borrowed_usd,
        }
        symbol_sets_by_scope = {
            "avusd": tuple(sorted(asset_symbols_family.get("usd", set()))),
            "aveth": tuple(sorted(asset_symbols_family.get("eth", set()))),
            "avbtc": tuple(sorted(asset_symbols_family.get("btc", set()))),
        }
        symbol_sets_by_scope["all"] = tuple(
            sorted({symbol for symbols in symbol_sets_by_scope.values() for symbol in symbols})
        )
        first_seen = first_seen_by_wallet.get(row.wallet_id)
        age_days = (business_date - first_seen).days if first_seen is not None else None
        wallet_metrics.append(
            WalletDashboardMetrics(
                wallet_id=row.wallet_id,
                wallet_address=row.wallet_address,
                is_signoff_eligible=row.is_signoff_eligible,
                has_any_activity=bool(visibility_row.has_any_activity) if visibility_row else False,
                has_any_borrow=bool(visibility_row.has_any_borrow) if visibility_row else False,
                multi_asset_flag=row.multi_asset_flag,
                risk_band=row.risk_band,
                health_factor_min=row.health_factor_min,
                leverage_ratio=row.leverage_ratio,
                age_days=age_days,
                observed_by_scope=observed_by_scope,
                idle_by_scope=idle_by_scope,
                collateral_by_scope=collateral_by_scope,
                fixed_yield_by_scope=fixed_yield_by_scope,
                yield_token_by_scope=yield_token_by_scope,
                other_defi_by_scope=other_defi_by_scope,
                base_by_scope=base_by_scope,
                staked_by_scope=staked_by_scope,
                boosted_by_scope=boosted_by_scope,
                borrowed_by_scope=borrowed_by_scope,
                asset_symbols_by_scope=symbol_sets_by_scope,
            )
        )
    return wallet_metrics


def _build_wallet_product_metrics(
    *,
    context: HolderDashboardContext,
    thresholds: ConsumerThresholdsConfig,
) -> list[WalletProductMetrics]:
    rows: list[WalletProductMetrics] = []
    for wallet in context.wallets:
        for product_scope in PRODUCT_SCOPES:
            monitored_presence_usd = wallet.observed_by_scope.get(product_scope, ZERO)
            if monitored_presence_usd < thresholds.verified_min_total_avant_usd:
                continue
            observed_exposure_usd = monitored_presence_usd
            prior_exposure_usd = context.prior_exposure_by_wallet_scope.get(
                (wallet.wallet_id, product_scope),
                ZERO,
            )
            aum_delta_7d_usd = observed_exposure_usd - prior_exposure_usd
            rows.append(
                WalletProductMetrics(
                    wallet_id=wallet.wallet_id,
                    wallet_address=wallet.wallet_address,
                    product_scope=product_scope,
                    monitored_presence_usd=monitored_presence_usd,
                    observed_exposure_usd=observed_exposure_usd,
                    wallet_held_usd=wallet.idle_by_scope.get(product_scope, ZERO),
                    canonical_deployed_usd=wallet.collateral_by_scope.get(product_scope, ZERO),
                    external_fixed_yield_pt_usd=wallet.fixed_yield_by_scope.get(
                        product_scope,
                        ZERO,
                    ),
                    external_yield_token_yt_usd=wallet.yield_token_by_scope.get(
                        product_scope,
                        ZERO,
                    ),
                    external_other_defi_usd=wallet.other_defi_by_scope.get(product_scope, ZERO),
                    has_any_defi_activity=wallet.has_any_activity,
                    has_any_defi_borrow=wallet.has_any_borrow,
                    has_canonical_activity=(
                        wallet.collateral_by_scope.get(product_scope, ZERO)
                        >= thresholds.classification_dust_floor_usd
                    )
                    or (
                        wallet.borrowed_by_scope.get(product_scope, ZERO)
                        > thresholds.leveraged_borrow_usd_floor
                    ),
                    segment=segment_for_exposure(
                        observed_exposure_usd,
                        thresholds=thresholds,
                    ),
                    is_attributed=observed_exposure_usd >= thresholds.verified_min_total_avant_usd,
                    asset_symbols=wallet.asset_symbols_by_scope.get(product_scope, ()),
                    borrowed_usd=wallet.borrowed_by_scope.get(product_scope, ZERO),
                    leverage_ratio=wallet.leverage_ratio,
                    health_factor_min=wallet.health_factor_min,
                    risk_band=wallet.risk_band,
                    age_days=wallet.age_days,
                    multi_asset_flag=wallet.multi_asset_flag,
                    aum_delta_7d_usd=aum_delta_7d_usd,
                    aum_delta_7d_pct=(
                        _share(aum_delta_7d_usd, prior_exposure_usd)
                        if prior_exposure_usd > ZERO
                        else None
                    ),
                    staked_usd=wallet.staked_by_scope.get(product_scope, ZERO),
                )
            )
    return rows


def _build_prior_exposure_map(
    *,
    holder_rows: list[HolderBehaviorDaily],
    token_rows: list[ConsumerDebankTokenDaily],
    registry_by_symbol: dict[str, tuple[str, str]],
    registry_symbols_sorted: list[tuple[str, tuple[str, str]]],
) -> dict[tuple[int, str], Decimal]:
    token_rows_by_wallet: dict[int, list[ConsumerDebankTokenDaily]] = defaultdict(list)
    for row in token_rows:
        token_rows_by_wallet[row.wallet_id].append(row)

    prior_exposure: dict[tuple[int, str], Decimal] = {}
    for row in holder_rows:
        wallet_usd = {
            "usd": row.wallet_family_usd_usd,
            "eth": row.wallet_family_eth_usd,
            "btc": row.wallet_family_btc_usd,
        }
        deployed_usd = {
            "usd": row.deployed_family_usd_usd,
            "eth": row.deployed_family_eth_usd,
            "btc": row.deployed_family_btc_usd,
        }
        fixed_yield_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        yield_token_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        other_defi_usd: dict[str, Decimal] = defaultdict(lambda: ZERO)
        for token_row in token_rows_by_wallet.get(row.wallet_id, []):
            if token_row.leg_type == "borrow" or token_row.in_config_surface:
                continue
            if is_excluded_visibility_protocol(token_row.protocol_code):
                continue
            family_code, _wrapper_class = _family_and_wrapper_for_token_symbol(
                token_row.token_symbol,
                registry_by_symbol=registry_by_symbol,
                registry_symbols_sorted=registry_symbols_sorted,
            )
            if family_code is None:
                continue
            if is_pendle_pt(token_row.protocol_code, token_row.token_symbol):
                fixed_yield_usd[family_code] += token_row.usd_value
            elif is_pendle_yt(token_row.protocol_code, token_row.token_symbol):
                yield_token_usd[family_code] += token_row.usd_value
            else:
                other_defi_usd[family_code] += token_row.usd_value
        exposure = _exposure_by_scope(
            wallet_usd=wallet_usd,
            deployed_usd=deployed_usd,
            fixed_yield_usd=fixed_yield_usd,
            yield_token_usd=yield_token_usd,
            other_defi_usd=other_defi_usd,
        )
        for scope, usd_value in exposure.items():
            prior_exposure[(row.wallet_id, scope)] = usd_value
    return prior_exposure


def build_holder_dashboard_context(
    *,
    session: Session,
    business_date: date,
    avant_tokens: AvantTokensConfig,
    thresholds: ConsumerThresholdsConfig,
) -> HolderDashboardContext:
    registry_by_symbol, registry_symbols_sorted = _avant_symbol_registry(
        avant_tokens,
        business_date=business_date,
    )
    holder_rows = session.scalars(
        select(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
    ).all()
    if not holder_rows:
        return HolderDashboardContext(
            business_date=business_date,
            as_of_ts_utc=None,
            wallets=[],
            prior_exposure_by_wallet_scope={},
        )

    debank_date = _resolve_debank_date(session, business_date)
    if debank_date is not None:
        visibility_rows = session.scalars(
            select(ConsumerDebankWalletDaily).where(
                ConsumerDebankWalletDaily.business_date == debank_date
            )
        ).all()
        token_rows = session.scalars(
            select(ConsumerDebankTokenDaily).where(
                ConsumerDebankTokenDaily.business_date == debank_date
            )
        ).all()
    else:
        visibility_rows = []
        token_rows = []
    helper = HolderBehaviorEngine(
        session,
        avant_tokens=avant_tokens,
        thresholds=thresholds,
    )
    wallet_ids = sorted({row.wallet_id for row in holder_rows})
    as_of_ts_utc = max(row.as_of_ts_utc for row in holder_rows)
    balance_rows = helper._load_wallet_balance_rows(
        wallet_ids=wallet_ids,
        as_of_ts_utc=as_of_ts_utc,
        business_date=business_date,
    )
    consumer_rows = helper._load_consumer_position_rows(
        wallet_ids=wallet_ids,
        as_of_ts_utc=as_of_ts_utc,
        business_date=business_date,
    )
    first_seen_by_wallet = {
        wallet_id: first_seen
        for wallet_id, first_seen in session.execute(
            select(
                ConsumerHolderUniverseDaily.wallet_id,
                func.min(ConsumerHolderUniverseDaily.business_date),
            )
            .where(ConsumerHolderUniverseDaily.wallet_id.in_(wallet_ids))
            .group_by(ConsumerHolderUniverseDaily.wallet_id)
        ).all()
    }
    prior_holder_rows = session.scalars(
        select(HolderBehaviorDaily).where(
            HolderBehaviorDaily.business_date == business_date - timedelta(days=7)
        )
    ).all()
    prior_debank_date = _resolve_debank_date(session, business_date - timedelta(days=7))
    prior_token_rows = (
        session.scalars(
            select(ConsumerDebankTokenDaily).where(
                ConsumerDebankTokenDaily.business_date == prior_debank_date
            )
        ).all()
        if prior_debank_date is not None
        else []
    )
    wallets = _build_wallet_metrics(
        business_date=business_date,
        holder_rows=holder_rows,
        visibility_rows=visibility_rows,
        token_rows=token_rows,
        balance_rows=balance_rows,
        consumer_rows=consumer_rows,
        first_seen_by_wallet=first_seen_by_wallet,
        registry_by_symbol=registry_by_symbol,
        registry_symbols_sorted=registry_symbols_sorted,
    )
    timestamps = [row.as_of_ts_utc for row in holder_rows]
    timestamps.extend(row.as_of_ts_utc for row in visibility_rows)
    timestamps.extend(row.as_of_ts_utc for row in token_rows)
    if timestamps:
        as_of_ts_utc = max(timestamps)
    return HolderDashboardContext(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallets=wallets,
        prior_exposure_by_wallet_scope=_build_prior_exposure_map(
            holder_rows=prior_holder_rows,
            token_rows=prior_token_rows,
            registry_by_symbol=registry_by_symbol,
            registry_symbols_sorted=registry_symbols_sorted,
        ),
    )


class HolderDashboardEngine:
    """Build persisted holder dashboard segment and protocol deployment tables."""

    def __init__(
        self,
        session: Session,
        *,
        avant_tokens: AvantTokensConfig,
        thresholds: ConsumerThresholdsConfig,
        holder_protocol_map: HolderProtocolMapConfig | None = None,
    ) -> None:
        self.session = session
        self.avant_tokens = avant_tokens
        self.thresholds = thresholds
        self.holder_protocol_map = holder_protocol_map

    def _protocol_mapping(self, protocol_code: str) -> tuple[str, str]:
        normalized = protocol_code.lower()
        if self.holder_protocol_map is None:
            return normalized, "other"
        entry = self.holder_protocol_map.by_protocol_code().get(normalized)
        if entry is None:
            return normalized, self.holder_protocol_map.defaults.primary_use
        return entry.canonical_protocol_code or normalized, entry.primary_use

    def compute_daily(self, *, business_date: date) -> HolderDashboardBuildSummary:
        context = build_holder_dashboard_context(
            session=self.session,
            business_date=business_date,
            avant_tokens=self.avant_tokens,
            thresholds=self.thresholds,
        )
        self.session.execute(
            delete(HolderProductSegmentDaily).where(
                HolderProductSegmentDaily.business_date == business_date
            )
        )
        self.session.execute(
            delete(HolderWalletProductDaily).where(
                HolderWalletProductDaily.business_date == business_date
            )
        )
        self.session.execute(
            delete(HolderProtocolDeployDaily).where(
                HolderProtocolDeployDaily.business_date == business_date
            )
        )
        if not context.wallets:
            return HolderDashboardBuildSummary(
                business_date=business_date,
                segment_rows_written=0,
                protocol_rows_written=0,
            )

        wallet_product_metrics = _build_wallet_product_metrics(
            context=context,
            thresholds=self.thresholds,
        )
        if wallet_product_metrics:
            self.session.execute(
                insert(HolderWalletProductDaily).values(
                    [
                        {
                            "business_date": business_date,
                            "as_of_ts_utc": context.as_of_ts_utc,
                            "wallet_id": row.wallet_id,
                            "wallet_address": row.wallet_address,
                            "product_scope": row.product_scope,
                            "monitored_presence_usd": row.monitored_presence_usd,
                            "observed_exposure_usd": row.observed_exposure_usd,
                            "wallet_held_usd": row.wallet_held_usd,
                            "canonical_deployed_usd": row.canonical_deployed_usd,
                            "external_fixed_yield_pt_usd": row.external_fixed_yield_pt_usd,
                            "external_yield_token_yt_usd": row.external_yield_token_yt_usd,
                            "external_other_defi_usd": row.external_other_defi_usd,
                            "has_any_defi_activity": row.has_any_defi_activity,
                            "has_any_defi_borrow": row.has_any_defi_borrow,
                            "has_canonical_activity": row.has_canonical_activity,
                            "segment": row.segment,
                            "is_attributed": row.is_attributed,
                            "asset_symbols_json": list(row.asset_symbols) or None,
                            "borrowed_usd": row.borrowed_usd,
                            "leverage_ratio": row.leverage_ratio,
                            "health_factor_min": row.health_factor_min,
                            "risk_band": row.risk_band,
                            "age_days": row.age_days,
                            "multi_asset_flag": row.multi_asset_flag,
                            "aum_delta_7d_usd": row.aum_delta_7d_usd,
                            "aum_delta_7d_pct": row.aum_delta_7d_pct,
                        }
                        for row in wallet_product_metrics
                    ]
                )
            )

        attributed_rows_by_scope: dict[str, list[WalletProductMetrics]] = defaultdict(list)
        attributed_rows_by_scope_segment: dict[tuple[str, str], list[WalletProductMetrics]] = (
            defaultdict(list)
        )
        for row in wallet_product_metrics:
            if not row.is_attributed or row.segment is None:
                continue
            attributed_rows_by_scope[row.product_scope].append(row)
            attributed_rows_by_scope_segment[(row.product_scope, row.segment)].append(row)

        prior_wallet_rows = self.session.scalars(
            select(HolderWalletProductDaily).where(
                HolderWalletProductDaily.business_date == business_date - timedelta(days=7)
            )
        ).all()
        prior_segmented_wallets: dict[tuple[str, str], set[int]] = defaultdict(set)
        for row in prior_wallet_rows:
            if not row.is_attributed or row.segment is None:
                continue
            prior_segmented_wallets[(row.product_scope, "all")].add(row.wallet_id)
            prior_segmented_wallets[(row.product_scope, str(row.segment))].add(row.wallet_id)

        segment_rows: list[dict[str, object]] = []
        for product_scope in PRODUCT_SCOPES:
            for cohort_segment in COHORT_SEGMENTS:
                rows = (
                    attributed_rows_by_scope.get(product_scope, [])
                    if cohort_segment == "all"
                    else attributed_rows_by_scope_segment.get((product_scope, cohort_segment), [])
                )
                holder_count = len(rows)
                current_wallet_ids = {row.wallet_id for row in rows}
                total_aum = sum((row.observed_exposure_usd for row in rows), ZERO)
                idle_usd = sum((row.wallet_held_usd for row in rows), ZERO)
                fixed_yield_pt_usd = sum((row.external_fixed_yield_pt_usd for row in rows), ZERO)
                yield_token_yt_usd = sum((row.external_yield_token_yt_usd for row in rows), ZERO)
                collateralized_usd = sum((row.canonical_deployed_usd for row in rows), ZERO)
                borrowed_usd = sum((row.borrowed_usd for row in rows), ZERO)
                staked_usd = sum((row.staked_usd for row in rows), ZERO)
                other_defi_usd = sum((row.external_other_defi_usd for row in rows), ZERO)
                defi_active_count = sum(1 for row in rows if row.has_any_defi_activity)
                avasset_deployed_count = sum(
                    1
                    for row in rows
                    if (
                        row.canonical_deployed_usd
                        + row.external_fixed_yield_pt_usd
                        + row.external_yield_token_yt_usd
                        + row.external_other_defi_usd
                    )
                    >= self.thresholds.classification_dust_floor_usd
                )
                conviction_gap_count = sum(
                    1
                    for row in rows
                    if row.has_any_defi_activity
                    and (
                        row.canonical_deployed_usd
                        + row.external_fixed_yield_pt_usd
                        + row.external_yield_token_yt_usd
                        + row.external_other_defi_usd
                    )
                    < self.thresholds.classification_dust_floor_usd
                )
                collateralized_count = sum(
                    1
                    for row in rows
                    if row.canonical_deployed_usd >= self.thresholds.classification_dust_floor_usd
                )
                borrowed_against_count = sum(
                    1
                    for row in rows
                    if row.borrowed_usd > self.thresholds.leveraged_borrow_usd_floor
                )
                multi_asset_count = sum(1 for row in rows if row.multi_asset_flag)
                age_values = [row.age_days for row in rows if row.age_days is not None]
                prior_total = sum(
                    (row.observed_exposure_usd - row.aum_delta_7d_usd for row in rows),
                    ZERO,
                )
                prior_wallet_ids = prior_segmented_wallets[(product_scope, cohort_segment)]
                segment_rows.append(
                    {
                        "business_date": business_date,
                        "as_of_ts_utc": context.as_of_ts_utc,
                        "product_scope": product_scope,
                        "cohort_segment": cohort_segment,
                        "holder_count": holder_count,
                        "defi_active_wallet_count": defi_active_count,
                        "avasset_deployed_wallet_count": avasset_deployed_count,
                        "conviction_gap_wallet_count": conviction_gap_count,
                        "collateralized_wallet_count": collateralized_count,
                        "borrowed_against_wallet_count": borrowed_against_count,
                        "multi_asset_wallet_count": multi_asset_count,
                        "observed_aum_usd": total_aum,
                        "avg_holding_usd": _share(total_aum, Decimal(holder_count))
                        if holder_count > 0
                        else None,
                        "median_age_days": int(median(age_values)) if age_values else None,
                        "idle_pct": _share(idle_usd, total_aum),
                        "fixed_yield_pt_pct": _share(fixed_yield_pt_usd, total_aum),
                        "collateralized_pct": _share(collateralized_usd, total_aum),
                        "borrowed_against_pct": _pct(borrowed_against_count, holder_count),
                        "staked_pct": _share(staked_usd, total_aum),
                        "defi_active_pct": _pct(defi_active_count, holder_count),
                        "avasset_deployed_pct": _pct(avasset_deployed_count, holder_count),
                        "conviction_gap_pct": _pct(conviction_gap_count, holder_count),
                        "multi_asset_pct": _pct(multi_asset_count, holder_count),
                        "aum_change_7d_pct": (
                            _share(total_aum - prior_total, prior_total)
                            if prior_total > ZERO
                            else None
                        ),
                        "new_wallet_count_7d": len(current_wallet_ids - prior_wallet_ids),
                        "exited_wallet_count_7d": len(prior_wallet_ids - current_wallet_ids),
                        "idle_usd": idle_usd,
                        "fixed_yield_pt_usd": fixed_yield_pt_usd,
                        "yield_token_yt_usd": yield_token_yt_usd,
                        "collateralized_usd": collateralized_usd,
                        "borrowed_usd": borrowed_usd,
                        "staked_usd": staked_usd,
                        "other_defi_usd": other_defi_usd,
                    }
                )
        if segment_rows:
            self.session.execute(insert(HolderProductSegmentDaily).values(segment_rows))

        current_holder_rows = self.session.scalars(
            select(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
        ).all()
        deploy_debank_date = _resolve_debank_date(self.session, business_date)
        current_token_rows = (
            self.session.scalars(
                select(ConsumerDebankTokenDaily).where(
                    ConsumerDebankTokenDaily.business_date == deploy_debank_date
                )
            ).all()
            if deploy_debank_date is not None
            else []
        )
        helper = HolderBehaviorEngine(
            self.session,
            avant_tokens=self.avant_tokens,
            thresholds=self.thresholds,
        )
        wallet_ids = sorted({row.wallet_id for row in current_holder_rows})
        consumer_rows = helper._load_consumer_position_rows(
            wallet_ids=wallet_ids,
            as_of_ts_utc=context.as_of_ts_utc,
            business_date=business_date,
        )
        wallet_product_by_key = {
            (row.wallet_id, row.product_scope): row
            for row in wallet_product_metrics
            if row.is_attributed and row.segment is not None
        }
        protocol_buckets: dict[tuple[str, str, str], dict[str, object]] = {}

        def _bucket(product_scope: str, protocol_code: str, chain_code: str) -> dict[str, object]:
            return protocol_buckets.setdefault(
                (product_scope, protocol_code, chain_code),
                {
                    "verified_ids": set(),
                    "core_ids": set(),
                    "whale_ids": set(),
                    "total_value_usd": ZERO,
                    "total_borrow_usd": ZERO,
                    "token_values": defaultdict(lambda: ZERO),
                    "use_values": defaultdict(lambda: ZERO),
                },
            )

        for row in consumer_rows:
            if row.collateral_family not in {"usd", "eth", "btc"}:
                continue
            family_scope = {
                "usd": "avusd",
                "eth": "aveth",
                "btc": "avbtc",
            }[row.collateral_family]
            normalized_protocol_code, _mapped_primary_use = self._protocol_mapping(
                row.protocol_code
            )
            for product_scope in ("all", family_scope):
                wallet_product = wallet_product_by_key.get((row.wallet_id, product_scope))
                if wallet_product is None or wallet_product.segment is None:
                    continue
                bucket = _bucket(product_scope, normalized_protocol_code, row.chain_code)
                bucket["total_value_usd"] = (
                    Decimal(str(bucket["total_value_usd"])) + row.collateral_usd
                )
                bucket["total_borrow_usd"] = (
                    Decimal(str(bucket["total_borrow_usd"])) + row.borrowed_usd
                )
                token_values = bucket["token_values"]
                assert isinstance(token_values, defaultdict)
                token_values[row.collateral_symbol] += row.collateral_usd
                use_values = bucket["use_values"]
                assert isinstance(use_values, defaultdict)
                use_values["collateral"] += row.collateral_usd
                if wallet_product.segment == "verified":
                    bucket["verified_ids"].add(row.wallet_id)
                elif wallet_product.segment == "core":
                    bucket["core_ids"].add(row.wallet_id)
                else:
                    bucket["whale_ids"].add(row.wallet_id)

        for row in current_token_rows:
            if row.leg_type == "borrow":
                continue
            if row.in_config_surface or is_excluded_visibility_protocol(row.protocol_code):
                continue
            family_code = family_for_token_symbol(row.token_symbol)
            if family_code is None:
                continue
            family_scope = {"usd": "avusd", "eth": "aveth", "btc": "avbtc"}[family_code]
            normalized_protocol_code, mapped_primary_use = self._protocol_mapping(row.protocol_code)
            use_category = _protocol_use_category(
                protocol_code=row.protocol_code,
                token_symbol=row.token_symbol,
                in_config_surface=bool(row.in_config_surface),
                mapped_primary_use=mapped_primary_use,
            )
            for product_scope in ("all", family_scope):
                wallet_product = wallet_product_by_key.get((row.wallet_id, product_scope))
                if wallet_product is None or wallet_product.segment is None:
                    continue
                bucket = _bucket(product_scope, normalized_protocol_code, row.chain_code)
                bucket["total_value_usd"] = Decimal(str(bucket["total_value_usd"])) + row.usd_value
                token_values = bucket["token_values"]
                assert isinstance(token_values, defaultdict)
                token_values[row.token_symbol] += row.usd_value
                use_values = bucket["use_values"]
                assert isinstance(use_values, defaultdict)
                use_values[use_category] += row.usd_value
                if wallet_product.segment == "verified":
                    bucket["verified_ids"].add(row.wallet_id)
                elif wallet_product.segment == "core":
                    bucket["core_ids"].add(row.wallet_id)
                else:
                    bucket["whale_ids"].add(row.wallet_id)

        protocol_rows = []
        for (product_scope, protocol_code, chain_code), bucket in sorted(
            protocol_buckets.items(),
            key=lambda item: (
                item[0][0],
                -len(item[1]["whale_ids"]),
                -len(item[1]["core_ids"]),
                -Decimal(str(item[1]["total_value_usd"])),
                item[0][1],
                item[0][2],
            ),
        ):
            token_values = bucket["token_values"]
            use_values = bucket["use_values"]
            assert isinstance(token_values, defaultdict)
            assert isinstance(use_values, defaultdict)
            dominant_symbols = [
                symbol
                for symbol, _usd in sorted(
                    token_values.items(),
                    key=lambda item: (-item[1], item[0]),
                )[:3]
            ]
            primary_use_category = "other_defi"
            if use_values:
                primary_use_category = max(
                    use_values.items(),
                    key=lambda item: (item[1], item[0]),
                )[0]
            protocol_rows.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": context.as_of_ts_utc,
                    "product_scope": product_scope,
                    "protocol_code": protocol_code,
                    "chain_code": chain_code,
                    "verified_wallet_count": len(bucket["verified_ids"]),
                    "core_wallet_count": len(bucket["core_ids"]),
                    "whale_wallet_count": len(bucket["whale_ids"]),
                    "total_value_usd": Decimal(str(bucket["total_value_usd"])),
                    "total_borrow_usd": Decimal(str(bucket["total_borrow_usd"])),
                    "dominant_token_symbols_json": dominant_symbols or None,
                    "primary_use": _primary_use_label(primary_use_category),
                }
            )
        if protocol_rows:
            self.session.execute(insert(HolderProtocolDeployDaily).values(protocol_rows))

        return HolderDashboardBuildSummary(
            business_date=business_date,
            segment_rows_written=len(segment_rows),
            protocol_rows_written=len(protocol_rows),
        )
