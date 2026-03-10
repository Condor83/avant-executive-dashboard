"""Config contracts and parsing for strategy and consumer surfaces."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONSUMER_THRESHOLDS_CONFIG_PATH = REPO_ROOT / "config/consumer_thresholds.yaml"
DEFAULT_HOLDER_EXCLUSIONS_CONFIG_PATH = REPO_ROOT / "config/holder_exclusions.yaml"
DEFAULT_HOLDER_UNIVERSE_CONFIG_PATH = REPO_ROOT / "config/holder_universe.yaml"
DEFAULT_HOLDER_PROTOCOL_MAP_CONFIG_PATH = REPO_ROOT / "config/holder_protocol_map.yaml"


class ConfigModel(BaseModel):
    """Strict pydantic base model for YAML contracts."""

    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class AaveMarket(ConfigModel):
    symbol: str
    asset: str
    decimals: int = Field(ge=0, le=36)
    supply_apy_fallback_pool_id: str | None = None


class AaveChainConfig(ConfigModel):
    pool: str
    pool_data_provider: str
    wallets: list[str]
    markets: list[AaveMarket]
    rate_reference_markets: list[AaveMarket] = Field(default_factory=list)
    incentives_controller: str | None = None
    oracle: str | None = None


class SparkMarket(ConfigModel):
    symbol: str
    asset: str
    decimals: int = Field(ge=0, le=36)
    supply_apy_fallback_pool_id: str | None = None


class SparkChainConfig(ConfigModel):
    pool: str
    pool_data_provider: str
    wallets: list[str]
    markets: list[SparkMarket]
    incentives_controller: str | None = None
    oracle: str | None = None


class MorphoMarket(ConfigModel):
    id: str
    loan_token: str
    collateral_token: str
    loan_decimals: int = Field(ge=0, le=36)
    loan_token_address: str | None = None
    collateral_decimals: int | None = Field(default=None, ge=0, le=36)
    collateral_token_address: str | None = None
    defillama_pool_id: str | None = None


class MorphoVault(ConfigModel):
    address: str
    note: str | None = None
    asset_address: str | None = None
    asset_symbol: str | None = None
    asset_decimals: int | None = Field(default=None, ge=0, le=36)
    chain_id: int | None = Field(default=None, ge=1)
    apy_source: Literal["morpho_api"] = "morpho_api"
    apy_lookback: str = Field(default="SIX_HOURS", min_length=1)


class MorphoChainConfig(ConfigModel):
    morpho: str
    wallets: list[str]
    markets: list[MorphoMarket]
    vaults: list[MorphoVault] = Field(default_factory=list)


class EulerVault(ConfigModel):
    address: str
    symbol: str
    asset_address: str
    asset_symbol: str
    asset_decimals: int = Field(ge=0, le=36)
    debt_supported: bool = True


class EulerChainConfig(ConfigModel):
    wallets: list[str]
    vaults: list[EulerVault]
    account_ids: list[int] = Field(default_factory=lambda: [0])

    @model_validator(mode="after")
    def validate_account_ids(self) -> EulerChainConfig:
        if not self.account_ids:
            raise ValueError("euler_v2 account_ids must not be empty")
        if any(account_id < 0 for account_id in self.account_ids):
            raise ValueError("euler_v2 account_ids must be non-negative")
        return self


class DolomiteMarket(ConfigModel):
    id: int
    symbol: str
    token_address: str | None = None
    defillama_pool_id: str | None = None
    decimals: int = Field(ge=0, le=36)


class DolomiteChainConfig(ConfigModel):
    margin: str
    wallets: list[str]
    markets: list[DolomiteMarket]
    account_numbers: list[int] = Field(default_factory=lambda: [0])

    @model_validator(mode="after")
    def validate_account_numbers(self) -> DolomiteChainConfig:
        if not self.account_numbers:
            raise ValueError("dolomite account_numbers must not be empty")
        if any(account_number < 0 for account_number in self.account_numbers):
            raise ValueError("dolomite account_numbers must be non-negative")
        return self


class KaminoMarket(ConfigModel):
    class TokenRef(ConfigModel):
        symbol: str
        mint: str
        decimals: int = Field(ge=0, le=36)

    market_pubkey: str
    name: str
    defillama_pool_id: str | None = None
    supply_token: TokenRef | None = None
    borrow_token: TokenRef | None = None


class KaminoChainConfig(ConfigModel):
    wallets: list[str] = Field(default_factory=list)
    markets: list[KaminoMarket]


class ZestMarket(ConfigModel):
    symbol: str
    asset_contract: str
    z_token: str
    borrow_fn: str
    decimals: int = Field(ge=0, le=36)


class ZestChainConfig(ConfigModel):
    wallets: list[str]
    pool_deployer: str
    pool_read: str
    markets: list[ZestMarket]


class WalletBalanceToken(ConfigModel):
    symbol: str
    address: str
    decimals: int = Field(ge=0, le=36)


class WalletBalanceChainConfig(ConfigModel):
    wallets: list[str] = Field(default_factory=list)
    tokens: list[WalletBalanceToken]


class TraderJoePool(ConfigModel):
    pool_address: str
    pool_type: Literal["joe_v2_lb", "joe_v2_pair"] = "joe_v2_lb"
    token_x_address: str
    token_x_symbol: str
    token_x_decimals: int = Field(ge=0, le=36)
    token_y_address: str
    token_y_symbol: str
    token_y_decimals: int = Field(ge=0, le=36)
    bin_ids: list[int] = Field(default_factory=list)
    include_in_yield: bool = False
    capital_bucket: str = "market_stability_ops"

    @model_validator(mode="after")
    def validate_bin_ids(self) -> TraderJoePool:
        if self.pool_type == "joe_v2_lb" and not self.bin_ids:
            raise ValueError("traderjoe_lp joe_v2_lb pools require at least one bin_id")
        if any(bin_id < 0 for bin_id in self.bin_ids):
            raise ValueError("traderjoe_lp bin_ids must be non-negative")
        return self


class TraderJoeChainConfig(ConfigModel):
    wallets: list[str]
    pools: list[TraderJoePool]


class StakedaoUnderlyingToken(ConfigModel):
    symbol: str
    address: str
    decimals: int = Field(ge=0, le=36)
    pool_index: int = Field(ge=0)


class StakedaoVault(ConfigModel):
    vault_address: str
    asset_address: str
    asset_decimals: int = Field(ge=0, le=36)
    underlyings: list[StakedaoUnderlyingToken]
    apy_source: Literal["fixed_apy_override"] | None = None
    fixed_apy: Decimal | None = Field(default=None, ge=0, le=10)
    review_after: date | None = None
    include_in_yield: bool = False
    capital_bucket: str = "pending_deployment"

    @model_validator(mode="after")
    def validate_fixed_apy_override(self) -> StakedaoVault:
        if self.apy_source == "fixed_apy_override" and self.fixed_apy is None:
            raise ValueError("stakedao fixed_apy_override requires fixed_apy")
        if self.fixed_apy is not None and self.apy_source != "fixed_apy_override":
            raise ValueError("stakedao fixed_apy requires apy_source=fixed_apy_override")
        if self.review_after is not None and self.fixed_apy is None:
            raise ValueError("stakedao review_after requires fixed_apy")
        return self


class StakedaoChainConfig(ConfigModel):
    wallets: list[str]
    vaults: list[StakedaoVault]


class EtherexPool(ConfigModel):
    pool_address: str
    position_manager_address: str
    token0_address: str
    token0_symbol: str
    token0_decimals: int = Field(ge=0, le=36)
    token1_address: str
    token1_symbol: str
    token1_decimals: int = Field(ge=0, le=36)
    fee: int = Field(ge=0)
    include_in_yield: bool = False
    capital_bucket: str = "market_stability_ops"


class EtherexChainConfig(ConfigModel):
    wallets: list[str]
    pools: list[EtherexPool]


class MarketsConfig(ConfigModel):
    """Canonical strategy scope config."""

    aave_v3: dict[str, AaveChainConfig]
    spark: dict[str, SparkChainConfig] = Field(default_factory=dict)
    morpho: dict[str, MorphoChainConfig]
    euler_v2: dict[str, EulerChainConfig]
    dolomite: dict[str, DolomiteChainConfig]
    kamino: dict[str, KaminoChainConfig]
    zest: dict[str, ZestChainConfig]
    wallet_balances: dict[str, WalletBalanceChainConfig]
    traderjoe_lp: dict[str, TraderJoeChainConfig] = Field(default_factory=dict)
    stakedao: dict[str, StakedaoChainConfig] = Field(default_factory=dict)
    etherex: dict[str, EtherexChainConfig] = Field(default_factory=dict)


class WalletProductAssignment(ConfigModel):
    """Canonical wallet -> product/tranche mapping."""

    wallet_address: str
    product_code: str
    product_family: Literal["stablecoin", "btc", "eth"]
    tranche: Literal["senior", "junior"]
    wallet_type: Literal["strategy", "customer", "internal"] = "strategy"


ProductFamily = Literal["stablecoin", "btc", "eth"]
Tranche = Literal["senior", "junior"]


class WalletProductsConfig(ConfigModel):
    """Normalized wallet products contract."""

    assignments: list[WalletProductAssignment]
    staked_token_contracts: dict[str, dict[str, str]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def ensure_unique_wallet_assignments(self) -> WalletProductsConfig:
        seen: dict[str, str] = {}
        for assignment in self.assignments:
            wallet_key = canonical_address(assignment.wallet_address)
            current = seen.get(wallet_key)
            if current and current != assignment.product_code:
                raise ValueError(
                    f"wallet {assignment.wallet_address} appears in multiple products: "
                    f"{current} and {assignment.product_code}"
                )
            seen[wallet_key] = assignment.product_code
        return self


class LegacyWalletProductsConfig(ConfigModel):
    """Legacy wallet-products contract currently used in this repo."""

    STRATEGY_WALLETS: dict[str, list[str]]
    STAKED_TOKEN_CONTRACTS: dict[str, dict[str, str]] = Field(default_factory=dict)


class ConsumerToken(ConfigModel):
    symbol: str
    address: str
    decimals: int = Field(ge=0, le=36)


class ConsumerMarket(ConfigModel):
    protocol: str
    chain: str
    name: str
    market_address: str
    collateral_token: ConsumerToken
    borrow_token: ConsumerToken


class ConsumerMarketsConfig(ConfigModel):
    markets: list[ConsumerMarket] = Field(default_factory=list)


class AvantToken(ConfigModel):
    chain_code: str
    token_address: str
    symbol: str
    asset_family: Literal["usd", "btc", "eth"]
    wrapper_class: Literal["base", "staked", "boosted"]
    decimals: int = Field(ge=0, le=36)
    pricing_policy: Literal["direct_price", "convert_to_assets", "direct_or_convert_to_assets"]
    underlying_token_address: str | None = None
    exchange_rate_contract: str | None = None
    nav_contract: str | None = None
    active_from: date | None = None
    active_to: date | None = None


class AvantTokensConfig(ConfigModel):
    tokens: list[AvantToken] = Field(default_factory=list)


class ConsumerRiskBandThresholds(ConfigModel):
    watch_health_factor_lt: Decimal = Field(gt=0)
    elevated_health_factor_lt: Decimal = Field(gt=0)
    critical_health_factor_lt: Decimal = Field(gt=0)
    elevated_leverage_ratio_gte: Decimal = Field(ge=0)
    watch_leverage_ratio_gte: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_ordering(self) -> ConsumerRiskBandThresholds:
        if not (
            self.critical_health_factor_lt
            <= self.elevated_health_factor_lt
            <= self.watch_health_factor_lt
        ):
            raise ValueError(
                "consumer risk-band health-factor thresholds must satisfy "
                "critical <= elevated <= watch"
            )
        if self.elevated_leverage_ratio_gte < self.watch_leverage_ratio_gte:
            raise ValueError(
                "consumer risk-band leverage thresholds must satisfy elevated >= watch"
            )
        return self


class ConsumerCapacityThresholds(ConfigModel):
    utilization_threshold: Decimal = Field(ge=0)
    top10_collateral_concentration_threshold: Decimal = Field(ge=0)
    collateral_growth_7d_threshold: Decimal = Field(ge=0)
    collateral_wallet_growth_7d_threshold: Decimal = Field(ge=0)
    near_limit_health_factor_threshold: Decimal = Field(gt=0)
    review_score_threshold: int = Field(ge=1)


class ConsumerWhaleThresholds(ConfigModel):
    top_assets_count: int = Field(ge=1, default=25)
    top_borrow_count: int = Field(ge=1, default=25)
    leaderboard_count: int = Field(ge=1, default=10)
    wallet_usd_threshold: Decimal = Field(ge=0, default=Decimal("1000000"))
    material_change_7d_usd: Decimal = Field(ge=0, default=Decimal("100000"))


class ConsumerSupplyCoverageConfig(ConfigModel):
    primary_chain_code: str = Field(min_length=1, default="avalanche")
    primary_token_symbol: str = Field(min_length=1, default="savUSD")


class ConsumerThresholdsConfig(ConfigModel):
    verified_min_total_avant_usd: Decimal = Field(ge=0, default=Decimal("100"))
    cohort_min_total_avant_usd: Decimal = Field(ge=0)
    classification_dust_floor_usd: Decimal = Field(ge=0)
    leveraged_borrow_usd_floor: Decimal = Field(ge=0, default=Decimal("0.00000001"))
    capacity: ConsumerCapacityThresholds
    risk_bands: ConsumerRiskBandThresholds
    whales: ConsumerWhaleThresholds = Field(default_factory=ConsumerWhaleThresholds)
    supply_coverage: ConsumerSupplyCoverageConfig = Field(
        default_factory=ConsumerSupplyCoverageConfig
    )


class HolderExclusion(ConfigModel):
    address: str = Field(min_length=1)
    chain_code: str | None = None
    label: str = Field(min_length=1)
    classification: Literal[
        "strategy",
        "internal",
        "protocol",
        "infrastructure",
        "ops",
        "treasury",
    ]
    exclude_from_monitoring: bool = True
    exclude_from_customer_float: bool = True
    notes: str | None = None

    @model_validator(mode="after")
    def normalize_values(self) -> HolderExclusion:
        self.address = canonical_address(self.address)
        if self.chain_code is not None:
            self.chain_code = self.chain_code.strip().lower()
        return self


class HolderExclusionsConfig(ConfigModel):
    exclusions: list[HolderExclusion] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_entries(self) -> HolderExclusionsConfig:
        seen: set[tuple[str | None, str]] = set()
        for exclusion in self.exclusions:
            key = (exclusion.chain_code, exclusion.address)
            if key in seen:
                raise ValueError(
                    "duplicate holder exclusion entry for "
                    f"chain={exclusion.chain_code or 'all'} address={exclusion.address}"
                )
            seen.add(key)
        return self


class HolderUniverseChain(ConfigModel):
    chain_code: str = Field(min_length=1)
    chain_id: str = Field(min_length=1)
    holder_provider: Literal["routescan"] = "routescan"

    @model_validator(mode="after")
    def normalize_values(self) -> HolderUniverseChain:
        self.chain_code = self.chain_code.strip().lower()
        self.chain_id = self.chain_id.strip()
        return self


class HolderUniverseConfig(ConfigModel):
    chains: list[HolderUniverseChain] = Field(default_factory=list)
    raw_holder_threshold_usd: Decimal = Field(ge=0, default=Decimal("100"))
    core_holder_threshold_usd: Decimal = Field(ge=0, default=Decimal("50000"))
    whale_holder_threshold_usd: Decimal = Field(ge=0, default=Decimal("1000000"))
    legacy_seed_globs: list[str] = Field(default_factory=lambda: ["consumer_wallets_*.yaml"])
    force_include_wallets: list[str] = Field(default_factory=list)
    exclude_protocol_wallets_by_default: bool = True
    exclude_contract_wallets_by_default: bool = True

    @model_validator(mode="after")
    def validate_thresholds(self) -> HolderUniverseConfig:
        if self.core_holder_threshold_usd < self.raw_holder_threshold_usd:
            raise ValueError("core_holder_threshold_usd must be >= raw_holder_threshold_usd")
        if self.whale_holder_threshold_usd < self.core_holder_threshold_usd:
            raise ValueError("whale_holder_threshold_usd must be >= core_holder_threshold_usd")
        self.force_include_wallets = [
            canonical_address(address) for address in self.force_include_wallets
        ]
        return self

    def chain_id_map(self) -> dict[str, str]:
        return {chain.chain_code: chain.chain_id for chain in self.chains}


PrimaryUseClass = Literal[
    "fixed_yield",
    "yield_token",
    "collateral",
    "restaking",
    "vault",
    "lp",
    "other",
]
ProtocolSurface = Literal["canonical_supported", "visibility_only"]


class HolderProtocolMapDefault(ConfigModel):
    surface: ProtocolSurface = "visibility_only"
    primary_use: PrimaryUseClass = "other"


class HolderProtocolMapEntry(ConfigModel):
    protocol_code: str = Field(min_length=1)
    canonical_protocol_code: str | None = None
    surface: ProtocolSurface = "visibility_only"
    primary_use: PrimaryUseClass = "other"

    @model_validator(mode="after")
    def normalize_values(self) -> HolderProtocolMapEntry:
        self.protocol_code = self.protocol_code.strip().lower()
        if self.canonical_protocol_code is not None:
            self.canonical_protocol_code = self.canonical_protocol_code.strip().lower()
        return self


class HolderProtocolMapConfig(ConfigModel):
    defaults: HolderProtocolMapDefault = Field(default_factory=HolderProtocolMapDefault)
    protocols: list[HolderProtocolMapEntry] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_protocols(self) -> HolderProtocolMapConfig:
        seen: set[str] = set()
        for protocol in self.protocols:
            if protocol.protocol_code in seen:
                raise ValueError(
                    f"duplicate holder protocol mapping for '{protocol.protocol_code}'"
                )
            seen.add(protocol.protocol_code)
        return self

    def by_protocol_code(self) -> dict[str, HolderProtocolMapEntry]:
        return {entry.protocol_code: entry for entry in self.protocols}


class IncreasingRiskThresholds(ConfigModel):
    """Thresholds where larger values represent higher risk."""

    low: Decimal = Field(ge=0)
    med: Decimal = Field(ge=0)
    high: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_ordering(self) -> IncreasingRiskThresholds:
        if not (self.low <= self.med <= self.high):
            raise ValueError("increasing thresholds must satisfy low <= med <= high")
        return self


class DecreasingRiskThresholds(ConfigModel):
    """Thresholds where smaller values represent higher risk."""

    low: Decimal = Field(ge=0)
    med: Decimal = Field(ge=0)
    high: Decimal = Field(ge=0)

    @model_validator(mode="after")
    def validate_ordering(self) -> DecreasingRiskThresholds:
        if not (self.high <= self.med <= self.low):
            raise ValueError("decreasing thresholds must satisfy high <= med <= low")
        return self


class KinkThresholdConfig(ConfigModel):
    utilization: IncreasingRiskThresholds
    default_target_utilization: Decimal = Field(gt=0, le=2)
    protocol_target_overrides: dict[str, Decimal] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_protocol_targets(self) -> KinkThresholdConfig:
        for protocol_code, value in self.protocol_target_overrides.items():
            if value <= 0:
                raise ValueError(f"kink protocol target override for '{protocol_code}' must be > 0")
        return self


class SpreadThresholdConfig(ConfigModel):
    net_spread_apy: DecreasingRiskThresholds


class BorrowSpikeThresholdConfig(ConfigModel):
    delta_apy: IncreasingRiskThresholds
    max_lookback_hours: int = Field(ge=1, le=720, default=48)


class LiquidityThresholdConfig(ConfigModel):
    available_ratio: DecreasingRiskThresholds


class RiskThresholdsConfig(ConfigModel):
    """Risk/alert threshold policy shared by compute risk and tests."""

    kink: KinkThresholdConfig
    spread: SpreadThresholdConfig
    borrow_spike: BorrowSpikeThresholdConfig
    liquidity: LiquidityThresholdConfig


class PTFixedYieldOverride(ConfigModel):
    """Manual fixed-yield override for a single PT-backed position."""

    position_key: str = Field(min_length=1)
    fixed_apy: Decimal = Field(gt=Decimal("0"))
    source: Literal["etherscan_manual", "pendle_manual"] = "etherscan_manual"
    tx_hash: str = Field(min_length=1)
    acquired_at_utc: str = Field(min_length=1)
    note: str | None = None


class PTFixedYieldOverridesConfig(ConfigModel):
    """Collection of PT fixed-yield overrides keyed by position."""

    overrides: list[PTFixedYieldOverride] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_unique_position_keys(self) -> PTFixedYieldOverridesConfig:
        seen: set[str] = set()
        for override in self.overrides:
            if override.position_key in seen:
                raise ValueError(f"duplicate PT fixed-yield override for '{override.position_key}'")
            seen.add(override.position_key)
        return self


LEGACY_PRODUCT_MAPPING: dict[str, tuple[str, ProductFamily, Tranche]] = {
    "savUSD": ("stablecoin_senior", "stablecoin", "senior"),
    "avUSD": ("stablecoin_senior", "stablecoin", "senior"),
    "savBTC": ("btc_senior", "btc", "senior"),
    "avBTC": ("btc_senior", "btc", "senior"),
    "savETH": ("eth_senior", "eth", "senior"),
    "avETH": ("eth_senior", "eth", "senior"),
    "avUSDx": ("stablecoin_junior", "stablecoin", "junior"),
    "avBTCx": ("btc_junior", "btc", "junior"),
    "avETHx": ("eth_junior", "eth", "junior"),
}


def canonical_address(value: str) -> str:
    """Normalize EVM-style addresses for deterministic deduplication."""

    cleaned = value.strip()
    if cleaned.startswith("0x"):
        return cleaned.lower()
    return cleaned


def _read_yaml(path: Path | str) -> dict[str, Any]:
    path_obj = Path(path)
    with path_obj.open("r", encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(f"{path_obj} must be a YAML mapping at the top level")
    return data


def load_markets_config(path: Path | str) -> MarketsConfig:
    """Load and validate `markets.yaml`."""

    raw = _read_yaml(path)
    try:
        return MarketsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid markets config at {path}: {exc}") from exc


def load_wallet_products_config(path: Path | str) -> WalletProductsConfig:
    """Load wallet-products config in canonical or legacy shape."""

    raw = _read_yaml(path)
    if "assignments" in raw:
        try:
            return WalletProductsConfig.model_validate(raw)
        except ValidationError as exc:
            raise ValueError(f"invalid wallet_products config at {path}: {exc}") from exc

    try:
        legacy = LegacyWalletProductsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid legacy wallet_products config at {path}: {exc}") from exc

    assignments: list[WalletProductAssignment] = []
    for token_name, wallets in legacy.STRATEGY_WALLETS.items():
        mapping = LEGACY_PRODUCT_MAPPING.get(token_name)
        if mapping is None:
            raise ValueError(
                f"wallet_products token '{token_name}' is not recognized; "
                "add it to LEGACY_PRODUCT_MAPPING"
            )
        product_code, product_family, tranche = mapping
        for wallet in wallets:
            assignments.append(
                WalletProductAssignment(
                    wallet_address=canonical_address(wallet),
                    product_code=product_code,
                    product_family=product_family,
                    tranche=tranche,
                    wallet_type="strategy",
                )
            )

    return WalletProductsConfig(
        assignments=assignments,
        staked_token_contracts=legacy.STAKED_TOKEN_CONTRACTS,
    )


def load_consumer_markets_config(path: Path | str) -> ConsumerMarketsConfig:
    """Load and validate `consumer_markets.yaml`."""

    raw = _read_yaml(path)
    try:
        return ConsumerMarketsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid consumer_markets config at {path}: {exc}") from exc


def load_avant_tokens_config(path: Path | str) -> AvantTokensConfig:
    """Load and validate `avant_tokens.yaml`."""

    raw = _read_yaml(path)
    try:
        return AvantTokensConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid avant tokens config at {path}: {exc}") from exc


def load_consumer_thresholds_config(
    path: Path | str = DEFAULT_CONSUMER_THRESHOLDS_CONFIG_PATH,
) -> ConsumerThresholdsConfig:
    """Load and validate consumer threshold config."""

    raw = _read_yaml(path)
    try:
        return ConsumerThresholdsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid consumer thresholds config at {path}: {exc}") from exc


def load_holder_exclusions_config(
    path: Path | str = DEFAULT_HOLDER_EXCLUSIONS_CONFIG_PATH,
) -> HolderExclusionsConfig:
    """Load and validate holder exclusion config."""

    path_obj = Path(path)
    if not path_obj.exists():
        return HolderExclusionsConfig()

    raw = _read_yaml(path_obj)
    try:
        return HolderExclusionsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid holder exclusions config at {path}: {exc}") from exc


def load_holder_universe_config(
    path: Path | str = DEFAULT_HOLDER_UNIVERSE_CONFIG_PATH,
) -> HolderUniverseConfig:
    """Load and validate holder-universe config."""

    raw = _read_yaml(path)
    try:
        return HolderUniverseConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid holder universe config at {path}: {exc}") from exc


def load_holder_protocol_map_config(
    path: Path | str = DEFAULT_HOLDER_PROTOCOL_MAP_CONFIG_PATH,
) -> HolderProtocolMapConfig:
    """Load and validate holder protocol normalization config."""

    raw = _read_yaml(path)
    try:
        return HolderProtocolMapConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid holder protocol map config at {path}: {exc}") from exc


def load_risk_thresholds_config(path: Path | str) -> RiskThresholdsConfig:
    """Load and validate risk threshold config."""

    raw = _read_yaml(path)
    try:
        return RiskThresholdsConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid risk thresholds config at {path}: {exc}") from exc


def load_pt_fixed_yield_overrides_config(
    path: Path | str,
) -> dict[str, PTFixedYieldOverride]:
    """Load optional PT fixed-yield manual overrides."""

    path_obj = Path(path)
    if not path_obj.exists():
        return {}

    raw = _read_yaml(path_obj)
    try:
        parsed = PTFixedYieldOverridesConfig.model_validate(raw)
    except ValidationError as exc:
        raise ValueError(f"invalid PT fixed-yield overrides config at {path}: {exc}") from exc
    return {override.position_key: override for override in parsed.overrides}
