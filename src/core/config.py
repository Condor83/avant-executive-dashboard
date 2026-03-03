"""Config contracts and parsing for strategy and consumer surfaces."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


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
    incentives_controller: str | None = None
    oracle: str | None = None


class MorphoMarket(ConfigModel):
    id: str
    loan_token: str
    collateral_token: str
    loan_decimals: int = Field(ge=0, le=36)
    collateral_decimals: int | None = Field(default=None, ge=0, le=36)
    defillama_pool_id: str | None = None


class MorphoVault(ConfigModel):
    address: str
    note: str | None = None


class MorphoChainConfig(ConfigModel):
    morpho: str
    wallets: list[str]
    markets: list[MorphoMarket]
    vaults: list[MorphoVault] = Field(default_factory=list)


class EulerVault(ConfigModel):
    address: str
    symbol: str


class EulerChainConfig(ConfigModel):
    wallets: list[str]
    vaults: list[EulerVault]


class DolomiteMarket(ConfigModel):
    id: int
    symbol: str
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
    market_pubkey: str
    name: str
    defillama_pool_id: str | None = None


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


class MarketsConfig(ConfigModel):
    """Canonical strategy scope config."""

    aave_v3: dict[str, AaveChainConfig]
    morpho: dict[str, MorphoChainConfig]
    euler_v2: dict[str, EulerChainConfig]
    dolomite: dict[str, DolomiteChainConfig]
    kamino: dict[str, KaminoChainConfig]
    zest: dict[str, ZestChainConfig]
    wallet_balances: dict[str, WalletBalanceChainConfig]


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
