"""Canonical database models for dimensions, snapshot facts, and served dashboard views."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    Boolean,
    DateTime,
    Enum,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from core.db.base import Base


class Wallet(Base):
    """Tracked wallet dimension."""

    __tablename__ = "wallets"

    wallet_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    address: Mapped[str] = mapped_column(String(128), unique=True, nullable=False, index=True)
    wallet_type: Mapped[str] = mapped_column(
        Enum("strategy", "customer", "internal", name="wallet_type_enum", native_enum=False),
        nullable=False,
        default="strategy",
    )
    label: Mapped[str | None] = mapped_column(String(255), nullable=True)


class Product(Base):
    """Product/tranche dimension."""

    __tablename__ = "products"

    product_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    product_code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)


class WalletProductMap(Base):
    """One-to-one wallet to product mapping."""

    __tablename__ = "wallet_product_map"

    wallet_id: Mapped[int] = mapped_column(
        ForeignKey("wallets.wallet_id", ondelete="CASCADE"), primary_key=True
    )
    product_id: Mapped[int] = mapped_column(ForeignKey("products.product_id", ondelete="CASCADE"))

    wallet: Mapped[Wallet] = relationship()
    product: Mapped[Product] = relationship()


class Protocol(Base):
    """Protocol registry dimension."""

    __tablename__ = "protocols"

    protocol_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    protocol_code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)


class Chain(Base):
    """Chain registry dimension."""

    __tablename__ = "chains"

    chain_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_code: Mapped[str] = mapped_column(String(64), unique=True, nullable=False, index=True)


class Token(Base):
    """Token registry dimension."""

    __tablename__ = "tokens"
    __table_args__ = (
        UniqueConstraint("chain_id", "address_or_mint", name="uq_tokens_chain_address"),
    )

    token_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_id: Mapped[int] = mapped_column(
        ForeignKey("chains.chain_id", ondelete="CASCADE"), nullable=False
    )
    address_or_mint: Mapped[str] = mapped_column(String(255), nullable=False)
    symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    decimals: Mapped[int] = mapped_column(Integer, nullable=False)

    chain: Mapped[Chain] = relationship()


class Market(Base):
    """Protocol-native market registry dimension."""

    __tablename__ = "markets"
    __table_args__ = (
        UniqueConstraint(
            "chain_id", "protocol_id", "market_address", name="uq_markets_chain_proto_addr"
        ),
    )

    market_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    chain_id: Mapped[int] = mapped_column(
        ForeignKey("chains.chain_id", ondelete="CASCADE"), nullable=False
    )
    protocol_id: Mapped[int] = mapped_column(
        ForeignKey("protocols.protocol_id", ondelete="CASCADE"), nullable=False
    )
    native_market_key: Mapped[str] = mapped_column(
        String(255), nullable=False, index=True, default=""
    )
    market_address: Mapped[str] = mapped_column(String(255), nullable=False)
    market_kind: Mapped[str] = mapped_column(
        Enum(
            "reserve",
            "market",
            "vault",
            "wallet_balance_token",
            "liquidity_book_pool",
            "concentrated_liquidity_pool",
            "vault_underlying",
            "consumer_market",
            "other",
            name="market_kind_enum",
            native_enum=False,
        ),
        nullable=False,
        default="other",
    )
    display_name: Mapped[str] = mapped_column(String(255), nullable=False, default="")
    base_asset_token_id: Mapped[int | None] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="SET NULL"), nullable=True
    )
    collateral_token_id: Mapped[int | None] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="SET NULL"), nullable=True
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)

    chain: Mapped[Chain] = relationship()
    protocol: Mapped[Protocol] = relationship()
    base_asset_token: Mapped[Token | None] = relationship(foreign_keys=[base_asset_token_id])
    collateral_token: Mapped[Token | None] = relationship(foreign_keys=[collateral_token_id])


class MarketExposure(Base):
    """Business-facing market exposure lens used by dashboard and portfolio grouping."""

    __tablename__ = "market_exposures"
    __table_args__ = (
        UniqueConstraint(
            "protocol_id",
            "chain_id",
            "exposure_kind",
            "supply_token_id",
            "debt_token_id",
            "collateral_token_id",
            name="uq_market_exposures_identity",
        ),
        UniqueConstraint("exposure_slug", name="uq_market_exposures_slug"),
    )

    market_exposure_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    protocol_id: Mapped[int] = mapped_column(
        ForeignKey("protocols.protocol_id", ondelete="CASCADE"), nullable=False
    )
    chain_id: Mapped[int] = mapped_column(
        ForeignKey("chains.chain_id", ondelete="CASCADE"), nullable=False
    )
    exposure_kind: Mapped[str] = mapped_column(
        Enum(
            "reserve_pair",
            "native_market",
            "vault_exposure",
            name="market_exposure_kind_enum",
            native_enum=False,
        ),
        nullable=False,
    )
    supply_token_id: Mapped[int | None] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="SET NULL"), nullable=True
    )
    debt_token_id: Mapped[int | None] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="SET NULL"), nullable=True
    )
    collateral_token_id: Mapped[int | None] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="SET NULL"), nullable=True
    )
    exposure_slug: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)

    protocol: Mapped[Protocol] = relationship()
    chain: Mapped[Chain] = relationship()
    supply_token: Mapped[Token | None] = relationship(foreign_keys=[supply_token_id])
    debt_token: Mapped[Token | None] = relationship(foreign_keys=[debt_token_id])
    collateral_token: Mapped[Token | None] = relationship(foreign_keys=[collateral_token_id])


class MarketExposureComponent(Base):
    """Mapping from dashboard exposure rows to canonical native markets."""

    __tablename__ = "market_exposure_components"
    __table_args__ = (
        UniqueConstraint(
            "market_exposure_id",
            "market_id",
            "component_role",
            name="uq_market_exposure_components_identity",
        ),
    )

    market_exposure_component_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    market_exposure_id: Mapped[int] = mapped_column(
        ForeignKey("market_exposures.market_exposure_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    market_id: Mapped[int] = mapped_column(
        ForeignKey("markets.market_id", ondelete="CASCADE"), nullable=False, index=True
    )
    component_role: Mapped[str] = mapped_column(
        Enum(
            "supply_market",
            "borrow_market",
            "collateral_market",
            "primary_market",
            name="market_component_role_enum",
            native_enum=False,
        ),
        nullable=False,
    )

    market_exposure: Mapped[MarketExposure] = relationship()
    market: Mapped[Market] = relationship()


class Position(Base):
    """Canonical unique position dimension used for time series and served portfolio views."""

    __tablename__ = "positions"
    __table_args__ = (UniqueConstraint("position_key", name="uq_positions_key"),)

    position_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    position_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    wallet_id: Mapped[int] = mapped_column(
        ForeignKey("wallets.wallet_id", ondelete="CASCADE"), nullable=False, index=True
    )
    product_id: Mapped[int | None] = mapped_column(
        ForeignKey("products.product_id", ondelete="SET NULL"), nullable=True, index=True
    )
    protocol_id: Mapped[int] = mapped_column(
        ForeignKey("protocols.protocol_id", ondelete="CASCADE"), nullable=False, index=True
    )
    chain_id: Mapped[int] = mapped_column(
        ForeignKey("chains.chain_id", ondelete="CASCADE"), nullable=False, index=True
    )
    market_id: Mapped[int | None] = mapped_column(
        ForeignKey("markets.market_id", ondelete="SET NULL"), nullable=True, index=True
    )
    market_exposure_id: Mapped[int | None] = mapped_column(
        ForeignKey("market_exposures.market_exposure_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    exposure_class: Mapped[str] = mapped_column(
        Enum(
            "core_lending",
            "idle_cash",
            "ops",
            "lp",
            "other",
            name="position_exposure_class_enum",
            native_enum=False,
        ),
        nullable=False,
        default="core_lending",
    )
    status: Mapped[str] = mapped_column(
        Enum("open", "closed", name="position_status_enum", native_enum=False),
        nullable=False,
        default="open",
        index=True,
    )
    display_name: Mapped[str] = mapped_column(String(255), nullable=False)
    opened_at_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )

    wallet: Mapped[Wallet] = relationship()
    product: Mapped[Product | None] = relationship()
    protocol: Mapped[Protocol] = relationship()
    chain: Mapped[Chain] = relationship()
    market: Mapped[Market | None] = relationship()
    market_exposure: Mapped[MarketExposure | None] = relationship()


class PositionSnapshot(Base):
    """Position snapshot fact table."""

    __tablename__ = "position_snapshots"
    __table_args__ = (
        UniqueConstraint("as_of_ts_utc", "position_key", name="uq_position_snapshots_asof_key"),
    )

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    block_number_or_slot: Mapped[str | None] = mapped_column(String(64), nullable=True)
    position_id: Mapped[int | None] = mapped_column(
        ForeignKey("positions.position_id", ondelete="SET NULL"), nullable=True, index=True
    )
    wallet_id: Mapped[int] = mapped_column(
        ForeignKey("wallets.wallet_id", ondelete="CASCADE"), nullable=False
    )
    market_id: Mapped[int] = mapped_column(
        ForeignKey("markets.market_id", ondelete="CASCADE"), nullable=False
    )
    position_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    supplied_amount: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supplied_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    collateral_amount: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    collateral_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    borrowed_amount: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    borrowed_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)

    supply_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    borrow_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    reward_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)

    equity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    health_factor: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    ltv: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    source: Mapped[str] = mapped_column(
        Enum("rpc", "debank", "defillama", name="snapshot_source_enum", native_enum=False),
        nullable=False,
    )

    position: Mapped[Position | None] = relationship()


class PositionSnapshotLeg(Base):
    """Leg-level position snapshot rows for supply and borrow exposures."""

    __tablename__ = "position_snapshot_legs"
    __table_args__ = (
        UniqueConstraint(
            "snapshot_id",
            "leg_type",
            name="uq_position_snapshot_legs_snapshot_leg_type",
        ),
    )

    position_snapshot_leg_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    snapshot_id: Mapped[int] = mapped_column(
        ForeignKey("position_snapshots.snapshot_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    leg_type: Mapped[str] = mapped_column(
        Enum("supply", "borrow", name="position_leg_type_enum", native_enum=False),
        nullable=False,
    )
    token_id: Mapped[int] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="CASCADE"), nullable=False, index=True
    )
    market_id: Mapped[int | None] = mapped_column(
        ForeignKey("markets.market_id", ondelete="SET NULL"), nullable=True, index=True
    )
    amount_native: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    usd_value: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    rate: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    estimated_daily_cashflow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    is_collateral: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    snapshot: Mapped[PositionSnapshot] = relationship()
    token: Mapped[Token] = relationship()
    market: Mapped[Market | None] = relationship()


class PositionFixedYieldCache(Base):
    """Cached fixed-yield metadata for positions that cannot use live market APY."""

    __tablename__ = "position_fixed_yield_cache"
    __table_args__ = (
        UniqueConstraint("position_key", name="uq_position_fixed_yield_cache_position_key"),
    )

    position_fixed_yield_cache_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    position_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    protocol_code: Mapped[str] = mapped_column(String(64), nullable=False)
    chain_code: Mapped[str] = mapped_column(String(64), nullable=False)
    wallet_address: Mapped[str] = mapped_column(String(128), nullable=False)
    market_ref: Mapped[str] = mapped_column(String(255), nullable=False)
    collateral_symbol: Mapped[str] = mapped_column(String(64), nullable=False)
    fixed_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    source: Mapped[str] = mapped_column(
        Enum(
            "pendle_history",
            name="fixed_yield_source_enum",
            native_enum=False,
        ),
        nullable=False,
    )
    position_size_native_at_refresh: Mapped[Decimal] = mapped_column(
        Numeric(38, 18), nullable=False
    )
    position_size_usd_at_refresh: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    lot_count: Mapped[int] = mapped_column(Integer, nullable=False)
    first_acquired_at_utc: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    last_refreshed_at_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)


class MarketSnapshot(Base):
    """Market-level snapshot fact table."""

    __tablename__ = "market_snapshots"
    __table_args__ = (
        UniqueConstraint(
            "as_of_ts_utc",
            "market_id",
            "source",
            name="uq_market_snapshots_asof_market_source",
        ),
    )

    snapshot_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    block_number_or_slot: Mapped[str | None] = mapped_column(String(64), nullable=True)
    market_id: Mapped[int] = mapped_column(
        ForeignKey("markets.market_id", ondelete="CASCADE"), nullable=False
    )

    total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    utilization: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supply_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    borrow_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    available_liquidity_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    max_ltv: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    liquidation_threshold: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    liquidation_penalty: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    caps_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    irm_params_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    source: Mapped[str] = mapped_column(
        Enum("rpc", "debank", "defillama", name="market_source_enum", native_enum=False),
        nullable=False,
    )


class Price(Base):
    """Token price fact table."""

    __tablename__ = "prices"
    __table_args__ = (
        UniqueConstraint("ts_utc", "token_id", "source", name="uq_prices_ts_token_source"),
    )

    price_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    token_id: Mapped[int] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="CASCADE"), nullable=False
    )
    price_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    source: Mapped[str] = mapped_column(
        Enum("rpc", "debank", "defillama", name="price_source_enum", native_enum=False),
        nullable=False,
    )
    confidence: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    token: Mapped[Token] = relationship()


class YieldDaily(Base):
    """Derived daily yield and fee rows at position + rollup levels."""

    __tablename__ = "yield_daily"
    __table_args__ = (
        UniqueConstraint(
            "business_date",
            "method",
            "row_key",
            name="uq_yield_daily_business_date_method_row_key",
        ),
    )

    yield_daily_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    wallet_id: Mapped[int | None] = mapped_column(
        ForeignKey("wallets.wallet_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    product_id: Mapped[int | None] = mapped_column(
        ForeignKey("products.product_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    protocol_id: Mapped[int | None] = mapped_column(
        ForeignKey("protocols.protocol_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    market_id: Mapped[int | None] = mapped_column(
        ForeignKey("markets.market_id", ondelete="SET NULL"),
        nullable=True,
    )
    row_key: Mapped[str] = mapped_column(String(255), nullable=False)
    position_key: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    gross_yield_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    strategy_fee_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avant_gop_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    net_yield_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avg_equity_usd: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    gross_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    post_strategy_fee_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    net_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    avant_gop_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    method: Mapped[str] = mapped_column(String(64), nullable=False)
    confidence_score: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    wallet: Mapped[Wallet | None] = relationship()
    product: Mapped[Product | None] = relationship()
    protocol: Mapped[Protocol | None] = relationship()
    market: Mapped[Market | None] = relationship()


class MarketOverviewDaily(Base):
    """Derived daily market overview rows at one deterministic as-of timestamp."""

    __tablename__ = "market_overview_daily"
    __table_args__ = (
        UniqueConstraint(
            "business_date",
            "market_id",
            name="uq_market_overview_daily_date_market",
        ),
    )

    market_overview_daily_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
    )
    market_id: Mapped[int] = mapped_column(
        ForeignKey("markets.market_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    source: Mapped[str] = mapped_column(
        Enum("rpc", "debank", "defillama", name="market_source_enum", native_enum=False),
        nullable=False,
    )
    total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    utilization: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    available_liquidity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supply_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    borrow_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    spread_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    avant_supplied_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avant_borrowed_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avant_supply_share: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    avant_borrow_share: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    max_ltv: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    liquidation_threshold: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    liquidation_penalty: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    market: Mapped[Market] = relationship()


class MarketHealthDaily(Base):
    """Persisted daily native-market health rows, including non-alerting trend metrics."""

    __tablename__ = "market_health_daily"
    __table_args__ = (
        UniqueConstraint("business_date", "market_id", name="uq_market_health_daily_date_market"),
    )

    market_health_daily_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    market_id: Mapped[int] = mapped_column(
        ForeignKey("markets.market_id", ondelete="CASCADE"), nullable=False, index=True
    )
    total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supply_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    borrow_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    utilization: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    available_liquidity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    available_liquidity_ratio: Mapped[Decimal | None] = mapped_column(
        Numeric(20, 10), nullable=True
    )
    borrow_apy_delta: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    distance_to_kink: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    risk_status: Mapped[str] = mapped_column(
        Enum("normal", "watch", "elevated", "critical", name="risk_status_enum", native_enum=False),
        nullable=False,
        default="normal",
    )
    active_alert_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    market: Mapped[Market] = relationship()


class Alert(Base):
    """Derived risk alert records for market/position monitoring."""

    __tablename__ = "alerts"
    __table_args__ = (
        Index(
            "uq_alerts_active_key",
            "alert_type",
            "entity_type",
            "entity_id",
            unique=True,
            postgresql_where=text("status IN ('open', 'ack')"),
        ),
    )

    alert_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ts_utc: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    alert_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    severity: Mapped[str] = mapped_column(
        Enum("low", "med", "high", name="alert_severity_enum", native_enum=False),
        nullable=False,
    )
    entity_type: Mapped[str] = mapped_column(
        Enum("market", "position", "wallet", name="alert_entity_type_enum", native_enum=False),
        nullable=False,
    )
    entity_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    status: Mapped[str] = mapped_column(
        Enum("open", "ack", "resolved", name="alert_status_enum", native_enum=False),
        nullable=False,
        index=True,
        default="open",
    )


class PortfolioPositionCurrent(Base):
    """Latest served portfolio positions with explicit supply and borrow leg semantics."""

    __tablename__ = "portfolio_positions_current"

    position_id: Mapped[int] = mapped_column(
        ForeignKey("positions.position_id", ondelete="CASCADE"), primary_key=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    wallet_id: Mapped[int] = mapped_column(
        ForeignKey("wallets.wallet_id", ondelete="CASCADE"), nullable=False, index=True
    )
    product_id: Mapped[int | None] = mapped_column(
        ForeignKey("products.product_id", ondelete="SET NULL"), nullable=True, index=True
    )
    protocol_id: Mapped[int] = mapped_column(
        ForeignKey("protocols.protocol_id", ondelete="CASCADE"), nullable=False, index=True
    )
    chain_id: Mapped[int] = mapped_column(
        ForeignKey("chains.chain_id", ondelete="CASCADE"), nullable=False, index=True
    )
    market_exposure_id: Mapped[int | None] = mapped_column(
        ForeignKey("market_exposures.market_exposure_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    scope_segment: Mapped[str] = mapped_column(
        Enum(
            "strategy_only",
            "customer_only",
            "overlap",
            "global",
            name="scope_segment_enum",
            native_enum=False,
        ),
        nullable=False,
        default="strategy_only",
    )
    supply_token_id: Mapped[int] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="CASCADE"), nullable=False
    )
    borrow_token_id: Mapped[int | None] = mapped_column(
        ForeignKey("tokens.token_id", ondelete="SET NULL"), nullable=True
    )
    supply_amount: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supply_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    borrow_amount: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    borrow_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    reward_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    net_equity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    leverage_ratio: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    health_factor: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    gross_yield_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    net_yield_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    gross_yield_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    net_yield_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    strategy_fee_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avant_gop_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    strategy_fee_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avant_gop_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    gross_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    net_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)


class PortfolioPositionDaily(Base):
    """Daily time-series rows for served portfolio positions."""

    __tablename__ = "portfolio_position_daily"
    __table_args__ = (
        UniqueConstraint(
            "business_date", "position_id", name="uq_portfolio_position_daily_date_position"
        ),
    )

    portfolio_position_daily_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    position_id: Mapped[int] = mapped_column(
        ForeignKey("positions.position_id", ondelete="CASCADE"), nullable=False, index=True
    )
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    market_exposure_id: Mapped[int | None] = mapped_column(
        ForeignKey("market_exposures.market_exposure_id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    scope_segment: Mapped[str] = mapped_column(
        Enum(
            "strategy_only",
            "customer_only",
            "overlap",
            "global",
            name="scope_segment_enum",
            native_enum=False,
        ),
        nullable=False,
        default="strategy_only",
    )
    supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    net_equity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    leverage_ratio: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    health_factor: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    gross_yield_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    net_yield_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    strategy_fee_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avant_gop_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    gross_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    net_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)


class PortfolioSummaryDaily(Base):
    """Daily rollup of served portfolio positions for the executive dashboard."""

    __tablename__ = "portfolio_summary_daily"
    __table_args__ = (
        UniqueConstraint(
            "business_date", "scope_segment", name="uq_portfolio_summary_daily_date_scope"
        ),
    )

    portfolio_summary_daily_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    scope_segment: Mapped[str] = mapped_column(
        Enum(
            "strategy_only",
            "customer_only",
            "overlap",
            "global",
            name="scope_segment_enum",
            native_enum=False,
        ),
        nullable=False,
    )
    total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_net_equity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    aggregate_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    total_gross_yield_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_net_yield_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_gross_yield_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_net_yield_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_strategy_fee_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_avant_gop_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_strategy_fee_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_avant_gop_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    avg_leverage_ratio: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    open_position_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class MarketExposureDaily(Base):
    """Primary served markets table for the paired exposure dashboard lens."""

    __tablename__ = "market_exposure_daily"
    __table_args__ = (
        UniqueConstraint(
            "business_date", "market_exposure_id", name="uq_market_exposure_daily_date_exposure"
        ),
    )

    market_exposure_daily_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    market_exposure_id: Mapped[int] = mapped_column(
        ForeignKey("market_exposures.market_exposure_id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    scope_segment: Mapped[str] = mapped_column(
        Enum(
            "strategy_only",
            "customer_only",
            "overlap",
            "global",
            name="scope_segment_enum",
            native_enum=False,
        ),
        nullable=False,
        default="strategy_only",
    )
    total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    weighted_supply_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    weighted_borrow_apy: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    utilization: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
    available_liquidity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    distance_to_kink: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    strategy_position_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    customer_position_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    active_alert_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    risk_status: Mapped[str] = mapped_column(
        Enum("normal", "watch", "elevated", "critical", name="risk_status_enum", native_enum=False),
        nullable=False,
        default="normal",
    )
    watch_status: Mapped[str] = mapped_column(
        Enum("normal", "watch", "alerting", name="watch_status_enum", native_enum=False),
        nullable=False,
        default="normal",
    )


class MarketSummaryDaily(Base):
    """Executive rollup of the served markets exposure layer."""

    __tablename__ = "market_summary_daily"
    __table_args__ = (
        UniqueConstraint(
            "business_date", "scope_segment", name="uq_market_summary_daily_date_scope"
        ),
    )

    market_summary_daily_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    business_date: Mapped[date] = mapped_column(nullable=False, index=True)
    scope_segment: Mapped[str] = mapped_column(
        Enum(
            "strategy_only",
            "customer_only",
            "overlap",
            "global",
            name="scope_segment_enum",
            native_enum=False,
        ),
        nullable=False,
    )
    total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    weighted_utilization: Mapped[Decimal | None] = mapped_column(Numeric(38, 18), nullable=True)
    total_available_liquidity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    markets_at_risk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    markets_on_watchlist_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)


class ExecutiveSummaryDaily(Base):
    """Persisted executive summary built from served portfolio and markets tables."""

    __tablename__ = "executive_summary_daily"

    business_date: Mapped[date] = mapped_column(primary_key=True)
    nav_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    portfolio_net_equity_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    portfolio_aggregate_roe: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    total_gross_yield_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_net_yield_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_gross_yield_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_net_yield_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_strategy_fee_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_avant_gop_daily_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_strategy_fee_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    total_avant_gop_mtd_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    market_total_supply_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    market_total_borrow_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    markets_at_risk_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    open_alert_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    customer_metrics_ready: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)


class DataQuality(Base):
    """Ingestion failure records keyed by as-of timestamp and entity context."""

    __tablename__ = "data_quality"

    data_quality_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    as_of_ts_utc: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    stage: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    protocol_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    chain_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    wallet_address: Mapped[str | None] = mapped_column(String(128), nullable=True)
    market_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    error_type: Mapped[str] = mapped_column(String(128), nullable=False)
    error_message: Mapped[str] = mapped_column(String(2000), nullable=False)
    payload_json: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
    )
