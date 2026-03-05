"""Canonical database models for dimensions and snapshot facts."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    func,
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
    """Protocol market registry dimension."""

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
    market_address: Mapped[str] = mapped_column(String(255), nullable=False)
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
    wallet_id: Mapped[int] = mapped_column(
        ForeignKey("wallets.wallet_id", ondelete="CASCADE"), nullable=False
    )
    market_id: Mapped[int] = mapped_column(
        ForeignKey("markets.market_id", ondelete="CASCADE"), nullable=False
    )
    position_key: Mapped[str] = mapped_column(String(255), nullable=False, index=True)

    supplied_amount: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
    supplied_usd: Mapped[Decimal] = mapped_column(Numeric(38, 18), nullable=False)
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
    utilization: Mapped[Decimal] = mapped_column(Numeric(20, 10), nullable=False)
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
    avant_supply_share: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    avant_borrow_share: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    max_ltv: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    liquidation_threshold: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)
    liquidation_penalty: Mapped[Decimal | None] = mapped_column(Numeric(20, 10), nullable=True)

    market: Mapped[Market] = relationship()


class Alert(Base):
    """Derived risk alert records for market/position monitoring."""

    __tablename__ = "alerts"

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
