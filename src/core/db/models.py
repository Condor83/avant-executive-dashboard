"""Canonical database models for dimensions and snapshot facts."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, Numeric, String, UniqueConstraint
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
