"""create canonical schema

Revision ID: 0001_canonical_schema
Revises:
Create Date: 2026-03-03 00:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0001_canonical_schema"
down_revision = None
branch_labels = None
depends_on = None


WALLET_TYPE_ENUM = sa.Enum(
    "strategy",
    "customer",
    "internal",
    name="wallet_type_enum",
    native_enum=False,
)
SNAPSHOT_SOURCE_ENUM = sa.Enum(
    "rpc", "debank", "defillama", name="snapshot_source_enum", native_enum=False
)
MARKET_SOURCE_ENUM = sa.Enum(
    "rpc", "debank", "defillama", name="market_source_enum", native_enum=False
)
PRICE_SOURCE_ENUM = sa.Enum(
    "rpc", "debank", "defillama", name="price_source_enum", native_enum=False
)


def upgrade() -> None:
    op.create_table(
        "wallets",
        sa.Column("wallet_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("address", sa.String(length=128), nullable=False),
        sa.Column("wallet_type", WALLET_TYPE_ENUM, nullable=False),
        sa.Column("label", sa.String(length=255), nullable=True),
    )
    op.create_index("ix_wallets_address", "wallets", ["address"], unique=True)

    op.create_table(
        "products",
        sa.Column("product_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("product_code", sa.String(length=64), nullable=False),
    )
    op.create_index("ix_products_product_code", "products", ["product_code"], unique=True)

    op.create_table(
        "protocols",
        sa.Column("protocol_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
    )
    op.create_index("ix_protocols_protocol_code", "protocols", ["protocol_code"], unique=True)

    op.create_table(
        "chains",
        sa.Column("chain_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
    )
    op.create_index("ix_chains_chain_code", "chains", ["chain_code"], unique=True)

    op.create_table(
        "tokens",
        sa.Column("token_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "chain_id",
            sa.Integer(),
            sa.ForeignKey("chains.chain_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("address_or_mint", sa.String(length=255), nullable=False),
        sa.Column("symbol", sa.String(length=64), nullable=False),
        sa.Column("decimals", sa.Integer(), nullable=False),
        sa.UniqueConstraint("chain_id", "address_or_mint", name="uq_tokens_chain_address"),
    )

    op.create_table(
        "markets",
        sa.Column("market_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "chain_id",
            sa.Integer(),
            sa.ForeignKey("chains.chain_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "protocol_id",
            sa.Integer(),
            sa.ForeignKey("protocols.protocol_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("market_address", sa.String(length=255), nullable=False),
        sa.Column(
            "base_asset_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "collateral_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.UniqueConstraint(
            "chain_id", "protocol_id", "market_address", name="uq_markets_chain_proto_addr"
        ),
    )

    op.create_table(
        "wallet_product_map",
        sa.Column(
            "wallet_id",
            sa.Integer(),
            sa.ForeignKey("wallets.wallet_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "product_id",
            sa.Integer(),
            sa.ForeignKey("products.product_id", ondelete="CASCADE"),
            nullable=False,
        ),
    )

    op.create_table(
        "position_snapshots",
        sa.Column("snapshot_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("block_number_or_slot", sa.String(length=64), nullable=True),
        sa.Column(
            "wallet_id",
            sa.Integer(),
            sa.ForeignKey("wallets.wallet_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("position_key", sa.String(length=255), nullable=False),
        sa.Column("supplied_amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("supplied_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("borrowed_amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("borrowed_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("supply_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("borrow_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("reward_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("health_factor", sa.Numeric(20, 10), nullable=True),
        sa.Column("ltv", sa.Numeric(20, 10), nullable=True),
        sa.Column("source", SNAPSHOT_SOURCE_ENUM, nullable=False),
    )
    op.create_index("ix_position_snapshots_as_of_ts_utc", "position_snapshots", ["as_of_ts_utc"])
    op.create_index("ix_position_snapshots_position_key", "position_snapshots", ["position_key"])

    op.create_table(
        "market_snapshots",
        sa.Column("snapshot_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("block_number_or_slot", sa.String(length=64), nullable=True),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("utilization", sa.Numeric(20, 10), nullable=False),
        sa.Column("supply_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("borrow_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("available_liquidity_usd", sa.Numeric(38, 18), nullable=True),
        sa.Column("caps_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("irm_params_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("source", MARKET_SOURCE_ENUM, nullable=False),
    )
    op.create_index("ix_market_snapshots_as_of_ts_utc", "market_snapshots", ["as_of_ts_utc"])

    op.create_table(
        "prices",
        sa.Column("price_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("price_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("source", PRICE_SOURCE_ENUM, nullable=False),
        sa.Column("confidence", sa.Numeric(20, 10), nullable=True),
        sa.UniqueConstraint("ts_utc", "token_id", "source", name="uq_prices_ts_token_source"),
    )
    op.create_index("ix_prices_ts_utc", "prices", ["ts_utc"])


def downgrade() -> None:
    op.drop_index("ix_prices_ts_utc", table_name="prices")
    op.drop_table("prices")

    op.drop_index("ix_market_snapshots_as_of_ts_utc", table_name="market_snapshots")
    op.drop_table("market_snapshots")

    op.drop_index("ix_position_snapshots_position_key", table_name="position_snapshots")
    op.drop_index("ix_position_snapshots_as_of_ts_utc", table_name="position_snapshots")
    op.drop_table("position_snapshots")

    op.drop_table("wallet_product_map")
    op.drop_table("markets")
    op.drop_table("tokens")

    op.drop_index("ix_chains_chain_code", table_name="chains")
    op.drop_table("chains")

    op.drop_index("ix_protocols_protocol_code", table_name="protocols")
    op.drop_table("protocols")

    op.drop_index("ix_products_product_code", table_name="products")
    op.drop_table("products")

    op.drop_index("ix_wallets_address", table_name="wallets")
    op.drop_table("wallets")
