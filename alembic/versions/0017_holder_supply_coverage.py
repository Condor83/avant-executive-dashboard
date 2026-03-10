"""add holder supply coverage tables

Revision ID: 0017_holder_supply_coverage
Revises: 0016_ceo_holder_intelligence
Create Date: 2026-03-09
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0017_holder_supply_coverage"
down_revision = "0016_ceo_holder_intelligence"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_token_holder_daily",
        sa.Column(
            "consumer_token_holder_daily_id", sa.Integer(), autoincrement=True, nullable=False
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column("token_symbol", sa.String(length=64), nullable=False),
        sa.Column("token_address", sa.String(length=128), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column(
            "balance_tokens", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("usd_value", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "holder_class",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'customer'"),
        ),
        sa.Column(
            "exclude_from_monitoring", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column(
            "exclude_from_customer_float",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "source_provider",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'routescan'"),
        ),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_token_holder_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "chain_code",
            "token_address",
            "wallet_id",
            name="uq_consumer_token_holder_daily_date_chain_token_wallet",
        ),
    )
    for index_name, columns in [
        ("ix_consumer_token_holder_daily_business_date", ["business_date"]),
        ("ix_consumer_token_holder_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_consumer_token_holder_daily_chain_code", ["chain_code"]),
        ("ix_consumer_token_holder_daily_token_symbol", ["token_symbol"]),
        ("ix_consumer_token_holder_daily_token_address", ["token_address"]),
        ("ix_consumer_token_holder_daily_wallet_id", ["wallet_id"]),
        ("ix_consumer_token_holder_daily_wallet_address", ["wallet_address"]),
    ]:
        op.create_index(index_name, "consumer_token_holder_daily", columns, unique=False)
    for column_name in [
        "balance_tokens",
        "usd_value",
        "holder_class",
        "exclude_from_monitoring",
        "exclude_from_customer_float",
        "source_provider",
    ]:
        op.alter_column("consumer_token_holder_daily", column_name, server_default=None)

    op.create_table(
        "consumer_debank_token_daily",
        sa.Column(
            "consumer_debank_token_daily_id", sa.Integer(), autoincrement=True, nullable=False
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
        sa.Column("token_symbol", sa.String(length=64), nullable=False),
        sa.Column("leg_type", sa.String(length=16), nullable=False),
        sa.Column("in_config_surface", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("usd_value", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_debank_token_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "wallet_id",
            "chain_code",
            "protocol_code",
            "token_symbol",
            "leg_type",
            name="uq_cons_debank_token_daily_date_wallet_chain_proto_token_leg",
        ),
    )
    for index_name, columns in [
        ("ix_consumer_debank_token_daily_business_date", ["business_date"]),
        ("ix_consumer_debank_token_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_consumer_debank_token_daily_wallet_id", ["wallet_id"]),
        ("ix_consumer_debank_token_daily_wallet_address", ["wallet_address"]),
        ("ix_consumer_debank_token_daily_chain_code", ["chain_code"]),
        ("ix_consumer_debank_token_daily_protocol_code", ["protocol_code"]),
        ("ix_consumer_debank_token_daily_token_symbol", ["token_symbol"]),
        ("ix_consumer_debank_token_daily_leg_type", ["leg_type"]),
    ]:
        op.create_index(index_name, "consumer_debank_token_daily", columns, unique=False)
    for column_name in [
        "in_config_surface",
        "usd_value",
    ]:
        op.alter_column("consumer_debank_token_daily", column_name, server_default=None)

    op.create_table(
        "holder_supply_coverage_daily",
        sa.Column(
            "holder_supply_coverage_daily_id", sa.Integer(), autoincrement=True, nullable=False
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column("token_symbol", sa.String(length=64), nullable=False),
        sa.Column("token_address", sa.String(length=128), nullable=False),
        sa.Column(
            "raw_holder_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "monitoring_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("core_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "signoff_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "wallets_with_same_chain_deployed_supply",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "wallets_with_cross_chain_supply",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "gross_supply_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "strategy_supply_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "internal_supply_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "explicit_excluded_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "net_customer_float_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "direct_holder_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "core_direct_holder_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "signoff_direct_holder_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "same_chain_deployed_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "cross_chain_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "core_same_chain_deployed_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "signoff_same_chain_deployed_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "covered_supply_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "core_covered_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "signoff_covered_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("covered_supply_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("core_covered_supply_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("signoff_covered_supply_pct", sa.Numeric(20, 10), nullable=True),
        sa.PrimaryKeyConstraint("holder_supply_coverage_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "chain_code",
            "token_symbol",
            name="uq_holder_supply_coverage_daily_date_chain_symbol",
        ),
    )
    for index_name, columns in [
        ("ix_holder_supply_coverage_daily_business_date", ["business_date"]),
        ("ix_holder_supply_coverage_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_holder_supply_coverage_daily_chain_code", ["chain_code"]),
        ("ix_holder_supply_coverage_daily_token_symbol", ["token_symbol"]),
        ("ix_holder_supply_coverage_daily_token_address", ["token_address"]),
    ]:
        op.create_index(index_name, "holder_supply_coverage_daily", columns, unique=False)
    for column_name in [
        "raw_holder_wallet_count",
        "monitoring_wallet_count",
        "core_wallet_count",
        "signoff_wallet_count",
        "wallets_with_same_chain_deployed_supply",
        "wallets_with_cross_chain_supply",
        "gross_supply_usd",
        "strategy_supply_usd",
        "internal_supply_usd",
        "explicit_excluded_supply_usd",
        "net_customer_float_usd",
        "direct_holder_supply_usd",
        "core_direct_holder_supply_usd",
        "signoff_direct_holder_supply_usd",
        "same_chain_deployed_supply_usd",
        "cross_chain_supply_usd",
        "core_same_chain_deployed_supply_usd",
        "signoff_same_chain_deployed_supply_usd",
        "covered_supply_usd",
        "core_covered_supply_usd",
        "signoff_covered_supply_usd",
    ]:
        op.alter_column("holder_supply_coverage_daily", column_name, server_default=None)


def downgrade() -> None:
    for index_name in [
        "ix_holder_supply_coverage_daily_token_address",
        "ix_holder_supply_coverage_daily_token_symbol",
        "ix_holder_supply_coverage_daily_chain_code",
        "ix_holder_supply_coverage_daily_as_of_ts_utc",
        "ix_holder_supply_coverage_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="holder_supply_coverage_daily")
    op.drop_table("holder_supply_coverage_daily")

    for index_name in [
        "ix_consumer_debank_token_daily_leg_type",
        "ix_consumer_debank_token_daily_token_symbol",
        "ix_consumer_debank_token_daily_protocol_code",
        "ix_consumer_debank_token_daily_chain_code",
        "ix_consumer_debank_token_daily_wallet_address",
        "ix_consumer_debank_token_daily_wallet_id",
        "ix_consumer_debank_token_daily_as_of_ts_utc",
        "ix_consumer_debank_token_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="consumer_debank_token_daily")
    op.drop_table("consumer_debank_token_daily")

    for index_name in [
        "ix_consumer_token_holder_daily_wallet_address",
        "ix_consumer_token_holder_daily_wallet_id",
        "ix_consumer_token_holder_daily_token_address",
        "ix_consumer_token_holder_daily_token_symbol",
        "ix_consumer_token_holder_daily_chain_code",
        "ix_consumer_token_holder_daily_as_of_ts_utc",
        "ix_consumer_token_holder_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="consumer_token_holder_daily")
    op.drop_table("consumer_token_holder_daily")
