"""add widened holder universe and dashboard rollup tables

Revision ID: 0019_holder_dashboard_reshape
Revises: 0018_hold_strat_dep
Create Date: 2026-03-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0019_holder_dashboard_reshape"
down_revision = "0018_hold_strat_dep"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_holder_universe_daily",
        sa.Column(
            "consumer_holder_universe_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("verified_total_avant_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("discovery_sources_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("is_signoff_eligible", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("exclusion_reason", sa.String(length=255), nullable=True),
        sa.Column("has_usd_exposure", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_eth_exposure", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_btc_exposure", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_holder_universe_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "wallet_id",
            name="uq_consumer_holder_universe_daily_date_wallet",
        ),
    )
    for index_name, columns in [
        ("ix_consumer_holder_universe_daily_business_date", ["business_date"]),
        ("ix_consumer_holder_universe_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_consumer_holder_universe_daily_wallet_id", ["wallet_id"]),
        ("ix_consumer_holder_universe_daily_wallet_address", ["wallet_address"]),
    ]:
        op.create_index(index_name, "consumer_holder_universe_daily", columns, unique=False)
    for column_name in [
        "is_signoff_eligible",
        "has_usd_exposure",
        "has_eth_exposure",
        "has_btc_exposure",
    ]:
        op.alter_column("consumer_holder_universe_daily", column_name, server_default=None)

    op.create_table(
        "holder_product_segment_daily",
        sa.Column(
            "holder_product_segment_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("product_scope", sa.String(length=16), nullable=False),
        sa.Column("cohort_segment", sa.String(length=16), nullable=False),
        sa.Column("holder_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "defi_active_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "avasset_deployed_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "conviction_gap_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "collateralized_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "borrowed_against_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "multi_asset_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "observed_aum_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("avg_holding_usd", sa.Numeric(38, 18), nullable=True),
        sa.Column("median_age_days", sa.Integer(), nullable=True),
        sa.Column("idle_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("fixed_yield_pt_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("collateralized_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("borrowed_against_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("staked_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("defi_active_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("avasset_deployed_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("conviction_gap_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("multi_asset_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("aum_change_7d_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("new_wallet_count_7d", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "exited_wallet_count_7d", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("idle_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "fixed_yield_pt_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "yield_token_yt_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "collateralized_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("borrowed_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("staked_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "other_defi_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.PrimaryKeyConstraint("holder_product_segment_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "product_scope",
            "cohort_segment",
            name="uq_holder_product_segment_daily_scope_segment",
        ),
    )
    for index_name, columns in [
        ("ix_holder_product_segment_daily_business_date", ["business_date"]),
        ("ix_holder_product_segment_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_holder_product_segment_daily_product_scope", ["product_scope"]),
        ("ix_holder_product_segment_daily_cohort_segment", ["cohort_segment"]),
    ]:
        op.create_index(index_name, "holder_product_segment_daily", columns, unique=False)
    for column_name in [
        "holder_count",
        "defi_active_wallet_count",
        "avasset_deployed_wallet_count",
        "conviction_gap_wallet_count",
        "collateralized_wallet_count",
        "borrowed_against_wallet_count",
        "multi_asset_wallet_count",
        "observed_aum_usd",
        "new_wallet_count_7d",
        "exited_wallet_count_7d",
        "idle_usd",
        "fixed_yield_pt_usd",
        "yield_token_yt_usd",
        "collateralized_usd",
        "borrowed_usd",
        "staked_usd",
        "other_defi_usd",
    ]:
        op.alter_column("holder_product_segment_daily", column_name, server_default=None)

    op.create_table(
        "holder_protocol_deploy_daily",
        sa.Column(
            "holder_protocol_deploy_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("product_scope", sa.String(length=16), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column(
            "verified_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("core_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("whale_wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "total_value_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "total_borrow_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "dominant_token_symbols_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True
        ),
        sa.Column(
            "primary_use",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text("'other_defi'"),
        ),
        sa.PrimaryKeyConstraint("holder_protocol_deploy_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "product_scope",
            "protocol_code",
            "chain_code",
            name="uq_holder_protocol_deploy_daily_scope_protocol_chain",
        ),
    )
    for index_name, columns in [
        ("ix_holder_protocol_deploy_daily_business_date", ["business_date"]),
        ("ix_holder_protocol_deploy_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_holder_protocol_deploy_daily_product_scope", ["product_scope"]),
        ("ix_holder_protocol_deploy_daily_protocol_code", ["protocol_code"]),
        ("ix_holder_protocol_deploy_daily_chain_code", ["chain_code"]),
    ]:
        op.create_index(index_name, "holder_protocol_deploy_daily", columns, unique=False)
    for column_name in [
        "verified_wallet_count",
        "core_wallet_count",
        "whale_wallet_count",
        "total_value_usd",
        "total_borrow_usd",
        "primary_use",
    ]:
        op.alter_column("holder_protocol_deploy_daily", column_name, server_default=None)


def downgrade() -> None:
    for index_name in [
        "ix_holder_protocol_deploy_daily_chain_code",
        "ix_holder_protocol_deploy_daily_protocol_code",
        "ix_holder_protocol_deploy_daily_product_scope",
        "ix_holder_protocol_deploy_daily_as_of_ts_utc",
        "ix_holder_protocol_deploy_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="holder_protocol_deploy_daily")
    op.drop_table("holder_protocol_deploy_daily")

    for index_name in [
        "ix_holder_product_segment_daily_cohort_segment",
        "ix_holder_product_segment_daily_product_scope",
        "ix_holder_product_segment_daily_as_of_ts_utc",
        "ix_holder_product_segment_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="holder_product_segment_daily")
    op.drop_table("holder_product_segment_daily")

    for index_name in [
        "ix_consumer_holder_universe_daily_wallet_address",
        "ix_consumer_holder_universe_daily_wallet_id",
        "ix_consumer_holder_universe_daily_as_of_ts_utc",
        "ix_consumer_holder_universe_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="consumer_holder_universe_daily")
    op.drop_table("consumer_holder_universe_daily")
