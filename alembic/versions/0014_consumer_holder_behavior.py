"""add consumer holder behavior daily tables

Revision ID: 0014_consumer_holder_behavior
Revises: 0013_executive_summary_ops
Create Date: 2026-03-09
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0014_consumer_holder_behavior"
down_revision = "0013_executive_summary_ops"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_cohort_daily",
        sa.Column("consumer_cohort_daily_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("verified_total_avant_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("discovery_sources_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("is_signoff_eligible", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("exclusion_reason", sa.String(length=255), nullable=True),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_cohort_daily_id"),
        sa.UniqueConstraint(
            "business_date", "wallet_id", name="uq_consumer_cohort_daily_date_wallet"
        ),
    )
    op.create_index(
        op.f("ix_consumer_cohort_daily_as_of_ts_utc"),
        "consumer_cohort_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_cohort_daily_business_date"),
        "consumer_cohort_daily",
        ["business_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_cohort_daily_wallet_address"),
        "consumer_cohort_daily",
        ["wallet_address"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_cohort_daily_wallet_id"),
        "consumer_cohort_daily",
        ["wallet_id"],
        unique=False,
    )
    op.alter_column(
        "consumer_cohort_daily",
        "is_signoff_eligible",
        server_default=None,
        existing_type=sa.Boolean(),
    )

    op.create_table(
        "holder_behavior_daily",
        sa.Column("holder_behavior_daily_id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("is_signoff_eligible", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("verified_total_avant_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column(
            "family_usd_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "family_btc_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "family_eth_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("base_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("staked_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("boosted_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("family_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("wrapper_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("multi_asset_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("multi_wrapper_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "idle_avant_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "idle_eligible_same_chain_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "avant_collateral_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("borrowed_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("leveraged_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "borrow_against_avant_flag",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("health_factor_min", sa.Numeric(20, 10), nullable=True),
        sa.Column("risk_band", sa.String(length=32), nullable=False, server_default="normal"),
        sa.Column("protocol_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("market_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("chain_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("behavior_tags_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("whale_rank_by_assets", sa.Integer(), nullable=True),
        sa.Column("whale_rank_by_borrow", sa.Integer(), nullable=True),
        sa.Column("total_avant_usd_delta_7d", sa.Numeric(38, 18), nullable=True),
        sa.Column("borrowed_usd_delta_7d", sa.Numeric(38, 18), nullable=True),
        sa.Column("avant_collateral_usd_delta_7d", sa.Numeric(38, 18), nullable=True),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("holder_behavior_daily_id"),
        sa.UniqueConstraint(
            "business_date", "wallet_id", name="uq_holder_behavior_daily_date_wallet"
        ),
    )
    op.create_index(
        op.f("ix_holder_behavior_daily_as_of_ts_utc"),
        "holder_behavior_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    op.create_index(
        op.f("ix_holder_behavior_daily_business_date"),
        "holder_behavior_daily",
        ["business_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_holder_behavior_daily_wallet_address"),
        "holder_behavior_daily",
        ["wallet_address"],
        unique=False,
    )
    op.create_index(
        op.f("ix_holder_behavior_daily_wallet_id"),
        "holder_behavior_daily",
        ["wallet_id"],
        unique=False,
    )

    op.create_table(
        "consumer_market_demand_daily",
        sa.Column(
            "consumer_market_demand_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("market_id", sa.Integer(), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column("collateral_family", sa.String(length=16), nullable=False),
        sa.Column("holder_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "collateral_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "leveraged_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "avant_collateral_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("borrowed_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "idle_eligible_same_chain_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("p50_leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("p90_leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("top10_collateral_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("utilization", sa.Numeric(20, 10), nullable=True),
        sa.Column("available_liquidity_usd", sa.Numeric(38, 18), nullable=True),
        sa.Column("cap_headroom_usd", sa.Numeric(38, 18), nullable=True),
        sa.Column(
            "capacity_pressure_score",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "needs_capacity_review",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column(
            "near_limit_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("avant_collateral_usd_delta_7d", sa.Numeric(38, 18), nullable=True),
        sa.Column("collateral_wallet_count_delta_7d", sa.Integer(), nullable=True),
        sa.ForeignKeyConstraint(["market_id"], ["markets.market_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_market_demand_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "market_id",
            name="uq_consumer_market_demand_daily_date_market",
        ),
    )
    op.create_index(
        op.f("ix_consumer_market_demand_daily_as_of_ts_utc"),
        "consumer_market_demand_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_market_demand_daily_business_date"),
        "consumer_market_demand_daily",
        ["business_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_market_demand_daily_chain_code"),
        "consumer_market_demand_daily",
        ["chain_code"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_market_demand_daily_market_id"),
        "consumer_market_demand_daily",
        ["market_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_market_demand_daily_protocol_code"),
        "consumer_market_demand_daily",
        ["protocol_code"],
        unique=False,
    )

    for table_name, columns in {
        "holder_behavior_daily": [
            "is_signoff_eligible",
            "family_usd_usd",
            "family_btc_usd",
            "family_eth_usd",
            "base_usd",
            "staked_usd",
            "boosted_usd",
            "family_count",
            "wrapper_count",
            "multi_asset_flag",
            "multi_wrapper_flag",
            "idle_avant_usd",
            "idle_eligible_same_chain_usd",
            "avant_collateral_usd",
            "borrowed_usd",
            "leveraged_flag",
            "borrow_against_avant_flag",
            "risk_band",
            "protocol_count",
            "market_count",
            "chain_count",
        ],
        "consumer_market_demand_daily": [
            "holder_count",
            "collateral_wallet_count",
            "leveraged_wallet_count",
            "avant_collateral_usd",
            "borrowed_usd",
            "idle_eligible_same_chain_usd",
            "capacity_pressure_score",
            "needs_capacity_review",
            "near_limit_wallet_count",
        ],
    }.items():
        for column in columns:
            op.alter_column(table_name, column, server_default=None)


def downgrade() -> None:
    op.drop_index(
        op.f("ix_consumer_market_demand_daily_protocol_code"),
        table_name="consumer_market_demand_daily",
    )
    op.drop_index(
        op.f("ix_consumer_market_demand_daily_market_id"),
        table_name="consumer_market_demand_daily",
    )
    op.drop_index(
        op.f("ix_consumer_market_demand_daily_chain_code"),
        table_name="consumer_market_demand_daily",
    )
    op.drop_index(
        op.f("ix_consumer_market_demand_daily_business_date"),
        table_name="consumer_market_demand_daily",
    )
    op.drop_index(
        op.f("ix_consumer_market_demand_daily_as_of_ts_utc"),
        table_name="consumer_market_demand_daily",
    )
    op.drop_table("consumer_market_demand_daily")

    op.drop_index(op.f("ix_holder_behavior_daily_wallet_id"), table_name="holder_behavior_daily")
    op.drop_index(
        op.f("ix_holder_behavior_daily_wallet_address"), table_name="holder_behavior_daily"
    )
    op.drop_index(
        op.f("ix_holder_behavior_daily_business_date"), table_name="holder_behavior_daily"
    )
    op.drop_index(op.f("ix_holder_behavior_daily_as_of_ts_utc"), table_name="holder_behavior_daily")
    op.drop_table("holder_behavior_daily")

    op.drop_index(op.f("ix_consumer_cohort_daily_wallet_id"), table_name="consumer_cohort_daily")
    op.drop_index(
        op.f("ix_consumer_cohort_daily_wallet_address"), table_name="consumer_cohort_daily"
    )
    op.drop_index(
        op.f("ix_consumer_cohort_daily_business_date"), table_name="consumer_cohort_daily"
    )
    op.drop_index(op.f("ix_consumer_cohort_daily_as_of_ts_utc"), table_name="consumer_cohort_daily")
    op.drop_table("consumer_cohort_daily")
