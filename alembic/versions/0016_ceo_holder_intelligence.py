"""add ceo holder intelligence tables and additive holder fields

Revision ID: 0016_ceo_holder_intelligence
Revises: 0015_consumer_debank_visibility
Create Date: 2026-03-09
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0016_ceo_holder_intelligence"
down_revision = "0015_consumer_debank_visibility"
branch_labels = None
depends_on = None


def upgrade() -> None:
    holder_behavior_columns = [
        ("wallet_held_avant_usd", sa.Numeric(38, 18), sa.text("0")),
        ("configured_deployed_avant_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_canonical_avant_exposure_usd", sa.Numeric(38, 18), sa.text("0")),
        ("wallet_family_usd_usd", sa.Numeric(38, 18), sa.text("0")),
        ("wallet_family_btc_usd", sa.Numeric(38, 18), sa.text("0")),
        ("wallet_family_eth_usd", sa.Numeric(38, 18), sa.text("0")),
        ("deployed_family_usd_usd", sa.Numeric(38, 18), sa.text("0")),
        ("deployed_family_btc_usd", sa.Numeric(38, 18), sa.text("0")),
        ("deployed_family_eth_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_family_usd_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_family_btc_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_family_eth_usd", sa.Numeric(38, 18), sa.text("0")),
        ("wallet_base_usd", sa.Numeric(38, 18), sa.text("0")),
        ("wallet_staked_usd", sa.Numeric(38, 18), sa.text("0")),
        ("wallet_boosted_usd", sa.Numeric(38, 18), sa.text("0")),
        ("deployed_base_usd", sa.Numeric(38, 18), sa.text("0")),
        ("deployed_staked_usd", sa.Numeric(38, 18), sa.text("0")),
        ("deployed_boosted_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_base_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_staked_usd", sa.Numeric(38, 18), sa.text("0")),
        ("total_boosted_usd", sa.Numeric(38, 18), sa.text("0")),
    ]
    for column_name, column_type, server_default in holder_behavior_columns:
        op.add_column(
            "holder_behavior_daily",
            sa.Column(column_name, column_type, nullable=False, server_default=server_default),
        )
    for column_name, column_type, _server_default in holder_behavior_columns:
        op.alter_column(
            "holder_behavior_daily",
            column_name,
            existing_type=column_type,
            server_default=None,
        )

    op.create_table(
        "holder_scorecard_daily",
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("tracked_holders", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("top10_holder_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("top25_holder_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("top100_holder_share", sa.Numeric(20, 10), nullable=True),
        sa.Column(
            "wallet_held_avant_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "configured_deployed_avant_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "total_canonical_avant_exposure_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("base_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("staked_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("boosted_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("single_asset_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("multi_asset_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("single_wrapper_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("multi_wrapper_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("configured_collateral_users_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column("configured_leveraged_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column(
            "whale_enter_count_7d", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column("whale_exit_count_7d", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "whale_borrow_up_count_7d", sa.Integer(), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "whale_collateral_up_count_7d",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "markets_needing_capacity_review",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("dq_verified_holder_pct", sa.Numeric(20, 10), nullable=True),
        sa.Column(
            "visibility_gap_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.PrimaryKeyConstraint("business_date"),
    )
    op.create_index(
        op.f("ix_holder_scorecard_daily_as_of_ts_utc"),
        "holder_scorecard_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    for column_name in [
        "tracked_holders",
        "wallet_held_avant_usd",
        "configured_deployed_avant_usd",
        "total_canonical_avant_exposure_usd",
        "whale_enter_count_7d",
        "whale_exit_count_7d",
        "whale_borrow_up_count_7d",
        "whale_collateral_up_count_7d",
        "markets_needing_capacity_review",
        "visibility_gap_wallet_count",
    ]:
        op.alter_column(
            "holder_scorecard_daily",
            column_name,
            server_default=None,
        )

    op.create_table(
        "holder_protocol_gap_daily",
        sa.Column(
            "holder_protocol_gap_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
        sa.Column("wallet_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "signoff_wallet_count",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "total_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "total_borrow_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("in_config_surface", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("gap_priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.PrimaryKeyConstraint("holder_protocol_gap_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "protocol_code",
            name="uq_holder_protocol_gap_daily_date_protocol",
        ),
    )
    op.create_index(
        op.f("ix_holder_protocol_gap_daily_as_of_ts_utc"),
        "holder_protocol_gap_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    op.create_index(
        op.f("ix_holder_protocol_gap_daily_business_date"),
        "holder_protocol_gap_daily",
        ["business_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_holder_protocol_gap_daily_protocol_code"),
        "holder_protocol_gap_daily",
        ["protocol_code"],
        unique=False,
    )
    for column_name in [
        "wallet_count",
        "signoff_wallet_count",
        "total_supply_usd",
        "total_borrow_usd",
        "in_config_surface",
        "gap_priority",
    ]:
        op.alter_column(
            "holder_protocol_gap_daily",
            column_name,
            server_default=None,
        )


def downgrade() -> None:
    op.drop_index(
        op.f("ix_holder_protocol_gap_daily_protocol_code"),
        table_name="holder_protocol_gap_daily",
    )
    op.drop_index(
        op.f("ix_holder_protocol_gap_daily_business_date"),
        table_name="holder_protocol_gap_daily",
    )
    op.drop_index(
        op.f("ix_holder_protocol_gap_daily_as_of_ts_utc"),
        table_name="holder_protocol_gap_daily",
    )
    op.drop_table("holder_protocol_gap_daily")

    op.drop_index(
        op.f("ix_holder_scorecard_daily_as_of_ts_utc"),
        table_name="holder_scorecard_daily",
    )
    op.drop_table("holder_scorecard_daily")

    for column_name in reversed(
        [
            "wallet_held_avant_usd",
            "configured_deployed_avant_usd",
            "total_canonical_avant_exposure_usd",
            "wallet_family_usd_usd",
            "wallet_family_btc_usd",
            "wallet_family_eth_usd",
            "deployed_family_usd_usd",
            "deployed_family_btc_usd",
            "deployed_family_eth_usd",
            "total_family_usd_usd",
            "total_family_btc_usd",
            "total_family_eth_usd",
            "wallet_base_usd",
            "wallet_staked_usd",
            "wallet_boosted_usd",
            "deployed_base_usd",
            "deployed_staked_usd",
            "deployed_boosted_usd",
            "total_base_usd",
            "total_staked_usd",
            "total_boosted_usd",
        ]
    ):
        op.drop_column("holder_behavior_daily", column_name)
