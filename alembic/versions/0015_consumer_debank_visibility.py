"""add consumer debank visibility tables

Revision ID: 0015_consumer_debank_visibility
Revises: 0014_consumer_holder_behavior
Create Date: 2026-03-09
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0015_consumer_debank_visibility"
down_revision = "0014_consumer_holder_behavior"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "consumer_debank_wallet_daily",
        sa.Column(
            "consumer_debank_wallet_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("in_seed_set", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("in_verified_cohort", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("in_signoff_cohort", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "seed_sources_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column(
            "discovery_sources_json",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
        sa.Column("fetch_succeeded", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("fetch_error_message", sa.String(length=255), nullable=True),
        sa.Column("has_any_activity", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_any_borrow", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "has_configured_surface_activity",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
        sa.Column("protocol_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("chain_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column(
            "configured_protocol_count",
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
        sa.Column(
            "configured_surface_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "configured_surface_borrow_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_debank_wallet_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "wallet_id",
            name="uq_consumer_debank_wallet_daily_date_wallet",
        ),
    )
    op.create_index(
        op.f("ix_consumer_debank_wallet_daily_as_of_ts_utc"),
        "consumer_debank_wallet_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_wallet_daily_business_date"),
        "consumer_debank_wallet_daily",
        ["business_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_wallet_daily_wallet_address"),
        "consumer_debank_wallet_daily",
        ["wallet_address"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_wallet_daily_wallet_id"),
        "consumer_debank_wallet_daily",
        ["wallet_id"],
        unique=False,
    )

    op.create_table(
        "consumer_debank_protocol_daily",
        sa.Column(
            "consumer_debank_protocol_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
        sa.Column("in_config_surface", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "borrow_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("consumer_debank_protocol_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "wallet_id",
            "chain_code",
            "protocol_code",
            name="uq_consumer_debank_protocol_daily_date_wallet_chain_protocol",
        ),
    )
    op.create_index(
        op.f("ix_consumer_debank_protocol_daily_as_of_ts_utc"),
        "consumer_debank_protocol_daily",
        ["as_of_ts_utc"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_protocol_daily_business_date"),
        "consumer_debank_protocol_daily",
        ["business_date"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_protocol_daily_chain_code"),
        "consumer_debank_protocol_daily",
        ["chain_code"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_protocol_daily_protocol_code"),
        "consumer_debank_protocol_daily",
        ["protocol_code"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_protocol_daily_wallet_address"),
        "consumer_debank_protocol_daily",
        ["wallet_address"],
        unique=False,
    )
    op.create_index(
        op.f("ix_consumer_debank_protocol_daily_wallet_id"),
        "consumer_debank_protocol_daily",
        ["wallet_id"],
        unique=False,
    )

    for table_name, columns in {
        "consumer_debank_wallet_daily": [
            "in_seed_set",
            "in_verified_cohort",
            "in_signoff_cohort",
            "fetch_succeeded",
            "has_any_activity",
            "has_any_borrow",
            "has_configured_surface_activity",
            "protocol_count",
            "chain_count",
            "configured_protocol_count",
            "total_supply_usd",
            "total_borrow_usd",
            "configured_surface_supply_usd",
            "configured_surface_borrow_usd",
        ],
        "consumer_debank_protocol_daily": [
            "in_config_surface",
            "supply_usd",
            "borrow_usd",
        ],
    }.items():
        for column in columns:
            op.alter_column(table_name, column, server_default=None)


def downgrade() -> None:
    op.drop_index(
        op.f("ix_consumer_debank_protocol_daily_wallet_id"),
        table_name="consumer_debank_protocol_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_protocol_daily_wallet_address"),
        table_name="consumer_debank_protocol_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_protocol_daily_protocol_code"),
        table_name="consumer_debank_protocol_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_protocol_daily_chain_code"),
        table_name="consumer_debank_protocol_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_protocol_daily_business_date"),
        table_name="consumer_debank_protocol_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_protocol_daily_as_of_ts_utc"),
        table_name="consumer_debank_protocol_daily",
    )
    op.drop_table("consumer_debank_protocol_daily")

    op.drop_index(
        op.f("ix_consumer_debank_wallet_daily_wallet_id"),
        table_name="consumer_debank_wallet_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_wallet_daily_wallet_address"),
        table_name="consumer_debank_wallet_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_wallet_daily_business_date"),
        table_name="consumer_debank_wallet_daily",
    )
    op.drop_index(
        op.f("ix_consumer_debank_wallet_daily_as_of_ts_utc"),
        table_name="consumer_debank_wallet_daily",
    )
    op.drop_table("consumer_debank_wallet_daily")
