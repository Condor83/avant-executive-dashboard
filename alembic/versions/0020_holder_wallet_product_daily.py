"""add holder wallet product attribution table and universe family totals

Revision ID: 0020_hold_wallet_prod
Revises: 0019_holder_dashboard_reshape
Create Date: 2026-03-10
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0020_hold_wallet_prod"
down_revision = "0019_holder_dashboard_reshape"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for column_name in [
        "verified_family_usd_usd",
        "verified_family_btc_usd",
        "verified_family_eth_usd",
        "verified_base_usd",
        "verified_staked_usd",
        "verified_boosted_usd",
    ]:
        op.add_column(
            "consumer_holder_universe_daily",
            sa.Column(
                column_name,
                sa.Numeric(38, 18),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )
        op.alter_column("consumer_holder_universe_daily", column_name, server_default=None)

    op.create_table(
        "holder_wallet_product_daily",
        sa.Column(
            "holder_wallet_product_daily_id",
            sa.Integer(),
            autoincrement=True,
            nullable=False,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("wallet_id", sa.Integer(), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("product_scope", sa.String(length=16), nullable=False),
        sa.Column(
            "monitored_presence_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "observed_exposure_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "wallet_held_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")
        ),
        sa.Column(
            "canonical_deployed_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "external_fixed_yield_pt_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "external_yield_token_yt_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column(
            "external_other_defi_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
        sa.Column("has_any_defi_activity", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("has_any_defi_borrow", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column(
            "has_canonical_activity", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
        sa.Column("segment", sa.String(length=16), nullable=True),
        sa.Column("is_attributed", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("asset_symbols_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("borrowed_usd", sa.Numeric(38, 18), nullable=False, server_default=sa.text("0")),
        sa.Column("leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("health_factor_min", sa.Numeric(20, 10), nullable=True),
        sa.Column("risk_band", sa.String(length=32), nullable=True),
        sa.Column("age_days", sa.Integer(), nullable=True),
        sa.Column("multi_asset_flag", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.Column("aum_delta_7d_usd", sa.Numeric(38, 18), nullable=True),
        sa.Column("aum_delta_7d_pct", sa.Numeric(20, 10), nullable=True),
        sa.ForeignKeyConstraint(["wallet_id"], ["wallets.wallet_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("holder_wallet_product_daily_id"),
        sa.UniqueConstraint(
            "business_date",
            "wallet_id",
            "product_scope",
            name="uq_holder_wallet_product_daily_date_wallet_scope",
        ),
    )
    for index_name, columns in [
        ("ix_holder_wallet_product_daily_business_date", ["business_date"]),
        ("ix_holder_wallet_product_daily_as_of_ts_utc", ["as_of_ts_utc"]),
        ("ix_holder_wallet_product_daily_wallet_id", ["wallet_id"]),
        ("ix_holder_wallet_product_daily_wallet_address", ["wallet_address"]),
        ("ix_holder_wallet_product_daily_product_scope", ["product_scope"]),
        ("ix_holder_wallet_product_daily_segment", ["segment"]),
    ]:
        op.create_index(index_name, "holder_wallet_product_daily", columns, unique=False)
    for column_name in [
        "monitored_presence_usd",
        "observed_exposure_usd",
        "wallet_held_usd",
        "canonical_deployed_usd",
        "external_fixed_yield_pt_usd",
        "external_yield_token_yt_usd",
        "external_other_defi_usd",
        "has_any_defi_activity",
        "has_any_defi_borrow",
        "has_canonical_activity",
        "is_attributed",
        "borrowed_usd",
        "multi_asset_flag",
    ]:
        op.alter_column("holder_wallet_product_daily", column_name, server_default=None)


def downgrade() -> None:
    for index_name in [
        "ix_holder_wallet_product_daily_segment",
        "ix_holder_wallet_product_daily_product_scope",
        "ix_holder_wallet_product_daily_wallet_address",
        "ix_holder_wallet_product_daily_wallet_id",
        "ix_holder_wallet_product_daily_as_of_ts_utc",
        "ix_holder_wallet_product_daily_business_date",
    ]:
        op.drop_index(index_name, table_name="holder_wallet_product_daily")
    op.drop_table("holder_wallet_product_daily")

    for column_name in [
        "verified_boosted_usd",
        "verified_staked_usd",
        "verified_base_usd",
        "verified_family_eth_usd",
        "verified_family_btc_usd",
        "verified_family_usd_usd",
    ]:
        op.drop_column("consumer_holder_universe_daily", column_name)
