"""add market snapshot risk fields and daily market overview table

Revision ID: 0006_market_overview
Revises: 0005_alerts
Create Date: 2026-03-04 12:15:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0006_market_overview"
down_revision = "0005_alerts"
branch_labels = None
depends_on = None

MARKET_SOURCE_ENUM = sa.Enum(
    "rpc",
    "debank",
    "defillama",
    name="market_source_enum",
    native_enum=False,
)


def upgrade() -> None:
    op.add_column("market_snapshots", sa.Column("max_ltv", sa.Numeric(20, 10), nullable=True))
    op.add_column(
        "market_snapshots",
        sa.Column("liquidation_threshold", sa.Numeric(20, 10), nullable=True),
    )
    op.add_column(
        "market_snapshots",
        sa.Column("liquidation_penalty", sa.Numeric(20, 10), nullable=True),
    )

    op.create_table(
        "market_overview_daily",
        sa.Column("market_overview_daily_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source", MARKET_SOURCE_ENUM, nullable=False),
        sa.Column("total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("utilization", sa.Numeric(20, 10), nullable=False),
        sa.Column("available_liquidity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("supply_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("borrow_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("spread_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("avant_supplied_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avant_borrowed_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avant_supply_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("avant_borrow_share", sa.Numeric(20, 10), nullable=True),
        sa.Column("max_ltv", sa.Numeric(20, 10), nullable=True),
        sa.Column("liquidation_threshold", sa.Numeric(20, 10), nullable=True),
        sa.Column("liquidation_penalty", sa.Numeric(20, 10), nullable=True),
        sa.UniqueConstraint(
            "business_date",
            "market_id",
            name="uq_market_overview_daily_date_market",
        ),
    )
    op.create_index(
        "ix_market_overview_daily_business_date",
        "market_overview_daily",
        ["business_date"],
    )
    op.create_index(
        "ix_market_overview_daily_as_of_ts_utc",
        "market_overview_daily",
        ["as_of_ts_utc"],
    )
    op.create_index(
        "ix_market_overview_daily_market_id",
        "market_overview_daily",
        ["market_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_market_overview_daily_market_id", table_name="market_overview_daily")
    op.drop_index("ix_market_overview_daily_as_of_ts_utc", table_name="market_overview_daily")
    op.drop_index("ix_market_overview_daily_business_date", table_name="market_overview_daily")
    op.drop_table("market_overview_daily")

    op.drop_column("market_snapshots", "liquidation_penalty")
    op.drop_column("market_snapshots", "liquidation_threshold")
    op.drop_column("market_snapshots", "max_ltv")
