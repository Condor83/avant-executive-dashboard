"""add ROE and equity columns to yield_daily

Revision ID: 0004_yield_daily_roe
Revises: 0003_yield_daily
Create Date: 2026-03-03 16:45:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0004_yield_daily_roe"
down_revision = "0003_yield_daily"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("yield_daily", sa.Column("avg_equity_usd", sa.Numeric(38, 18), nullable=True))
    op.add_column("yield_daily", sa.Column("gross_roe", sa.Numeric(20, 10), nullable=True))
    op.add_column(
        "yield_daily",
        sa.Column("post_strategy_fee_roe", sa.Numeric(20, 10), nullable=True),
    )
    op.add_column("yield_daily", sa.Column("net_roe", sa.Numeric(20, 10), nullable=True))
    op.add_column("yield_daily", sa.Column("avant_gop_roe", sa.Numeric(20, 10), nullable=True))


def downgrade() -> None:
    op.drop_column("yield_daily", "avant_gop_roe")
    op.drop_column("yield_daily", "net_roe")
    op.drop_column("yield_daily", "post_strategy_fee_roe")
    op.drop_column("yield_daily", "gross_roe")
    op.drop_column("yield_daily", "avg_equity_usd")
