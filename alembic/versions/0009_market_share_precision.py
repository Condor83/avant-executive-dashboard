"""widen market overview concentration ratio precision

Revision ID: 0009_market_share_precision
Revises: 0008_portfolio_markets_reset
Create Date: 2026-03-06 02:15:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0009_market_share_precision"
down_revision = "0008_portfolio_markets_reset"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "market_overview_daily",
        "avant_supply_share",
        existing_type=sa.Numeric(20, 10),
        type_=sa.Numeric(38, 18),
        existing_nullable=True,
    )
    op.alter_column(
        "market_overview_daily",
        "avant_borrow_share",
        existing_type=sa.Numeric(20, 10),
        type_=sa.Numeric(38, 18),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "market_overview_daily",
        "avant_borrow_share",
        existing_type=sa.Numeric(38, 18),
        type_=sa.Numeric(20, 10),
        existing_nullable=True,
    )
    op.alter_column(
        "market_overview_daily",
        "avant_supply_share",
        existing_type=sa.Numeric(38, 18),
        type_=sa.Numeric(20, 10),
        existing_nullable=True,
    )
