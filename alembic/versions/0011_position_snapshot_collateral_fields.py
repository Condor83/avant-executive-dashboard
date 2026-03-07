"""Add collateral amount and USD fields to position snapshots.

Revision ID: 0011_position_snapshot_collat
Revises: 0010_market_view_ratio_precision
Create Date: 2026-03-05
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0011_position_snapshot_collat"
down_revision = "0010_market_view_ratio_precision"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "position_snapshots",
        sa.Column("collateral_amount", sa.Numeric(38, 18), nullable=True),
    )
    op.add_column(
        "position_snapshots",
        sa.Column("collateral_usd", sa.Numeric(38, 18), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("position_snapshots", "collateral_usd")
    op.drop_column("position_snapshots", "collateral_amount")
