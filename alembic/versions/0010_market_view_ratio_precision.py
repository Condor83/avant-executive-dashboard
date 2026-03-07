"""widen served market ratio precision

Revision ID: 0010_market_view_ratio_precision
Revises: 0009_market_share_precision
Create Date: 2026-03-06 02:35:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0010_market_view_ratio_precision"
down_revision = "0009_market_share_precision"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "market_exposure_daily",
        "utilization",
        existing_type=sa.Numeric(20, 10),
        type_=sa.Numeric(38, 18),
        existing_nullable=False,
    )
    op.alter_column(
        "market_summary_daily",
        "weighted_utilization",
        existing_type=sa.Numeric(20, 10),
        type_=sa.Numeric(38, 18),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "market_summary_daily",
        "weighted_utilization",
        existing_type=sa.Numeric(38, 18),
        type_=sa.Numeric(20, 10),
        existing_nullable=True,
    )
    op.alter_column(
        "market_exposure_daily",
        "utilization",
        existing_type=sa.Numeric(38, 18),
        type_=sa.Numeric(20, 10),
        existing_nullable=False,
    )
