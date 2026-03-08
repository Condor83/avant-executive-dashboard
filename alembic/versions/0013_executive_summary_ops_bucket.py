"""add market stability ops capital to executive summary

Revision ID: 0013_executive_summary_ops
Revises: 0012_position_fixed_yield_cache
Create Date: 2026-03-08
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0013_executive_summary_ops"
down_revision = "0012_position_fixed_yield_cache"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "executive_summary_daily",
        sa.Column(
            "market_stability_ops_net_equity_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.alter_column(
        "executive_summary_daily",
        "market_stability_ops_net_equity_usd",
        server_default=None,
        existing_type=sa.Numeric(38, 18),
    )


def downgrade() -> None:
    op.drop_column("executive_summary_daily", "market_stability_ops_net_equity_usd")
