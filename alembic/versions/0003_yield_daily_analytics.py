"""add yield_daily derived analytics table

Revision ID: 0003_yield_daily
Revises: 0002_runner_dq
Create Date: 2026-03-03 13:20:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0003_yield_daily"
down_revision = "0002_runner_dq"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "yield_daily",
        sa.Column("yield_daily_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column(
            "wallet_id",
            sa.Integer(),
            sa.ForeignKey("wallets.wallet_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "product_id",
            sa.Integer(),
            sa.ForeignKey("products.product_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "protocol_id",
            sa.Integer(),
            sa.ForeignKey("protocols.protocol_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("position_key", sa.String(length=255), nullable=True),
        sa.Column("gross_yield_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("strategy_fee_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avant_gop_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_yield_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("method", sa.String(length=64), nullable=False),
        sa.Column("confidence_score", sa.Numeric(20, 10), nullable=True),
    )
    op.create_index("ix_yield_daily_business_date", "yield_daily", ["business_date"])
    op.create_index("ix_yield_daily_wallet_id", "yield_daily", ["wallet_id"])
    op.create_index("ix_yield_daily_product_id", "yield_daily", ["product_id"])
    op.create_index("ix_yield_daily_protocol_id", "yield_daily", ["protocol_id"])
    op.create_index("ix_yield_daily_position_key", "yield_daily", ["position_key"])


def downgrade() -> None:
    op.drop_index("ix_yield_daily_position_key", table_name="yield_daily")
    op.drop_index("ix_yield_daily_protocol_id", table_name="yield_daily")
    op.drop_index("ix_yield_daily_product_id", table_name="yield_daily")
    op.drop_index("ix_yield_daily_wallet_id", table_name="yield_daily")
    op.drop_index("ix_yield_daily_business_date", table_name="yield_daily")
    op.drop_table("yield_daily")
