"""Add cached fixed-yield metadata for position-level overrides.

Revision ID: 0012_position_fixed_yield_cache
Revises: 0011_position_snapshot_collat
Create Date: 2026-03-06
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0012_position_fixed_yield_cache"
down_revision = "0011_position_snapshot_collat"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "position_fixed_yield_cache",
        sa.Column(
            "position_fixed_yield_cache_id", sa.Integer(), autoincrement=True, nullable=False
        ),
        sa.Column("position_key", sa.String(length=255), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=False),
        sa.Column("chain_code", sa.String(length=64), nullable=False),
        sa.Column("wallet_address", sa.String(length=128), nullable=False),
        sa.Column("market_ref", sa.String(length=255), nullable=False),
        sa.Column("collateral_symbol", sa.String(length=64), nullable=False),
        sa.Column("fixed_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column(
            "source",
            sa.Enum("pendle_history", name="fixed_yield_source_enum", native_enum=False),
            nullable=False,
        ),
        sa.Column("position_size_native_at_refresh", sa.Numeric(38, 18), nullable=False),
        sa.Column("position_size_usd_at_refresh", sa.Numeric(38, 18), nullable=False),
        sa.Column("lot_count", sa.Integer(), nullable=False),
        sa.Column("first_acquired_at_utc", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_refreshed_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.PrimaryKeyConstraint("position_fixed_yield_cache_id"),
        sa.UniqueConstraint(
            "position_key",
            name="uq_position_fixed_yield_cache_position_key",
        ),
    )
    op.create_index(
        "ix_position_fixed_yield_cache_position_key",
        "position_fixed_yield_cache",
        ["position_key"],
        unique=False,
    )
    op.create_index(
        "ix_position_fixed_yield_cache_last_refreshed_at_utc",
        "position_fixed_yield_cache",
        ["last_refreshed_at_utc"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_position_fixed_yield_cache_last_refreshed_at_utc",
        table_name="position_fixed_yield_cache",
    )
    op.drop_index(
        "ix_position_fixed_yield_cache_position_key", table_name="position_fixed_yield_cache"
    )
    op.drop_table("position_fixed_yield_cache")
    sa.Enum("pendle_history", name="fixed_yield_source_enum", native_enum=False).drop(
        op.get_bind(), checkfirst=True
    )
