"""add data_quality table and idempotency constraints

Revision ID: 0002_runner_dq
Revises: 0001_canonical_schema
Create Date: 2026-03-03 01:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = "0002_runner_dq"
down_revision = "0001_canonical_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint(
        "uq_position_snapshots_asof_key",
        "position_snapshots",
        ["as_of_ts_utc", "position_key"],
    )
    op.create_unique_constraint(
        "uq_market_snapshots_asof_market_source",
        "market_snapshots",
        ["as_of_ts_utc", "market_id", "source"],
    )

    op.create_table(
        "data_quality",
        sa.Column("data_quality_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stage", sa.String(length=64), nullable=False),
        sa.Column("protocol_code", sa.String(length=64), nullable=True),
        sa.Column("chain_code", sa.String(length=64), nullable=True),
        sa.Column("wallet_address", sa.String(length=128), nullable=True),
        sa.Column("market_ref", sa.String(length=255), nullable=True),
        sa.Column("error_type", sa.String(length=128), nullable=False),
        sa.Column("error_message", sa.String(length=2000), nullable=False),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
    )
    op.create_index("ix_data_quality_as_of_ts_utc", "data_quality", ["as_of_ts_utc"])
    op.create_index("ix_data_quality_stage", "data_quality", ["stage"])


def downgrade() -> None:
    op.drop_index("ix_data_quality_stage", table_name="data_quality")
    op.drop_index("ix_data_quality_as_of_ts_utc", table_name="data_quality")
    op.drop_table("data_quality")

    op.drop_constraint(
        "uq_market_snapshots_asof_market_source",
        "market_snapshots",
        type_="unique",
    )
    op.drop_constraint(
        "uq_position_snapshots_asof_key",
        "position_snapshots",
        type_="unique",
    )
