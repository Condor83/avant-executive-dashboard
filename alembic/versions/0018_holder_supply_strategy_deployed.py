"""add strategy deployed supply to holder coverage

Revision ID: 0018_hold_strat_dep
Revises: 0017_holder_supply_coverage
Create Date: 2026-03-09
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0018_hold_strat_dep"
down_revision = "0017_holder_supply_coverage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "holder_supply_coverage_daily",
        sa.Column(
            "strategy_deployed_supply_usd",
            sa.Numeric(38, 18),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.alter_column(
        "holder_supply_coverage_daily",
        "strategy_deployed_supply_usd",
        server_default=None,
    )


def downgrade() -> None:
    op.drop_column("holder_supply_coverage_daily", "strategy_deployed_supply_usd")
