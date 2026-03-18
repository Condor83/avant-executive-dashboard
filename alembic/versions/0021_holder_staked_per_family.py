"""add per-family staked breakdowns to holder universe and behavior tables

Revision ID: 0021_staked_per_family
Revises: 0020_hold_wallet_prod
Create Date: 2026-03-11
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0021_staked_per_family"
down_revision = "0020_hold_wallet_prod"
branch_labels = None
depends_on = None


def upgrade() -> None:
    for column_name in [
        "verified_staked_usd_usd",
        "verified_staked_eth_usd",
        "verified_staked_btc_usd",
    ]:
        op.add_column(
            "consumer_holder_universe_daily",
            sa.Column(
                column_name,
                sa.Numeric(38, 18),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )
        op.alter_column("consumer_holder_universe_daily", column_name, server_default=None)

    for column_name in [
        "wallet_staked_usd_usd",
        "wallet_staked_eth_usd",
        "wallet_staked_btc_usd",
        "deployed_staked_usd_usd",
        "deployed_staked_eth_usd",
        "deployed_staked_btc_usd",
    ]:
        op.add_column(
            "holder_behavior_daily",
            sa.Column(
                column_name,
                sa.Numeric(38, 18),
                nullable=False,
                server_default=sa.text("0"),
            ),
        )
        op.alter_column("holder_behavior_daily", column_name, server_default=None)


def downgrade() -> None:
    for column_name in [
        "deployed_staked_btc_usd",
        "deployed_staked_eth_usd",
        "deployed_staked_usd_usd",
        "wallet_staked_btc_usd",
        "wallet_staked_eth_usd",
        "wallet_staked_usd_usd",
    ]:
        op.drop_column("holder_behavior_daily", column_name)

    for column_name in [
        "verified_staked_btc_usd",
        "verified_staked_eth_usd",
        "verified_staked_usd_usd",
    ]:
        op.drop_column("consumer_holder_universe_daily", column_name)
