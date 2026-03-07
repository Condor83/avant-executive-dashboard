"""add portfolio and markets contract reset tables

Revision ID: 0008_portfolio_markets_reset
Revises: 0007_storage_idempotency
Create Date: 2026-03-05 18:00:00
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "0008_portfolio_markets_reset"
down_revision = "0007_storage_idempotency"
branch_labels = None
depends_on = None

MARKET_KIND_ENUM = sa.Enum(
    "reserve",
    "market",
    "vault",
    "wallet_balance_token",
    "liquidity_book_pool",
    "concentrated_liquidity_pool",
    "vault_underlying",
    "consumer_market",
    "other",
    name="market_kind_enum",
    native_enum=False,
)
MARKET_EXPOSURE_KIND_ENUM = sa.Enum(
    "reserve_pair",
    "native_market",
    "vault_exposure",
    name="market_exposure_kind_enum",
    native_enum=False,
)
MARKET_COMPONENT_ROLE_ENUM = sa.Enum(
    "supply_market",
    "borrow_market",
    "collateral_market",
    "primary_market",
    name="market_component_role_enum",
    native_enum=False,
)
POSITION_EXPOSURE_CLASS_ENUM = sa.Enum(
    "core_lending",
    "idle_cash",
    "ops",
    "lp",
    "other",
    name="position_exposure_class_enum",
    native_enum=False,
)
POSITION_STATUS_ENUM = sa.Enum(
    "open",
    "closed",
    name="position_status_enum",
    native_enum=False,
)
POSITION_LEG_TYPE_ENUM = sa.Enum(
    "supply",
    "borrow",
    name="position_leg_type_enum",
    native_enum=False,
)
SCOPE_SEGMENT_ENUM = sa.Enum(
    "strategy_only",
    "customer_only",
    "overlap",
    "global",
    name="scope_segment_enum",
    native_enum=False,
)
RISK_STATUS_ENUM = sa.Enum(
    "normal",
    "watch",
    "elevated",
    "critical",
    name="risk_status_enum",
    native_enum=False,
)
WATCH_STATUS_ENUM = sa.Enum(
    "normal",
    "watch",
    "alerting",
    name="watch_status_enum",
    native_enum=False,
)


def upgrade() -> None:
    op.add_column(
        "markets",
        sa.Column("native_market_key", sa.String(length=255), nullable=True),
    )
    op.add_column(
        "markets",
        sa.Column("market_kind", MARKET_KIND_ENUM, nullable=True),
    )
    op.add_column(
        "markets",
        sa.Column("display_name", sa.String(length=255), nullable=True),
    )
    op.create_index("ix_markets_native_market_key", "markets", ["native_market_key"])
    op.execute(
        "UPDATE markets SET native_market_key = market_address WHERE native_market_key IS NULL"
    )
    op.execute(
        """
        UPDATE markets
        SET market_kind = COALESCE(NULLIF(metadata_json->>'kind', ''), 'other')
        WHERE market_kind IS NULL
        """
    )
    op.execute(
        """
        UPDATE markets
        SET display_name = COALESCE(
            NULLIF(metadata_json->>'name', ''),
            NULLIF(metadata_json->>'symbol', ''),
            NULLIF(metadata_json->>'asset_symbol', ''),
            market_address
        )
        WHERE display_name IS NULL OR display_name = ''
        """
    )
    op.alter_column(
        "markets", "native_market_key", existing_type=sa.String(length=255), nullable=False
    )
    op.alter_column("markets", "market_kind", existing_type=MARKET_KIND_ENUM, nullable=False)
    op.alter_column("markets", "display_name", existing_type=sa.String(length=255), nullable=False)

    op.create_table(
        "market_exposures",
        sa.Column("market_exposure_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "protocol_id",
            sa.Integer(),
            sa.ForeignKey("protocols.protocol_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "chain_id",
            sa.Integer(),
            sa.ForeignKey("chains.chain_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("exposure_kind", MARKET_EXPOSURE_KIND_ENUM, nullable=False),
        sa.Column(
            "supply_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "debt_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "collateral_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("exposure_slug", sa.String(length=255), nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.UniqueConstraint(
            "protocol_id",
            "chain_id",
            "exposure_kind",
            "supply_token_id",
            "debt_token_id",
            "collateral_token_id",
            name="uq_market_exposures_identity",
        ),
        sa.UniqueConstraint("exposure_slug", name="uq_market_exposures_slug"),
    )
    op.create_index("ix_market_exposures_exposure_slug", "market_exposures", ["exposure_slug"])

    op.create_table(
        "market_exposure_components",
        sa.Column(
            "market_exposure_component_id", sa.Integer(), primary_key=True, autoincrement=True
        ),
        sa.Column(
            "market_exposure_id",
            sa.Integer(),
            sa.ForeignKey("market_exposures.market_exposure_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("component_role", MARKET_COMPONENT_ROLE_ENUM, nullable=False),
        sa.UniqueConstraint(
            "market_exposure_id",
            "market_id",
            "component_role",
            name="uq_market_exposure_components_identity",
        ),
    )
    op.create_index(
        "ix_market_exposure_components_market_exposure_id",
        "market_exposure_components",
        ["market_exposure_id"],
    )
    op.create_index(
        "ix_market_exposure_components_market_id",
        "market_exposure_components",
        ["market_id"],
    )

    op.create_table(
        "positions",
        sa.Column("position_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("position_key", sa.String(length=255), nullable=False),
        sa.Column(
            "wallet_id",
            sa.Integer(),
            sa.ForeignKey("wallets.wallet_id", ondelete="CASCADE"),
            nullable=False,
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
            sa.ForeignKey("protocols.protocol_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "chain_id",
            sa.Integer(),
            sa.ForeignKey("chains.chain_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column(
            "market_exposure_id",
            sa.Integer(),
            sa.ForeignKey("market_exposures.market_exposure_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("exposure_class", POSITION_EXPOSURE_CLASS_ENUM, nullable=False),
        sa.Column("status", POSITION_STATUS_ENUM, nullable=False),
        sa.Column("display_name", sa.String(length=255), nullable=False),
        sa.Column("opened_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at_utc", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("position_key", name="uq_positions_key"),
    )
    op.create_index("ix_positions_position_key", "positions", ["position_key"])
    op.create_index("ix_positions_wallet_id", "positions", ["wallet_id"])
    op.create_index("ix_positions_product_id", "positions", ["product_id"])
    op.create_index("ix_positions_protocol_id", "positions", ["protocol_id"])
    op.create_index("ix_positions_chain_id", "positions", ["chain_id"])
    op.create_index("ix_positions_market_id", "positions", ["market_id"])
    op.create_index("ix_positions_market_exposure_id", "positions", ["market_exposure_id"])
    op.create_index("ix_positions_status", "positions", ["status"])
    op.create_index("ix_positions_last_seen_at_utc", "positions", ["last_seen_at_utc"])

    op.add_column(
        "position_snapshots",
        sa.Column(
            "position_id",
            sa.Integer(),
            sa.ForeignKey("positions.position_id", ondelete="SET NULL"),
            nullable=True,
        ),
    )
    op.create_index("ix_position_snapshots_position_id", "position_snapshots", ["position_id"])

    op.create_table(
        "position_snapshot_legs",
        sa.Column("position_snapshot_leg_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column(
            "snapshot_id",
            sa.Integer(),
            sa.ForeignKey("position_snapshots.snapshot_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("leg_type", POSITION_LEG_TYPE_ENUM, nullable=False),
        sa.Column(
            "token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("amount_native", sa.Numeric(38, 18), nullable=False),
        sa.Column("usd_value", sa.Numeric(38, 18), nullable=False),
        sa.Column("rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("estimated_daily_cashflow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("is_collateral", sa.Boolean(), nullable=False, server_default=sa.false()),
        sa.UniqueConstraint(
            "snapshot_id",
            "leg_type",
            name="uq_position_snapshot_legs_snapshot_leg_type",
        ),
    )
    op.create_index(
        "ix_position_snapshot_legs_snapshot_id", "position_snapshot_legs", ["snapshot_id"]
    )
    op.create_index("ix_position_snapshot_legs_token_id", "position_snapshot_legs", ["token_id"])
    op.create_index("ix_position_snapshot_legs_market_id", "position_snapshot_legs", ["market_id"])

    op.create_table(
        "market_health_daily",
        sa.Column("market_health_daily_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "market_id",
            sa.Integer(),
            sa.ForeignKey("markets.market_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("supply_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("borrow_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("utilization", sa.Numeric(20, 10), nullable=False),
        sa.Column("available_liquidity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("available_liquidity_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("borrow_apy_delta", sa.Numeric(20, 10), nullable=True),
        sa.Column("distance_to_kink", sa.Numeric(20, 10), nullable=True),
        sa.Column("risk_status", RISK_STATUS_ENUM, nullable=False),
        sa.Column("active_alert_count", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint(
            "business_date", "market_id", name="uq_market_health_daily_date_market"
        ),
    )
    op.create_index(
        "ix_market_health_daily_business_date", "market_health_daily", ["business_date"]
    )
    op.create_index("ix_market_health_daily_as_of_ts_utc", "market_health_daily", ["as_of_ts_utc"])
    op.create_index("ix_market_health_daily_market_id", "market_health_daily", ["market_id"])

    op.create_table(
        "portfolio_positions_current",
        sa.Column(
            "position_id",
            sa.Integer(),
            sa.ForeignKey("positions.position_id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "wallet_id",
            sa.Integer(),
            sa.ForeignKey("wallets.wallet_id", ondelete="CASCADE"),
            nullable=False,
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
            sa.ForeignKey("protocols.protocol_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "chain_id",
            sa.Integer(),
            sa.ForeignKey("chains.chain_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "market_exposure_id",
            sa.Integer(),
            sa.ForeignKey("market_exposures.market_exposure_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("scope_segment", SCOPE_SEGMENT_ENUM, nullable=False),
        sa.Column(
            "supply_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "borrow_token_id",
            sa.Integer(),
            sa.ForeignKey("tokens.token_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("supply_amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("supply_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("borrow_amount", sa.Numeric(38, 18), nullable=False),
        sa.Column("borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("borrow_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("reward_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("net_equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("health_factor", sa.Numeric(20, 10), nullable=True),
        sa.Column("gross_yield_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_yield_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("gross_yield_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_yield_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("strategy_fee_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avant_gop_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("strategy_fee_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avant_gop_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("gross_roe", sa.Numeric(20, 10), nullable=True),
        sa.Column("net_roe", sa.Numeric(20, 10), nullable=True),
    )
    op.create_index(
        "ix_portfolio_positions_current_business_date",
        "portfolio_positions_current",
        ["business_date"],
    )
    op.create_index(
        "ix_portfolio_positions_current_as_of_ts_utc",
        "portfolio_positions_current",
        ["as_of_ts_utc"],
    )
    op.create_index(
        "ix_portfolio_positions_current_wallet_id", "portfolio_positions_current", ["wallet_id"]
    )
    op.create_index(
        "ix_portfolio_positions_current_product_id", "portfolio_positions_current", ["product_id"]
    )
    op.create_index(
        "ix_portfolio_positions_current_protocol_id", "portfolio_positions_current", ["protocol_id"]
    )
    op.create_index(
        "ix_portfolio_positions_current_chain_id", "portfolio_positions_current", ["chain_id"]
    )
    op.create_index(
        "ix_portfolio_positions_current_market_exposure_id",
        "portfolio_positions_current",
        ["market_exposure_id"],
    )

    op.create_table(
        "portfolio_position_daily",
        sa.Column(
            "portfolio_position_daily_id", sa.Integer(), primary_key=True, autoincrement=True
        ),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column(
            "position_id",
            sa.Integer(),
            sa.ForeignKey("positions.position_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("as_of_ts_utc", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "market_exposure_id",
            sa.Integer(),
            sa.ForeignKey("market_exposures.market_exposure_id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("scope_segment", SCOPE_SEGMENT_ENUM, nullable=False),
        sa.Column("supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("health_factor", sa.Numeric(20, 10), nullable=True),
        sa.Column("gross_yield_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("net_yield_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("strategy_fee_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avant_gop_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("gross_roe", sa.Numeric(20, 10), nullable=True),
        sa.Column("net_roe", sa.Numeric(20, 10), nullable=True),
        sa.UniqueConstraint(
            "business_date", "position_id", name="uq_portfolio_position_daily_date_position"
        ),
    )
    op.create_index(
        "ix_portfolio_position_daily_business_date", "portfolio_position_daily", ["business_date"]
    )
    op.create_index(
        "ix_portfolio_position_daily_position_id", "portfolio_position_daily", ["position_id"]
    )
    op.create_index(
        "ix_portfolio_position_daily_as_of_ts_utc", "portfolio_position_daily", ["as_of_ts_utc"]
    )
    op.create_index(
        "ix_portfolio_position_daily_market_exposure_id",
        "portfolio_position_daily",
        ["market_exposure_id"],
    )

    op.create_table(
        "portfolio_summary_daily",
        sa.Column("portfolio_summary_daily_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("scope_segment", SCOPE_SEGMENT_ENUM, nullable=False),
        sa.Column("total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_net_equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("aggregate_roe", sa.Numeric(20, 10), nullable=True),
        sa.Column("total_gross_yield_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_net_yield_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_gross_yield_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_net_yield_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_strategy_fee_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_avant_gop_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_strategy_fee_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_avant_gop_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("avg_leverage_ratio", sa.Numeric(20, 10), nullable=True),
        sa.Column("open_position_count", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint(
            "business_date", "scope_segment", name="uq_portfolio_summary_daily_date_scope"
        ),
    )
    op.create_index(
        "ix_portfolio_summary_daily_business_date", "portfolio_summary_daily", ["business_date"]
    )

    op.create_table(
        "market_exposure_daily",
        sa.Column("market_exposure_daily_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column(
            "market_exposure_id",
            sa.Integer(),
            sa.ForeignKey("market_exposures.market_exposure_id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("scope_segment", SCOPE_SEGMENT_ENUM, nullable=False),
        sa.Column("total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("weighted_supply_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("weighted_borrow_apy", sa.Numeric(20, 10), nullable=False),
        sa.Column("utilization", sa.Numeric(20, 10), nullable=False),
        sa.Column("available_liquidity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("distance_to_kink", sa.Numeric(20, 10), nullable=True),
        sa.Column("strategy_position_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("customer_position_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("active_alert_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("risk_status", RISK_STATUS_ENUM, nullable=False),
        sa.Column("watch_status", WATCH_STATUS_ENUM, nullable=False),
        sa.UniqueConstraint(
            "business_date", "market_exposure_id", name="uq_market_exposure_daily_date_exposure"
        ),
    )
    op.create_index(
        "ix_market_exposure_daily_business_date", "market_exposure_daily", ["business_date"]
    )
    op.create_index(
        "ix_market_exposure_daily_market_exposure_id",
        "market_exposure_daily",
        ["market_exposure_id"],
    )

    op.create_table(
        "market_summary_daily",
        sa.Column("market_summary_daily_id", sa.Integer(), primary_key=True, autoincrement=True),
        sa.Column("business_date", sa.Date(), nullable=False),
        sa.Column("scope_segment", SCOPE_SEGMENT_ENUM, nullable=False),
        sa.Column("total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("weighted_utilization", sa.Numeric(20, 10), nullable=True),
        sa.Column("total_available_liquidity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("markets_at_risk_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("markets_on_watchlist_count", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint(
            "business_date", "scope_segment", name="uq_market_summary_daily_date_scope"
        ),
    )
    op.create_index(
        "ix_market_summary_daily_business_date", "market_summary_daily", ["business_date"]
    )

    op.create_table(
        "executive_summary_daily",
        sa.Column("business_date", sa.Date(), primary_key=True),
        sa.Column("nav_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("portfolio_net_equity_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("portfolio_aggregate_roe", sa.Numeric(20, 10), nullable=True),
        sa.Column("total_gross_yield_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_net_yield_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_gross_yield_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_net_yield_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_strategy_fee_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_avant_gop_daily_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_strategy_fee_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("total_avant_gop_mtd_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("market_total_supply_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("market_total_borrow_usd", sa.Numeric(38, 18), nullable=False),
        sa.Column("markets_at_risk_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("open_alert_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "customer_metrics_ready", sa.Boolean(), nullable=False, server_default=sa.false()
        ),
    )


def downgrade() -> None:
    op.drop_table("executive_summary_daily")

    op.drop_index("ix_market_summary_daily_business_date", table_name="market_summary_daily")
    op.drop_table("market_summary_daily")

    op.drop_index("ix_market_exposure_daily_market_exposure_id", table_name="market_exposure_daily")
    op.drop_index("ix_market_exposure_daily_business_date", table_name="market_exposure_daily")
    op.drop_table("market_exposure_daily")

    op.drop_index("ix_portfolio_summary_daily_business_date", table_name="portfolio_summary_daily")
    op.drop_table("portfolio_summary_daily")

    op.drop_index(
        "ix_portfolio_position_daily_market_exposure_id", table_name="portfolio_position_daily"
    )
    op.drop_index("ix_portfolio_position_daily_as_of_ts_utc", table_name="portfolio_position_daily")
    op.drop_index("ix_portfolio_position_daily_position_id", table_name="portfolio_position_daily")
    op.drop_index(
        "ix_portfolio_position_daily_business_date", table_name="portfolio_position_daily"
    )
    op.drop_table("portfolio_position_daily")

    op.drop_index(
        "ix_portfolio_positions_current_market_exposure_id",
        table_name="portfolio_positions_current",
    )
    op.drop_index(
        "ix_portfolio_positions_current_chain_id", table_name="portfolio_positions_current"
    )
    op.drop_index(
        "ix_portfolio_positions_current_protocol_id", table_name="portfolio_positions_current"
    )
    op.drop_index(
        "ix_portfolio_positions_current_product_id", table_name="portfolio_positions_current"
    )
    op.drop_index(
        "ix_portfolio_positions_current_wallet_id", table_name="portfolio_positions_current"
    )
    op.drop_index(
        "ix_portfolio_positions_current_as_of_ts_utc", table_name="portfolio_positions_current"
    )
    op.drop_index(
        "ix_portfolio_positions_current_business_date", table_name="portfolio_positions_current"
    )
    op.drop_table("portfolio_positions_current")

    op.drop_index("ix_market_health_daily_market_id", table_name="market_health_daily")
    op.drop_index("ix_market_health_daily_as_of_ts_utc", table_name="market_health_daily")
    op.drop_index("ix_market_health_daily_business_date", table_name="market_health_daily")
    op.drop_table("market_health_daily")

    op.drop_index("ix_position_snapshot_legs_market_id", table_name="position_snapshot_legs")
    op.drop_index("ix_position_snapshot_legs_token_id", table_name="position_snapshot_legs")
    op.drop_index("ix_position_snapshot_legs_snapshot_id", table_name="position_snapshot_legs")
    op.drop_table("position_snapshot_legs")

    op.drop_index("ix_position_snapshots_position_id", table_name="position_snapshots")
    op.drop_column("position_snapshots", "position_id")

    op.drop_index("ix_positions_last_seen_at_utc", table_name="positions")
    op.drop_index("ix_positions_status", table_name="positions")
    op.drop_index("ix_positions_market_exposure_id", table_name="positions")
    op.drop_index("ix_positions_market_id", table_name="positions")
    op.drop_index("ix_positions_chain_id", table_name="positions")
    op.drop_index("ix_positions_protocol_id", table_name="positions")
    op.drop_index("ix_positions_product_id", table_name="positions")
    op.drop_index("ix_positions_wallet_id", table_name="positions")
    op.drop_index("ix_positions_position_key", table_name="positions")
    op.drop_table("positions")

    op.drop_index(
        "ix_market_exposure_components_market_id", table_name="market_exposure_components"
    )
    op.drop_index(
        "ix_market_exposure_components_market_exposure_id", table_name="market_exposure_components"
    )
    op.drop_table("market_exposure_components")

    op.drop_index("ix_market_exposures_exposure_slug", table_name="market_exposures")
    op.drop_table("market_exposures")

    op.drop_index("ix_markets_native_market_key", table_name="markets")
    op.drop_column("markets", "display_name")
    op.drop_column("markets", "market_kind")
    op.drop_column("markets", "native_market_key")
