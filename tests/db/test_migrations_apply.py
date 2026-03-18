"""Migration application tests."""

from __future__ import annotations

from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def _config_for(database_url: str) -> Config:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def test_migrations_apply_cleanly(postgres_database_url: str) -> None:
    config = _config_for(postgres_database_url)

    command.upgrade(config, "head")

    engine = create_engine(postgres_database_url)
    inspector = inspect(engine)
    expected_tables = {
        "wallets",
        "products",
        "wallet_product_map",
        "protocols",
        "chains",
        "tokens",
        "markets",
        "position_snapshots",
        "position_fixed_yield_cache",
        "market_snapshots",
        "prices",
        "data_quality",
        "yield_daily",
        "alerts",
        "market_overview_daily",
        "executive_summary_daily",
        "consumer_cohort_daily",
        "consumer_holder_universe_daily",
        "holder_behavior_daily",
        "consumer_market_demand_daily",
        "consumer_debank_wallet_daily",
        "consumer_debank_protocol_daily",
        "consumer_token_holder_daily",
        "consumer_debank_token_daily",
        "holder_scorecard_daily",
        "holder_protocol_gap_daily",
        "holder_supply_coverage_daily",
        "holder_product_segment_daily",
        "holder_protocol_deploy_daily",
    }
    assert expected_tables.issubset(set(inspector.get_table_names()))

    market_snapshot_columns = {
        column["name"] for column in inspector.get_columns("market_snapshots")
    }
    assert {"max_ltv", "liquidation_threshold", "liquidation_penalty"}.issubset(
        market_snapshot_columns
    )
    yield_daily_columns = {column["name"] for column in inspector.get_columns("yield_daily")}
    assert "row_key" in yield_daily_columns
    position_snapshot_columns = {
        column["name"] for column in inspector.get_columns("position_snapshots")
    }
    assert {"collateral_amount", "collateral_usd"}.issubset(position_snapshot_columns)
    fixed_yield_cache_columns = {
        column["name"] for column in inspector.get_columns("position_fixed_yield_cache")
    }
    assert {"position_key", "fixed_apy", "position_size_native_at_refresh"}.issubset(
        fixed_yield_cache_columns
    )
    executive_summary_columns = {
        column["name"] for column in inspector.get_columns("executive_summary_daily")
    }
    assert "market_stability_ops_net_equity_usd" in executive_summary_columns
    consumer_cohort_columns = {
        column["name"] for column in inspector.get_columns("consumer_cohort_daily")
    }
    assert {"as_of_ts_utc", "wallet_id", "verified_total_avant_usd"}.issubset(
        consumer_cohort_columns
    )
    consumer_holder_universe_columns = {
        column["name"] for column in inspector.get_columns("consumer_holder_universe_daily")
    }
    assert {
        "as_of_ts_utc",
        "wallet_id",
        "verified_total_avant_usd",
        "has_usd_exposure",
        "has_eth_exposure",
        "has_btc_exposure",
        "verified_staked_usd_usd",
        "verified_staked_eth_usd",
        "verified_staked_btc_usd",
    }.issubset(consumer_holder_universe_columns)
    holder_behavior_columns = {
        column["name"] for column in inspector.get_columns("holder_behavior_daily")
    }
    assert {
        "wallet_id",
        "as_of_ts_utc",
        "verified_total_avant_usd",
        "wallet_held_avant_usd",
        "configured_deployed_avant_usd",
        "total_canonical_avant_exposure_usd",
        "borrowed_usd_delta_7d",
        "wallet_staked_usd_usd",
        "wallet_staked_eth_usd",
        "wallet_staked_btc_usd",
        "deployed_staked_usd_usd",
        "deployed_staked_eth_usd",
        "deployed_staked_btc_usd",
    }.issubset(holder_behavior_columns)
    holder_scorecard_columns = {
        column["name"] for column in inspector.get_columns("holder_scorecard_daily")
    }
    assert {
        "tracked_holders",
        "total_canonical_avant_exposure_usd",
        "whale_collateral_up_count_7d",
        "visibility_gap_wallet_count",
    }.issubset(holder_scorecard_columns)
    holder_protocol_gap_columns = {
        column["name"] for column in inspector.get_columns("holder_protocol_gap_daily")
    }
    assert {
        "as_of_ts_utc",
        "protocol_code",
        "signoff_wallet_count",
        "gap_priority",
    }.issubset(holder_protocol_gap_columns)
    consumer_market_demand_columns = {
        column["name"] for column in inspector.get_columns("consumer_market_demand_daily")
    }
    assert {"as_of_ts_utc", "capacity_pressure_score", "cap_headroom_usd"}.issubset(
        consumer_market_demand_columns
    )
    consumer_debank_wallet_columns = {
        column["name"] for column in inspector.get_columns("consumer_debank_wallet_daily")
    }
    assert {
        "as_of_ts_utc",
        "wallet_id",
        "in_seed_set",
        "fetch_succeeded",
        "configured_surface_borrow_usd",
    }.issubset(consumer_debank_wallet_columns)
    consumer_debank_protocol_columns = {
        column["name"] for column in inspector.get_columns("consumer_debank_protocol_daily")
    }
    assert {
        "as_of_ts_utc",
        "wallet_id",
        "chain_code",
        "protocol_code",
        "in_config_surface",
    }.issubset(consumer_debank_protocol_columns)
    consumer_token_holder_columns = {
        column["name"] for column in inspector.get_columns("consumer_token_holder_daily")
    }
    assert {
        "as_of_ts_utc",
        "chain_code",
        "token_symbol",
        "wallet_id",
        "usd_value",
        "holder_class",
        "exclude_from_monitoring",
        "exclude_from_customer_float",
    }.issubset(consumer_token_holder_columns)
    consumer_debank_token_columns = {
        column["name"] for column in inspector.get_columns("consumer_debank_token_daily")
    }
    assert {
        "as_of_ts_utc",
        "wallet_id",
        "chain_code",
        "protocol_code",
        "token_symbol",
        "leg_type",
        "usd_value",
    }.issubset(consumer_debank_token_columns)
    holder_supply_coverage_columns = {
        column["name"] for column in inspector.get_columns("holder_supply_coverage_daily")
    }
    assert {
        "as_of_ts_utc",
        "chain_code",
        "token_symbol",
        "monitoring_wallet_count",
        "net_customer_float_usd",
        "covered_supply_usd",
        "cross_chain_supply_usd",
        "covered_supply_pct",
    }.issubset(holder_supply_coverage_columns)
    holder_product_segment_columns = {
        column["name"] for column in inspector.get_columns("holder_product_segment_daily")
    }
    assert {
        "as_of_ts_utc",
        "product_scope",
        "cohort_segment",
        "observed_aum_usd",
        "defi_active_wallet_count",
        "avasset_deployed_wallet_count",
        "idle_usd",
        "other_defi_usd",
    }.issubset(holder_product_segment_columns)
    holder_protocol_deploy_columns = {
        column["name"] for column in inspector.get_columns("holder_protocol_deploy_daily")
    }
    assert {
        "as_of_ts_utc",
        "product_scope",
        "protocol_code",
        "chain_code",
        "verified_wallet_count",
        "total_value_usd",
        "dominant_token_symbols_json",
        "primary_use",
    }.issubset(holder_protocol_deploy_columns)

    position_snapshot_constraints = {
        constraint["name"] for constraint in inspector.get_unique_constraints("position_snapshots")
    }
    assert "uq_position_snapshots_asof_key" in position_snapshot_constraints

    market_snapshot_constraints = {
        constraint["name"] for constraint in inspector.get_unique_constraints("market_snapshots")
    }
    assert "uq_market_snapshots_asof_market_source" in market_snapshot_constraints

    market_overview_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("market_overview_daily")
    }
    assert "uq_market_overview_daily_date_market" in market_overview_constraints

    yield_daily_constraints = {
        constraint["name"] for constraint in inspector.get_unique_constraints("yield_daily")
    }
    assert "uq_yield_daily_business_date_method_row_key" in yield_daily_constraints
    fixed_yield_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("position_fixed_yield_cache")
    }
    assert "uq_position_fixed_yield_cache_position_key" in fixed_yield_constraints
    consumer_cohort_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_cohort_daily")
    }
    assert "uq_consumer_cohort_daily_date_wallet" in consumer_cohort_constraints
    consumer_holder_universe_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_holder_universe_daily")
    }
    assert "uq_consumer_holder_universe_daily_date_wallet" in consumer_holder_universe_constraints
    holder_behavior_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("holder_behavior_daily")
    }
    assert "uq_holder_behavior_daily_date_wallet" in holder_behavior_constraints
    consumer_market_demand_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_market_demand_daily")
    }
    assert "uq_consumer_market_demand_daily_date_market" in consumer_market_demand_constraints
    consumer_debank_wallet_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_debank_wallet_daily")
    }
    assert "uq_consumer_debank_wallet_daily_date_wallet" in consumer_debank_wallet_constraints
    consumer_debank_protocol_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_debank_protocol_daily")
    }
    assert (
        "uq_consumer_debank_protocol_daily_date_wallet_chain_protocol"
        in consumer_debank_protocol_constraints
    )
    consumer_token_holder_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_token_holder_daily")
    }
    assert (
        "uq_consumer_token_holder_daily_date_chain_token_wallet"
        in consumer_token_holder_constraints
    )
    consumer_debank_token_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("consumer_debank_token_daily")
    }
    assert (
        "uq_cons_debank_token_daily_date_wallet_chain_proto_token_leg"
        in consumer_debank_token_constraints
    )
    holder_protocol_gap_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("holder_protocol_gap_daily")
    }
    assert "uq_holder_protocol_gap_daily_date_protocol" in holder_protocol_gap_constraints
    holder_supply_coverage_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("holder_supply_coverage_daily")
    }
    assert "uq_holder_supply_coverage_daily_date_chain_symbol" in holder_supply_coverage_constraints
    holder_product_segment_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("holder_product_segment_daily")
    }
    assert "uq_holder_product_segment_daily_scope_segment" in holder_product_segment_constraints
    holder_protocol_deploy_constraints = {
        constraint["name"]
        for constraint in inspector.get_unique_constraints("holder_protocol_deploy_daily")
    }
    assert (
        "uq_holder_protocol_deploy_daily_scope_protocol_chain" in holder_protocol_deploy_constraints
    )

    with engine.connect() as connection:
        alert_index_def = connection.execute(
            text(
                """
                SELECT indexdef
                FROM pg_indexes
                WHERE schemaname = current_schema()
                  AND tablename = 'alerts'
                  AND indexname = 'uq_alerts_active_key'
                """
            )
        ).scalar_one()
    assert "UNIQUE" in alert_index_def
    assert "WHERE" in alert_index_def
    assert "status" in alert_index_def


def test_migration_0002_dedupes_existing_snapshot_duplicates(postgres_database_url: str) -> None:
    config = _config_for(postgres_database_url)
    command.upgrade(config, "0001_canonical_schema")

    engine = create_engine(postgres_database_url)
    with engine.begin() as connection:
        wallet_id = connection.execute(
            text(
                """
                INSERT INTO wallets (address, wallet_type)
                VALUES (:address, :wallet_type)
                RETURNING wallet_id
                """
            ),
            {
                "address": "0x1111111111111111111111111111111111111111",
                "wallet_type": "strategy",
            },
        ).scalar_one()
        protocol_id = connection.execute(
            text(
                """
                INSERT INTO protocols (protocol_code)
                VALUES (:protocol_code)
                RETURNING protocol_id
                """
            ),
            {"protocol_code": "aave_v3"},
        ).scalar_one()
        chain_id = connection.execute(
            text(
                """
                INSERT INTO chains (chain_code)
                VALUES (:chain_code)
                RETURNING chain_id
                """
            ),
            {"chain_code": "ethereum"},
        ).scalar_one()
        market_id = connection.execute(
            text(
                """
                INSERT INTO markets (
                    chain_id,
                    protocol_id,
                    market_address,
                    base_asset_token_id,
                    collateral_token_id,
                    metadata_json
                )
                VALUES (:chain_id, :protocol_id, :market_address, NULL, NULL, '{}'::jsonb)
                RETURNING market_id
                """
            ),
            {
                "chain_id": chain_id,
                "protocol_id": protocol_id,
                "market_address": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            },
        ).scalar_one()

        duplicate_as_of = "2026-03-03T12:00:00+00:00"
        connection.execute(
            text(
                """
                INSERT INTO position_snapshots (
                    as_of_ts_utc,
                    block_number_or_slot,
                    wallet_id,
                    market_id,
                    position_key,
                    supplied_amount,
                    supplied_usd,
                    borrowed_amount,
                    borrowed_usd,
                    supply_apy,
                    borrow_apy,
                    reward_apy,
                    equity_usd,
                    health_factor,
                    ltv,
                    source
                )
                VALUES
                    (
                        :as_of_ts_utc, '1', :wallet_id, :market_id, 'dup-pos',
                        1, 1, 0, 0, 0, 0, 0, 1, NULL, NULL, 'rpc'
                    ),
                    (
                        :as_of_ts_utc, '2', :wallet_id, :market_id, 'dup-pos',
                        2, 2, 0, 0, 0, 0, 0, 2, NULL, NULL, 'rpc'
                    )
                """
            ),
            {
                "as_of_ts_utc": duplicate_as_of,
                "wallet_id": wallet_id,
                "market_id": market_id,
            },
        )
        connection.execute(
            text(
                """
                INSERT INTO market_snapshots (
                    as_of_ts_utc,
                    block_number_or_slot,
                    market_id,
                    total_supply_usd,
                    total_borrow_usd,
                    utilization,
                    supply_apy,
                    borrow_apy,
                    available_liquidity_usd,
                    caps_json,
                    irm_params_json,
                    source
                )
                VALUES
                    (:as_of_ts_utc, '1', :market_id, 10, 2, 0.2, 0.01, 0.02, 8, NULL, NULL, 'rpc'),
                    (:as_of_ts_utc, '2', :market_id, 20, 4, 0.2, 0.03, 0.04, 16, NULL, NULL, 'rpc')
                """
            ),
            {"as_of_ts_utc": duplicate_as_of, "market_id": market_id},
        )

    command.upgrade(config, "head")

    with engine.connect() as connection:
        position_rows = connection.execute(
            text(
                """
                SELECT block_number_or_slot, supplied_usd
                FROM position_snapshots
                WHERE position_key = 'dup-pos'
                """
            )
        ).all()
        market_rows = connection.execute(
            text(
                """
                SELECT block_number_or_slot, total_supply_usd
                FROM market_snapshots
                WHERE market_id = :market_id AND source = 'rpc'
                """
            ),
            {"market_id": market_id},
        ).all()

    assert len(position_rows) == 1
    assert position_rows[0][0] == "2"
    assert position_rows[0][1] == Decimal("2")
    assert len(market_rows) == 1
    assert market_rows[0][0] == "2"
    assert market_rows[0][1] == Decimal("20")


def test_migration_0007_backfills_row_keys_and_dedupes_active_alerts(
    postgres_database_url: str,
) -> None:
    config = _config_for(postgres_database_url)
    command.upgrade(config, "0006_market_overview")

    engine = create_engine(postgres_database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO yield_daily (
                    business_date,
                    wallet_id,
                    product_id,
                    protocol_id,
                    market_id,
                    position_key,
                    gross_yield_usd,
                    strategy_fee_usd,
                    avant_gop_usd,
                    net_yield_usd,
                    avg_equity_usd,
                    gross_roe,
                    post_strategy_fee_roe,
                    net_roe,
                    avant_gop_roe,
                    method,
                    confidence_score
                )
                VALUES
                    (
                        '2026-03-03', NULL, NULL, NULL, NULL, 'dup-position',
                        10, 1.5, 0.85, 7.65, 100, 0.1, 0.085, 0.0765, 0.0085,
                        'apy_prorated_sod_eod', 1
                    ),
                    (
                        '2026-03-03', NULL, NULL, NULL, NULL, 'dup-position',
                        12, 1.8, 1.02, 9.18, 120, 0.1, 0.085, 0.0765, 0.0085,
                        'apy_prorated_sod_eod', 1
                    )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO alerts (
                    ts_utc,
                    alert_type,
                    severity,
                    entity_type,
                    entity_id,
                    payload_json,
                    status
                )
                VALUES
                    (
                        '2026-03-03T12:00:00+00:00', 'KINK_NEAR', 'med',
                        'market', '42', '{"version": 1}'::jsonb, 'open'
                    ),
                    (
                        '2026-03-03T13:00:00+00:00', 'KINK_NEAR', 'high',
                        'market', '42', '{"version": 2}'::jsonb, 'ack'
                    )
                """
            )
        )

    command.upgrade(config, "head")

    with engine.connect() as connection:
        yield_rows = connection.execute(
            text(
                """
                SELECT row_key, gross_yield_usd
                FROM yield_daily
                WHERE business_date = '2026-03-03'
                  AND method = 'apy_prorated_sod_eod'
                ORDER BY yield_daily_id
                """
            )
        ).all()
        active_alert_rows = connection.execute(
            text(
                """
                SELECT status, severity, payload_json
                FROM alerts
                WHERE alert_type = 'KINK_NEAR'
                  AND entity_type = 'market'
                  AND entity_id = '42'
                  AND status IN ('open', 'ack')
                ORDER BY alert_id
                """
            )
        ).all()
        resolved_alert_count = connection.execute(
            text(
                """
                SELECT count(*)
                FROM alerts
                WHERE alert_type = 'KINK_NEAR'
                  AND entity_type = 'market'
                  AND entity_id = '42'
                  AND status = 'resolved'
                """
            )
        ).scalar_one()

    assert len(yield_rows) == 1
    assert yield_rows[0][0] == "position:dup-position"
    assert yield_rows[0][1] == Decimal("12")

    assert len(active_alert_rows) == 1
    assert active_alert_rows[0][0] == "ack"
    assert active_alert_rows[0][1] == "high"
    assert active_alert_rows[0][2] == {"version": 2}
    assert resolved_alert_count == 1
