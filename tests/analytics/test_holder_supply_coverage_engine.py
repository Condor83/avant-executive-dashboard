"""Holder supply coverage engine tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.holder_supply_coverage_engine import HolderSupplyCoverageEngine
from core.config import load_avant_tokens_config, load_consumer_thresholds_config
from core.db.models import (
    Chain,
    ConsumerCohortDaily,
    ConsumerDebankTokenDaily,
    ConsumerTokenHolderDaily,
    HolderBehaviorDaily,
    HolderSupplyCoverageDaily,
    Market,
    PositionSnapshot,
    Price,
    Protocol,
    Token,
    Wallet,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_holder_supply_coverage_engine_persists_customer_float_and_cross_chain_coverage(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 9)
    as_of_ts_utc = datetime(2026, 3, 10, 5, 0, tzinfo=UTC)

    with Session(engine) as session:
        wallets = [
            Wallet(address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wallet_type="strategy"),
            Wallet(address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", wallet_type="internal"),
            Wallet(address="0xcccccccccccccccccccccccccccccccccccccccc", wallet_type="customer"),
            Wallet(address="0xdddddddddddddddddddddddddddddddddddddddd", wallet_type="customer"),
            Wallet(address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", wallet_type="customer"),
        ]
        session.add_all(wallets)
        session.flush()

        chain = Chain(chain_code="avalanche")
        protocols = [
            Protocol(protocol_code="morpho"),
            Protocol(protocol_code="traderjoe_lp"),
        ]
        session.add(chain)
        session.add_all(protocols)
        session.flush()

        savusd = Token(
            chain_id=chain.chain_id,
            address_or_mint="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
            symbol="savUSD",
            decimals=18,
        )
        avusd = Token(
            chain_id=chain.chain_id,
            address_or_mint="0x24de8771bc5ddb3362db529fc3358f2df3a0e346",
            symbol="avUSD",
            decimals=18,
        )
        session.add_all([savusd, avusd])
        session.flush()

        savusd_market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocols[0].protocol_id,
            native_market_key="savusd-market",
            market_address="0x1111111111111111111111111111111111111111",
            market_kind="market",
            display_name="savUSD",
            base_asset_token_id=savusd.token_id,
        )
        savusd_avusd_pool = Market(
            chain_id=chain.chain_id,
            protocol_id=protocols[1].protocol_id,
            native_market_key="savusd-avusd-pool",
            market_address="0x2222222222222222222222222222222222222222",
            market_kind="liquidity_book_pool",
            display_name="savUSD / avUSD Pool",
            base_asset_token_id=avusd.token_id,
            collateral_token_id=savusd.token_id,
        )
        session.add_all([savusd_market, savusd_avusd_pool])
        session.flush()

        session.add_all(
            [
                Price(
                    ts_utc=as_of_ts_utc,
                    token_id=avusd.token_id,
                    price_usd=Decimal("1"),
                    source="rpc",
                    confidence=Decimal("1"),
                ),
            ]
        )
        session.flush()

        session.add_all(
            [
                ConsumerTokenHolderDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code="avalanche",
                    token_symbol="savUSD",
                    token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    balance_tokens=Decimal("100"),
                    usd_value=Decimal("100"),
                    holder_class="strategy",
                    exclude_from_monitoring=True,
                    exclude_from_customer_float=True,
                    source_provider="routescan",
                ),
                ConsumerTokenHolderDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code="avalanche",
                    token_symbol="savUSD",
                    token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                    wallet_id=wallets[1].wallet_id,
                    wallet_address=wallets[1].address,
                    balance_tokens=Decimal("50"),
                    usd_value=Decimal("50"),
                    holder_class="internal",
                    exclude_from_monitoring=True,
                    exclude_from_customer_float=True,
                    source_provider="routescan",
                ),
                ConsumerTokenHolderDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code="avalanche",
                    token_symbol="savUSD",
                    token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    balance_tokens=Decimal("200"),
                    usd_value=Decimal("200"),
                    holder_class="customer",
                    exclude_from_monitoring=False,
                    exclude_from_customer_float=False,
                    source_provider="routescan",
                ),
                ConsumerTokenHolderDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code="avalanche",
                    token_symbol="savUSD",
                    token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                    wallet_id=wallets[3].wallet_id,
                    wallet_address=wallets[3].address,
                    balance_tokens=Decimal("150"),
                    usd_value=Decimal("150"),
                    holder_class="customer",
                    exclude_from_monitoring=False,
                    exclude_from_customer_float=False,
                    source_provider="routescan",
                ),
                ConsumerTokenHolderDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    chain_code="avalanche",
                    token_symbol="savUSD",
                    token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                    wallet_id=wallets[4].wallet_id,
                    wallet_address=wallets[4].address,
                    balance_tokens=Decimal("80"),
                    usd_value=Decimal("80"),
                    holder_class="ops",
                    exclude_from_monitoring=True,
                    exclude_from_customer_float=True,
                    source_provider="routescan",
                ),
            ]
        )
        session.add_all(
            [
                PositionSnapshot(
                    as_of_ts_utc=datetime(2026, 3, 9, 20, 0, tzinfo=UTC),
                    wallet_id=wallets[0].wallet_id,
                    market_id=savusd_market.market_id,
                    position_key="morpho:avalanche:strategy:savusd",
                    supplied_amount=Decimal("10"),
                    supplied_usd=Decimal("10"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("10"),
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    market_id=savusd_market.market_id,
                    position_key="morpho:avalanche:strategy:savusd",
                    supplied_amount=Decimal("25"),
                    supplied_usd=Decimal("25"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("25"),
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=datetime(2026, 3, 9, 20, 0, tzinfo=UTC),
                    wallet_id=wallets[0].wallet_id,
                    market_id=savusd_avusd_pool.market_id,
                    position_key="traderjoe_lp:avalanche:strategy:savusd-avusd",
                    supplied_amount=Decimal("40"),
                    supplied_usd=Decimal("60"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("60"),
                    source="rpc",
                ),
                PositionSnapshot(
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    market_id=savusd_avusd_pool.market_id,
                    position_key="traderjoe_lp:avalanche:strategy:savusd-avusd",
                    supplied_amount=Decimal("50"),
                    supplied_usd=Decimal("80"),
                    collateral_amount=None,
                    collateral_usd=None,
                    borrowed_amount=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    supply_apy=Decimal("0"),
                    borrow_apy=Decimal("0"),
                    reward_apy=Decimal("0"),
                    equity_usd=Decimal("80"),
                    source="rpc",
                ),
                ConsumerDebankTokenDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    chain_code="avalanche",
                    protocol_code="morpho",
                    token_symbol="savUSD",
                    leg_type="supply",
                    in_config_surface=True,
                    usd_value=Decimal("40"),
                ),
                ConsumerDebankTokenDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    chain_code="ethereum",
                    protocol_code="morpho",
                    token_symbol="savUSD",
                    leg_type="supply",
                    in_config_surface=False,
                    usd_value=Decimal("30"),
                ),
                ConsumerDebankTokenDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[3].wallet_id,
                    wallet_address=wallets[3].address,
                    chain_code="ethereum",
                    protocol_code="wallet_balance",
                    token_symbol="savUSD",
                    leg_type="wallet",
                    in_config_surface=False,
                    usd_value=Decimal("20"),
                ),
                ConsumerDebankTokenDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[3].wallet_id,
                    wallet_address=wallets[3].address,
                    chain_code="avalanche",
                    protocol_code="morpho",
                    token_symbol="savUSD",
                    leg_type="borrow",
                    in_config_surface=True,
                    usd_value=Decimal("10"),
                ),
            ]
        )
        session.add_all(
            [
                ConsumerCohortDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    verified_total_avant_usd=Decimal("200"),
                    discovery_sources_json={"sources": ["routescan"]},
                    is_signoff_eligible=True,
                    exclusion_reason=None,
                ),
                ConsumerCohortDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[3].wallet_id,
                    wallet_address=wallets[3].address,
                    verified_total_avant_usd=Decimal("150"),
                    discovery_sources_json={"sources": ["legacy_seed"]},
                    is_signoff_eligible=True,
                    exclusion_reason=None,
                ),
            ]
        )
        session.add_all(
            [
                HolderBehaviorDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    is_signoff_eligible=True,
                    verified_total_avant_usd=Decimal("200"),
                    wallet_held_avant_usd=Decimal("200"),
                    configured_deployed_avant_usd=Decimal("40"),
                    total_canonical_avant_exposure_usd=Decimal("240"),
                    wallet_family_usd_usd=Decimal("200"),
                    wallet_family_btc_usd=Decimal("0"),
                    wallet_family_eth_usd=Decimal("0"),
                    deployed_family_usd_usd=Decimal("40"),
                    deployed_family_btc_usd=Decimal("0"),
                    deployed_family_eth_usd=Decimal("0"),
                    total_family_usd_usd=Decimal("240"),
                    total_family_btc_usd=Decimal("0"),
                    total_family_eth_usd=Decimal("0"),
                    family_usd_usd=Decimal("240"),
                    family_btc_usd=Decimal("0"),
                    family_eth_usd=Decimal("0"),
                    wallet_base_usd=Decimal("200"),
                    wallet_staked_usd=Decimal("0"),
                    wallet_boosted_usd=Decimal("0"),
                    deployed_base_usd=Decimal("40"),
                    deployed_staked_usd=Decimal("0"),
                    deployed_boosted_usd=Decimal("0"),
                    total_base_usd=Decimal("240"),
                    total_staked_usd=Decimal("0"),
                    total_boosted_usd=Decimal("0"),
                    base_usd=Decimal("240"),
                    staked_usd=Decimal("0"),
                    boosted_usd=Decimal("0"),
                    family_count=1,
                    wrapper_count=1,
                    multi_asset_flag=False,
                    multi_wrapper_flag=False,
                    idle_avant_usd=Decimal("200"),
                    idle_eligible_same_chain_usd=Decimal("200"),
                    avant_collateral_usd=Decimal("40"),
                    borrowed_usd=Decimal("0"),
                    leveraged_flag=False,
                    borrow_against_avant_flag=False,
                    leverage_ratio=None,
                    health_factor_min=None,
                    risk_band="normal",
                    protocol_count=1,
                    market_count=1,
                    chain_count=1,
                    behavior_tags_json=[],
                    whale_rank_by_assets=None,
                    whale_rank_by_borrow=None,
                    total_avant_usd_delta_7d=None,
                    borrowed_usd_delta_7d=None,
                    avant_collateral_usd_delta_7d=None,
                ),
                HolderBehaviorDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[3].wallet_id,
                    wallet_address=wallets[3].address,
                    is_signoff_eligible=True,
                    verified_total_avant_usd=Decimal("150"),
                    wallet_held_avant_usd=Decimal("150"),
                    configured_deployed_avant_usd=Decimal("0"),
                    total_canonical_avant_exposure_usd=Decimal("150"),
                    wallet_family_usd_usd=Decimal("150"),
                    wallet_family_btc_usd=Decimal("0"),
                    wallet_family_eth_usd=Decimal("0"),
                    deployed_family_usd_usd=Decimal("0"),
                    deployed_family_btc_usd=Decimal("0"),
                    deployed_family_eth_usd=Decimal("0"),
                    total_family_usd_usd=Decimal("150"),
                    total_family_btc_usd=Decimal("0"),
                    total_family_eth_usd=Decimal("0"),
                    family_usd_usd=Decimal("150"),
                    family_btc_usd=Decimal("0"),
                    family_eth_usd=Decimal("0"),
                    wallet_base_usd=Decimal("150"),
                    wallet_staked_usd=Decimal("0"),
                    wallet_boosted_usd=Decimal("0"),
                    deployed_base_usd=Decimal("0"),
                    deployed_staked_usd=Decimal("0"),
                    deployed_boosted_usd=Decimal("0"),
                    total_base_usd=Decimal("150"),
                    total_staked_usd=Decimal("0"),
                    total_boosted_usd=Decimal("0"),
                    base_usd=Decimal("150"),
                    staked_usd=Decimal("0"),
                    boosted_usd=Decimal("0"),
                    family_count=1,
                    wrapper_count=1,
                    multi_asset_flag=False,
                    multi_wrapper_flag=False,
                    idle_avant_usd=Decimal("150"),
                    idle_eligible_same_chain_usd=Decimal("150"),
                    avant_collateral_usd=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    leveraged_flag=False,
                    borrow_against_avant_flag=False,
                    leverage_ratio=None,
                    health_factor_min=None,
                    risk_band="normal",
                    protocol_count=0,
                    market_count=0,
                    chain_count=2,
                    behavior_tags_json=[],
                    whale_rank_by_assets=None,
                    whale_rank_by_borrow=None,
                    total_avant_usd_delta_7d=None,
                    borrowed_usd_delta_7d=None,
                    avant_collateral_usd_delta_7d=None,
                ),
            ]
        )
        session.commit()

        summary = HolderSupplyCoverageEngine(
            session,
            avant_tokens=load_avant_tokens_config(Path("config/avant_tokens.yaml")),
            thresholds=load_consumer_thresholds_config(Path("config/consumer_thresholds.yaml")),
        ).compute_daily(business_date=business_date)
        session.commit()

        assert summary.rows_written == 1

        row = session.scalar(
            select(HolderSupplyCoverageDaily).where(
                HolderSupplyCoverageDaily.business_date == business_date,
                HolderSupplyCoverageDaily.chain_code == "avalanche",
                HolderSupplyCoverageDaily.token_symbol == "savUSD",
            )
        )

        assert row is not None
        assert row.raw_holder_wallet_count == 5
        assert row.monitoring_wallet_count == 2
        assert row.core_wallet_count == 2
        assert row.signoff_wallet_count == 2
        assert row.gross_supply_usd == Decimal("580")
        assert row.strategy_supply_usd == Decimal("100")
        assert row.strategy_deployed_supply_usd == Decimal("55")
        assert row.internal_supply_usd == Decimal("50")
        assert row.explicit_excluded_supply_usd == Decimal("80")
        assert row.net_customer_float_usd == Decimal("295")
        assert row.direct_holder_supply_usd == Decimal("350")
        assert row.same_chain_deployed_supply_usd == Decimal("40")
        assert row.cross_chain_supply_usd == Decimal("50")
        assert row.covered_supply_usd == Decimal("440")
        assert row.covered_supply_pct == Decimal("1.4915254237")
        assert row.core_covered_supply_usd == Decimal("440")
        assert row.signoff_covered_supply_usd == Decimal("440")
        assert row.wallets_with_same_chain_deployed_supply == 1
        assert row.wallets_with_cross_chain_supply == 2
