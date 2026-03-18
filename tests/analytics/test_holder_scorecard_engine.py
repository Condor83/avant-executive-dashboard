"""Holder scorecard engine tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.holder_scorecard_engine import HolderScorecardEngine
from core.config import load_avant_tokens_config, load_consumer_thresholds_config
from core.db.models import (
    Chain,
    ConsumerCohortDaily,
    ConsumerDebankProtocolDaily,
    ConsumerDebankTokenDaily,
    ConsumerDebankWalletDaily,
    ConsumerMarketDemandDaily,
    HolderBehaviorDaily,
    HolderProtocolGapDaily,
    HolderScorecardDaily,
    Market,
    Protocol,
    Token,
    Wallet,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _holder_row(
    *,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_id: int,
    wallet_address: str,
    signoff: bool,
    wallet_held: str,
    deployed: str,
    borrowed: str,
    total_base: str,
    total_staked: str,
    total_boosted: str,
    risk_band: str = "normal",
) -> HolderBehaviorDaily:
    wallet_held_dec = Decimal(wallet_held)
    deployed_dec = Decimal(deployed)
    total_dec = wallet_held_dec + deployed_dec
    return HolderBehaviorDaily(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_id=wallet_id,
        wallet_address=wallet_address,
        is_signoff_eligible=signoff,
        verified_total_avant_usd=wallet_held_dec,
        wallet_held_avant_usd=wallet_held_dec,
        configured_deployed_avant_usd=deployed_dec,
        total_canonical_avant_exposure_usd=total_dec,
        wallet_family_usd_usd=wallet_held_dec,
        wallet_family_btc_usd=Decimal("0"),
        wallet_family_eth_usd=Decimal("0"),
        deployed_family_usd_usd=deployed_dec,
        deployed_family_btc_usd=Decimal("0"),
        deployed_family_eth_usd=Decimal("0"),
        total_family_usd_usd=total_dec,
        total_family_btc_usd=Decimal("0"),
        total_family_eth_usd=Decimal("0"),
        family_usd_usd=total_dec,
        family_btc_usd=Decimal("0"),
        family_eth_usd=Decimal("0"),
        wallet_base_usd=wallet_held_dec,
        wallet_staked_usd=Decimal("0"),
        wallet_boosted_usd=Decimal("0"),
        deployed_base_usd=Decimal(total_base) - wallet_held_dec
        if Decimal(total_base) > wallet_held_dec
        else Decimal("0"),
        deployed_staked_usd=Decimal(total_staked),
        deployed_boosted_usd=Decimal(total_boosted),
        total_base_usd=Decimal(total_base),
        total_staked_usd=Decimal(total_staked),
        total_boosted_usd=Decimal(total_boosted),
        base_usd=Decimal(total_base),
        staked_usd=Decimal(total_staked),
        boosted_usd=Decimal(total_boosted),
        family_count=1,
        wrapper_count=sum(
            1
            for value in [Decimal(total_base), Decimal(total_staked), Decimal(total_boosted)]
            if value > 0
        ),
        multi_asset_flag=False,
        multi_wrapper_flag=sum(
            1
            for value in [Decimal(total_base), Decimal(total_staked), Decimal(total_boosted)]
            if value > 0
        )
        >= 2,
        idle_avant_usd=wallet_held_dec,
        idle_eligible_same_chain_usd=wallet_held_dec,
        avant_collateral_usd=deployed_dec,
        borrowed_usd=Decimal(borrowed),
        leveraged_flag=Decimal(borrowed) > Decimal("0"),
        borrow_against_avant_flag=Decimal(borrowed) > Decimal("0") and deployed_dec > 0,
        leverage_ratio=(Decimal(borrowed) / deployed_dec) if deployed_dec > 0 else None,
        health_factor_min=Decimal("1.5") if Decimal(borrowed) > 0 else None,
        risk_band=risk_band,
        protocol_count=1 if deployed_dec > 0 else 0,
        market_count=1 if deployed_dec > 0 else 0,
        chain_count=1,
        behavior_tags_json=[],
        whale_rank_by_assets=None,
        whale_rank_by_borrow=None,
        total_avant_usd_delta_7d=None,
        borrowed_usd_delta_7d=None,
        avant_collateral_usd_delta_7d=None,
        wallet_staked_usd_usd=Decimal("0"),
        wallet_staked_eth_usd=Decimal("0"),
        wallet_staked_btc_usd=Decimal("0"),
        deployed_staked_usd_usd=Decimal(total_staked),
        deployed_staked_eth_usd=Decimal("0"),
        deployed_staked_btc_usd=Decimal("0"),
    )


def test_holder_scorecard_engine_persists_scorecard_and_protocol_gaps(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 9)
    prior_business_date = date(2026, 3, 2)
    as_of_ts_utc = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        morpho = Protocol(protocol_code="morpho")
        euler = Protocol(protocol_code="euler")
        session.add_all([chain, morpho, euler])
        session.flush()

        token = Token(
            chain_id=chain.chain_id,
            address_or_mint="0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            symbol="USDC",
            decimals=6,
        )
        session.add(token)
        session.flush()

        markets = [
            Market(
                chain_id=chain.chain_id,
                protocol_id=morpho.protocol_id,
                native_market_key="morpho-usdc",
                market_address="0x1000000000000000000000000000000000000001",
                market_kind="consumer_market",
                display_name="Morpho USDC",
                base_asset_token_id=token.token_id,
                collateral_token_id=token.token_id,
            ),
            Market(
                chain_id=chain.chain_id,
                protocol_id=euler.protocol_id,
                native_market_key="euler-usdc",
                market_address="0x2000000000000000000000000000000000000002",
                market_kind="consumer_market",
                display_name="Euler USDC",
                base_asset_token_id=token.token_id,
                collateral_token_id=token.token_id,
            ),
        ]
        session.add_all(markets)
        session.flush()

        wallets = [
            Wallet(address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", wallet_type="customer"),
            Wallet(address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", wallet_type="customer"),
            Wallet(address="0xcccccccccccccccccccccccccccccccccccccccc", wallet_type="customer"),
            Wallet(address="0xdddddddddddddddddddddddddddddddddddddddd", wallet_type="customer"),
            Wallet(address="0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee", wallet_type="customer"),
        ]
        session.add_all(wallets)
        session.flush()

        session.add_all(
            [
                ConsumerCohortDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    verified_total_avant_usd=Decimal("150"),
                    discovery_sources_json={"sources": ["prior_cohort"]},
                    is_signoff_eligible=True,
                    exclusion_reason=None,
                ),
                ConsumerCohortDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[1].wallet_id,
                    wallet_address=wallets[1].address,
                    verified_total_avant_usd=Decimal("70"),
                    discovery_sources_json={"sources": ["routescan"]},
                    is_signoff_eligible=True,
                    exclusion_reason=None,
                ),
                ConsumerCohortDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    verified_total_avant_usd=Decimal("50"),
                    discovery_sources_json={"sources": ["legacy_seed"]},
                    is_signoff_eligible=True,
                    exclusion_reason=None,
                ),
                ConsumerCohortDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[4].wallet_id,
                    wallet_address=wallets[4].address,
                    verified_total_avant_usd=Decimal("90"),
                    discovery_sources_json={"sources": ["legacy_seed"]},
                    is_signoff_eligible=False,
                    exclusion_reason="price_missing",
                ),
            ]
        )
        session.add_all(
            [
                _holder_row(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    signoff=True,
                    wallet_held="1500000",
                    deployed="1000000",
                    borrowed="500000",
                    total_base="1500000",
                    total_staked="1000000",
                    total_boosted="0",
                    risk_band="watch",
                ),
                _holder_row(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[1].wallet_id,
                    wallet_address=wallets[1].address,
                    signoff=True,
                    wallet_held="700000",
                    deployed="800000",
                    borrowed="400000",
                    total_base="700000",
                    total_staked="0",
                    total_boosted="800000",
                    risk_band="elevated",
                ),
                _holder_row(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    signoff=True,
                    wallet_held="500000",
                    deployed="0",
                    borrowed="0",
                    total_base="500000",
                    total_staked="0",
                    total_boosted="0",
                ),
                _holder_row(
                    business_date=prior_business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    signoff=True,
                    wallet_held="1400000",
                    deployed="600000",
                    borrowed="200000",
                    total_base="1400000",
                    total_staked="600000",
                    total_boosted="0",
                ),
                _holder_row(
                    business_date=prior_business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[3].wallet_id,
                    wallet_address=wallets[3].address,
                    signoff=True,
                    wallet_held="600000",
                    deployed="500000",
                    borrowed="100000",
                    total_base="1100000",
                    total_staked="0",
                    total_boosted="0",
                ),
            ]
        )
        session.add_all(
            [
                ConsumerMarketDemandDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    market_id=markets[0].market_id,
                    protocol_code="morpho",
                    chain_code="ethereum",
                    collateral_family="usd",
                    holder_count=2,
                    collateral_wallet_count=2,
                    leveraged_wallet_count=2,
                    avant_collateral_usd=Decimal("180"),
                    borrowed_usd=Decimal("90"),
                    idle_eligible_same_chain_usd=Decimal("220"),
                    p50_leverage_ratio=Decimal("0.5"),
                    p90_leverage_ratio=Decimal("0.5"),
                    top10_collateral_share=Decimal("1"),
                    utilization=Decimal("0.9"),
                    available_liquidity_usd=Decimal("20"),
                    cap_headroom_usd=Decimal("10"),
                    capacity_pressure_score=3,
                    needs_capacity_review=True,
                    near_limit_wallet_count=0,
                    avant_collateral_usd_delta_7d=Decimal("40"),
                    collateral_wallet_count_delta_7d=1,
                ),
                ConsumerMarketDemandDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    market_id=markets[1].market_id,
                    protocol_code="euler",
                    chain_code="ethereum",
                    collateral_family="usd",
                    holder_count=1,
                    collateral_wallet_count=1,
                    leveraged_wallet_count=0,
                    avant_collateral_usd=Decimal("0"),
                    borrowed_usd=Decimal("0"),
                    idle_eligible_same_chain_usd=Decimal("50"),
                    p50_leverage_ratio=None,
                    p90_leverage_ratio=None,
                    top10_collateral_share=None,
                    utilization=Decimal("0.2"),
                    available_liquidity_usd=Decimal("100"),
                    cap_headroom_usd=Decimal("80"),
                    capacity_pressure_score=0,
                    needs_capacity_review=False,
                    near_limit_wallet_count=0,
                    avant_collateral_usd_delta_7d=Decimal("0"),
                    collateral_wallet_count_delta_7d=0,
                ),
            ]
        )
        session.add_all(
            [
                ConsumerDebankWalletDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    in_seed_set=True,
                    in_verified_cohort=True,
                    in_signoff_cohort=True,
                    seed_sources_json=["alpha"],
                    discovery_sources_json=["prior_cohort"],
                    fetch_succeeded=True,
                    fetch_error_message=None,
                    has_any_activity=True,
                    has_any_borrow=True,
                    has_configured_surface_activity=True,
                    protocol_count=2,
                    chain_count=1,
                    configured_protocol_count=1,
                    total_supply_usd=Decimal("400"),
                    total_borrow_usd=Decimal("150"),
                    configured_surface_supply_usd=Decimal("200"),
                    configured_surface_borrow_usd=Decimal("50"),
                ),
                ConsumerDebankWalletDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[1].wallet_id,
                    wallet_address=wallets[1].address,
                    in_seed_set=True,
                    in_verified_cohort=True,
                    in_signoff_cohort=True,
                    seed_sources_json=["beta"],
                    discovery_sources_json=["routescan"],
                    fetch_succeeded=True,
                    fetch_error_message=None,
                    has_any_activity=True,
                    has_any_borrow=True,
                    has_configured_surface_activity=False,
                    protocol_count=1,
                    chain_count=1,
                    configured_protocol_count=0,
                    total_supply_usd=Decimal("250"),
                    total_borrow_usd=Decimal("80"),
                    configured_surface_supply_usd=Decimal("0"),
                    configured_surface_borrow_usd=Decimal("0"),
                ),
                ConsumerDebankWalletDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[2].wallet_id,
                    wallet_address=wallets[2].address,
                    in_seed_set=True,
                    in_verified_cohort=True,
                    in_signoff_cohort=True,
                    seed_sources_json=["gamma"],
                    discovery_sources_json=["legacy_seed"],
                    fetch_succeeded=True,
                    fetch_error_message=None,
                    has_any_activity=False,
                    has_any_borrow=False,
                    has_configured_surface_activity=False,
                    protocol_count=0,
                    chain_count=1,
                    configured_protocol_count=0,
                    total_supply_usd=Decimal("0"),
                    total_borrow_usd=Decimal("0"),
                    configured_surface_supply_usd=Decimal("0"),
                    configured_surface_borrow_usd=Decimal("0"),
                ),
            ]
        )
        session.add_all(
            [
                ConsumerDebankProtocolDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    chain_code="ethereum",
                    protocol_code="aave_v3",
                    in_config_surface=False,
                    supply_usd=Decimal("300"),
                    borrow_usd=Decimal("100"),
                ),
                ConsumerDebankProtocolDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[1].wallet_id,
                    wallet_address=wallets[1].address,
                    chain_code="ethereum",
                    protocol_code="aave_v3",
                    in_config_surface=False,
                    supply_usd=Decimal("250"),
                    borrow_usd=Decimal("80"),
                ),
                ConsumerDebankProtocolDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallets[0].wallet_id,
                    wallet_address=wallets[0].address,
                    chain_code="ethereum",
                    protocol_code="morpho",
                    in_config_surface=True,
                    supply_usd=Decimal("200"),
                    borrow_usd=Decimal("50"),
                ),
            ]
        )
        session.commit()

    thresholds = load_consumer_thresholds_config("config/consumer_thresholds.yaml")
    avant_tokens = load_avant_tokens_config("config/avant_tokens.yaml")
    with Session(engine) as session:
        summary = HolderScorecardEngine(
            session,
            thresholds=thresholds,
            avant_tokens=avant_tokens,
        ).compute_daily(business_date=business_date)
        session.commit()

        assert summary.scorecard_rows_written == 1
        assert summary.protocol_gap_rows_written == 2

        scorecard_row = session.get(HolderScorecardDaily, business_date)
        assert scorecard_row is not None
        assert scorecard_row.wallet_held_avant_usd == Decimal("2700000")
        assert scorecard_row.configured_deployed_avant_usd == Decimal("1800000")
        assert scorecard_row.total_canonical_avant_exposure_usd == Decimal("4500000")
        assert scorecard_row.base_share == Decimal("0.6000000000")
        assert scorecard_row.staked_share == Decimal("0.2222222222")
        assert scorecard_row.boosted_share == Decimal("0.1777777778")
        assert scorecard_row.top10_holder_share == Decimal("1")
        assert scorecard_row.configured_collateral_users_pct == Decimal("0.6666666667")
        assert scorecard_row.configured_leveraged_pct == Decimal("0.6666666667")
        assert scorecard_row.whale_enter_count_7d == 1
        assert scorecard_row.whale_exit_count_7d == 1
        assert scorecard_row.whale_borrow_up_count_7d == 1
        assert scorecard_row.whale_collateral_up_count_7d == 1
        assert scorecard_row.markets_needing_capacity_review == 1
        assert scorecard_row.dq_verified_holder_pct == Decimal("0.75")
        assert scorecard_row.visibility_gap_wallet_count == 1

        protocol_gaps = session.scalars(
            select(HolderProtocolGapDaily)
            .where(HolderProtocolGapDaily.business_date == business_date)
            .order_by(HolderProtocolGapDaily.gap_priority.asc())
        ).all()
        assert [row.protocol_code for row in protocol_gaps] == ["aave_v3", "morpho"]
        assert protocol_gaps[0].signoff_wallet_count == 2
        assert protocol_gaps[0].total_borrow_usd == Decimal("180")
        assert protocol_gaps[0].in_config_surface is False
        assert protocol_gaps[1].signoff_wallet_count == 1
        assert protocol_gaps[1].in_config_surface is True


def test_holder_scorecard_uses_observed_wrapper_mix_for_shares(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 9)
    as_of_ts_utc = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)

    with Session(engine) as session:
        wallet = Wallet(
            address="0xffffffffffffffffffffffffffffffffffffffff", wallet_type="customer"
        )
        session.add(wallet)
        session.flush()
        session.add(
            ConsumerCohortDaily(
                business_date=business_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                verified_total_avant_usd=Decimal("600000"),
                discovery_sources_json={"sources": ["debank_avasset_activity"]},
                is_signoff_eligible=True,
                exclusion_reason=None,
            )
        )
        session.add(
            _holder_row(
                business_date=business_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                signoff=True,
                wallet_held="100000",
                deployed="0",
                borrowed="0",
                total_base="100000",
                total_staked="0",
                total_boosted="0",
            )
        )
        session.add_all(
            [
                ConsumerDebankWalletDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallet.wallet_id,
                    wallet_address=wallet.address,
                    in_seed_set=True,
                    in_verified_cohort=True,
                    in_signoff_cohort=True,
                    seed_sources_json=["legacy_seed"],
                    discovery_sources_json=["debank_avasset_activity"],
                    fetch_succeeded=True,
                    fetch_error_message=None,
                    has_any_activity=True,
                    has_any_borrow=False,
                    has_configured_surface_activity=False,
                    protocol_count=1,
                    chain_count=1,
                    configured_protocol_count=0,
                    total_supply_usd=Decimal("500000"),
                    total_borrow_usd=Decimal("0"),
                    configured_surface_supply_usd=Decimal("0"),
                    configured_surface_borrow_usd=Decimal("0"),
                ),
                ConsumerDebankTokenDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallet.wallet_id,
                    wallet_address=wallet.address,
                    chain_code="ethereum",
                    protocol_code="yearn",
                    token_symbol="savUSD",
                    leg_type="supply",
                    in_config_surface=False,
                    usd_value=Decimal("300000"),
                ),
                ConsumerDebankTokenDaily(
                    business_date=business_date,
                    as_of_ts_utc=as_of_ts_utc,
                    wallet_id=wallet.wallet_id,
                    wallet_address=wallet.address,
                    chain_code="ethereum",
                    protocol_code="yearn",
                    token_symbol="wbravUSDC",
                    leg_type="supply",
                    in_config_surface=False,
                    usd_value=Decimal("200000"),
                ),
            ]
        )
        session.commit()

    thresholds = load_consumer_thresholds_config("config/consumer_thresholds.yaml")
    avant_tokens = load_avant_tokens_config("config/avant_tokens.yaml")
    with Session(engine) as session:
        summary = HolderScorecardEngine(
            session,
            thresholds=thresholds,
            avant_tokens=avant_tokens,
        ).compute_daily(
            business_date=business_date,
            write_protocol_gaps=False,
        )
        session.commit()

        assert summary.scorecard_rows_written == 1
        scorecard_row = session.get(HolderScorecardDaily, business_date)
        assert scorecard_row is not None
        assert scorecard_row.base_share == Decimal("0.1666666667")
        assert scorecard_row.staked_share == Decimal("0.5000000000")
        assert scorecard_row.boosted_share == Decimal("0.3333333333")


def test_holder_scorecard_uses_fallback_debank_date_for_gap_metrics(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 9)
    debank_date = date(2026, 3, 8)
    as_of_ts_utc = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)

    with Session(engine) as session:
        wallet = Wallet(
            address="0xabababababababababababababababababababab", wallet_type="customer"
        )
        session.add(wallet)
        session.flush()

        session.add(
            ConsumerCohortDaily(
                business_date=business_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                verified_total_avant_usd=Decimal("200000"),
                discovery_sources_json={"sources": ["routescan"]},
                is_signoff_eligible=True,
                exclusion_reason=None,
            )
        )
        session.add(
            _holder_row(
                business_date=business_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                signoff=True,
                wallet_held="200000",
                deployed="0",
                borrowed="0",
                total_base="200000",
                total_staked="0",
                total_boosted="0",
            )
        )
        session.add(
            ConsumerDebankWalletDaily(
                business_date=debank_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                in_seed_set=True,
                in_verified_cohort=True,
                in_signoff_cohort=True,
                seed_sources_json=["routescan"],
                discovery_sources_json=["routescan"],
                fetch_succeeded=True,
                fetch_error_message=None,
                has_any_activity=True,
                has_any_borrow=True,
                has_configured_surface_activity=False,
                protocol_count=1,
                chain_count=1,
                configured_protocol_count=0,
                total_supply_usd=Decimal("125000"),
                total_borrow_usd=Decimal("25000"),
                configured_surface_supply_usd=Decimal("0"),
                configured_surface_borrow_usd=Decimal("0"),
            )
        )
        session.add(
            ConsumerDebankProtocolDaily(
                business_date=debank_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                chain_code="ethereum",
                protocol_code="aave_v3",
                in_config_surface=False,
                supply_usd=Decimal("125000"),
                borrow_usd=Decimal("25000"),
            )
        )
        session.commit()

    thresholds = load_consumer_thresholds_config("config/consumer_thresholds.yaml")
    avant_tokens = load_avant_tokens_config("config/avant_tokens.yaml")
    with Session(engine) as session:
        summary = HolderScorecardEngine(
            session,
            thresholds=thresholds,
            avant_tokens=avant_tokens,
        ).compute_daily(business_date=business_date)
        session.commit()

        assert summary.scorecard_rows_written == 1
        assert summary.protocol_gap_rows_written == 1

        scorecard_row = session.get(HolderScorecardDaily, business_date)
        assert scorecard_row is not None
        assert scorecard_row.visibility_gap_wallet_count == 1

        protocol_gap = session.scalar(
            select(HolderProtocolGapDaily).where(
                HolderProtocolGapDaily.business_date == business_date,
                HolderProtocolGapDaily.protocol_code == "aave_v3",
            )
        )
        assert protocol_gap is not None
        assert protocol_gap.signoff_wallet_count == 1
        assert protocol_gap.total_borrow_usd == Decimal("25000")
