"""Consumer holder intelligence API tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from core.db.models import (
    ConsumerDebankTokenDaily,
    ConsumerDebankWalletDaily,
    ConsumerMarketDemandDaily,
    ConsumerTokenHolderDaily,
    HolderBehaviorDaily,
    HolderProductSegmentDaily,
    HolderProtocolDeployDaily,
    HolderProtocolGapDaily,
    HolderWalletProductDaily,
    Market,
    Wallet,
)
from tests.api.conftest import SeedMetadata

ZERO = Decimal("0")


def _api_session(client: TestClient) -> Session:
    override = next(iter(client.app.dependency_overrides.values()))
    return override.__closure__[0].cell_contents


def _seed_holder_dashboard_rows(session: Session, meta: SeedMetadata) -> None:
    business_date = meta.business_date
    as_of = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)
    wallets = session.query(Wallet).order_by(Wallet.wallet_id.asc()).limit(12).all()
    market = session.query(Market).order_by(Market.market_id.asc()).first()
    assert len(wallets) >= 11
    assert market is not None

    session.query(HolderProductSegmentDaily).filter(
        HolderProductSegmentDaily.business_date == business_date
    ).delete()
    session.query(HolderProtocolDeployDaily).filter(
        HolderProtocolDeployDaily.business_date == business_date
    ).delete()
    session.query(HolderWalletProductDaily).filter(
        HolderWalletProductDaily.business_date == business_date
    ).delete()
    session.query(HolderBehaviorDaily).filter(
        HolderBehaviorDaily.business_date == business_date
    ).delete()
    session.query(ConsumerTokenHolderDaily).filter(
        ConsumerTokenHolderDaily.business_date == business_date
    ).delete()
    session.query(ConsumerDebankTokenDaily).filter(
        ConsumerDebankTokenDaily.business_date == business_date
    ).delete()
    session.query(ConsumerDebankWalletDaily).filter(
        ConsumerDebankWalletDaily.business_date == business_date
    ).delete()
    session.query(HolderProtocolGapDaily).filter(
        HolderProtocolGapDaily.business_date == business_date
    ).delete()
    session.query(ConsumerMarketDemandDaily).filter(
        ConsumerMarketDemandDaily.business_date == business_date
    ).delete()

    segment_rows = [
        # all products
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of,
            "product_scope": "all",
            "cohort_segment": "verified",
            "holder_count": 8,
            "defi_active_wallet_count": 2,
            "avasset_deployed_wallet_count": 1,
            "conviction_gap_wallet_count": 1,
            "collateralized_wallet_count": 0,
            "borrowed_against_wallet_count": 0,
            "multi_asset_wallet_count": 1,
            "observed_aum_usd": Decimal("120000"),
            "avg_holding_usd": Decimal("15000"),
            "median_age_days": 9,
            "idle_pct": Decimal("0.72"),
            "fixed_yield_pt_pct": Decimal("0.08"),
            "collateralized_pct": Decimal("0.00"),
            "borrowed_against_pct": Decimal("0.00"),
            "staked_pct": Decimal("0.10"),
            "defi_active_pct": Decimal("0.25"),
            "avasset_deployed_pct": Decimal("0.125"),
            "conviction_gap_pct": Decimal("0.125"),
            "multi_asset_pct": Decimal("0.125"),
            "aum_change_7d_pct": Decimal("0.10"),
            "new_wallet_count_7d": 2,
            "exited_wallet_count_7d": 1,
            "idle_usd": Decimal("86400"),
            "fixed_yield_pt_usd": Decimal("9600"),
            "yield_token_yt_usd": Decimal("0"),
            "collateralized_usd": Decimal("0"),
            "borrowed_usd": Decimal("0"),
            "staked_usd": Decimal("12000"),
            "other_defi_usd": Decimal("24000"),
        },
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of,
            "product_scope": "all",
            "cohort_segment": "core",
            "holder_count": 2,
            "defi_active_wallet_count": 2,
            "avasset_deployed_wallet_count": 2,
            "conviction_gap_wallet_count": 0,
            "collateralized_wallet_count": 2,
            "borrowed_against_wallet_count": 1,
            "multi_asset_wallet_count": 1,
            "observed_aum_usd": Decimal("1400000"),
            "avg_holding_usd": Decimal("700000"),
            "median_age_days": 84,
            "idle_pct": Decimal("0.31"),
            "fixed_yield_pt_pct": Decimal("0.12"),
            "collateralized_pct": Decimal("0.44"),
            "borrowed_against_pct": Decimal("0.50"),
            "staked_pct": Decimal("0.18"),
            "defi_active_pct": Decimal("1"),
            "avasset_deployed_pct": Decimal("1"),
            "conviction_gap_pct": Decimal("0"),
            "multi_asset_pct": Decimal("0.50"),
            "aum_change_7d_pct": Decimal("0.18"),
            "new_wallet_count_7d": 1,
            "exited_wallet_count_7d": 0,
            "idle_usd": Decimal("434000"),
            "fixed_yield_pt_usd": Decimal("168000"),
            "yield_token_yt_usd": Decimal("12000"),
            "collateralized_usd": Decimal("616000"),
            "borrowed_usd": Decimal("220000"),
            "staked_usd": Decimal("252000"),
            "other_defi_usd": Decimal("170000"),
        },
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of,
            "product_scope": "all",
            "cohort_segment": "whale",
            "holder_count": 1,
            "defi_active_wallet_count": 1,
            "avasset_deployed_wallet_count": 1,
            "conviction_gap_wallet_count": 0,
            "collateralized_wallet_count": 1,
            "borrowed_against_wallet_count": 1,
            "multi_asset_wallet_count": 1,
            "observed_aum_usd": Decimal("2400000"),
            "avg_holding_usd": Decimal("2400000"),
            "median_age_days": 132,
            "idle_pct": Decimal("0.22"),
            "fixed_yield_pt_pct": Decimal("0.14"),
            "collateralized_pct": Decimal("0.52"),
            "borrowed_against_pct": Decimal("1"),
            "staked_pct": Decimal("0.08"),
            "defi_active_pct": Decimal("1"),
            "avasset_deployed_pct": Decimal("1"),
            "conviction_gap_pct": Decimal("0"),
            "multi_asset_pct": Decimal("1"),
            "aum_change_7d_pct": Decimal("0.06"),
            "new_wallet_count_7d": 0,
            "exited_wallet_count_7d": 0,
            "idle_usd": Decimal("528000"),
            "fixed_yield_pt_usd": Decimal("336000"),
            "yield_token_yt_usd": Decimal("42000"),
            "collateralized_usd": Decimal("1248000"),
            "borrowed_usd": Decimal("640000"),
            "staked_usd": Decimal("192000"),
            "other_defi_usd": Decimal("246000"),
        },
    ]
    for row in list(segment_rows):
        segment_rows.append(
            {
                **row,
                "product_scope": "avusd",
                "observed_aum_usd": row["observed_aum_usd"],
            }
        )
    segment_rows.append(
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of,
            "product_scope": "all",
            "cohort_segment": "all",
            "holder_count": 11,
            "defi_active_wallet_count": 5,
            "avasset_deployed_wallet_count": 4,
            "conviction_gap_wallet_count": 1,
            "collateralized_wallet_count": 3,
            "borrowed_against_wallet_count": 2,
            "multi_asset_wallet_count": 3,
            "observed_aum_usd": Decimal("3920000"),
            "avg_holding_usd": Decimal("356363.636363636363"),
            "median_age_days": 42,
            "idle_pct": Decimal("0.27"),
            "fixed_yield_pt_pct": Decimal("0.13"),
            "collateralized_pct": Decimal("0.47"),
            "borrowed_against_pct": Decimal("0.18"),
            "staked_pct": Decimal("0.11"),
            "defi_active_pct": Decimal("0.4545454545"),
            "avasset_deployed_pct": Decimal("0.3636363636"),
            "conviction_gap_pct": Decimal("0.0909090909"),
            "multi_asset_pct": Decimal("0.2727272727"),
            "aum_change_7d_pct": Decimal("0.11"),
            "new_wallet_count_7d": 3,
            "exited_wallet_count_7d": 1,
            "idle_usd": Decimal("1048400"),
            "fixed_yield_pt_usd": Decimal("513600"),
            "yield_token_yt_usd": Decimal("54000"),
            "collateralized_usd": Decimal("1864000"),
            "borrowed_usd": Decimal("860000"),
            "staked_usd": Decimal("456000"),
            "other_defi_usd": Decimal("440000"),
        }
    )
    segment_rows.append(
        {
            **segment_rows[-1],
            "product_scope": "avusd",
        }
    )
    session.add_all(HolderProductSegmentDaily(**row) for row in segment_rows)

    wallet_product_rows = [
        HolderWalletProductDaily(
            business_date=business_date,
            as_of_ts_utc=as_of,
            wallet_id=wallets[0].wallet_id,
            wallet_address=wallets[0].address,
            product_scope="all",
            monitored_presence_usd=Decimal("2602000"),
            observed_exposure_usd=Decimal("2602000"),
            wallet_held_usd=Decimal("2280000"),
            canonical_deployed_usd=Decimal("980000"),
            external_fixed_yield_pt_usd=Decimal("280000"),
            external_yield_token_yt_usd=Decimal("42000"),
            external_other_defi_usd=ZERO,
            has_any_defi_activity=True,
            has_any_defi_borrow=True,
            has_canonical_activity=True,
            segment="whale",
            is_attributed=True,
            asset_symbols_json=["savUSD", "PT-savUSD-14MAY2026", "YT-savUSD-14MAY2026"],
            borrowed_usd=Decimal("640000"),
            leverage_ratio=Decimal("0.6530612245"),
            health_factor_min=Decimal("1.31"),
            risk_band="watch",
            age_days=132,
            multi_asset_flag=True,
            aum_delta_7d_usd=Decimal("180000"),
            aum_delta_7d_pct=Decimal("0.0749375520"),
        ),
        HolderWalletProductDaily(
            business_date=business_date,
            as_of_ts_utc=as_of,
            wallet_id=wallets[1].wallet_id,
            wallet_address=wallets[1].address,
            product_scope="all",
            monitored_presence_usd=Decimal("750000"),
            observed_exposure_usd=Decimal("750000"),
            wallet_held_usd=Decimal("300000"),
            canonical_deployed_usd=Decimal("280000"),
            external_fixed_yield_pt_usd=ZERO,
            external_yield_token_yt_usd=ZERO,
            external_other_defi_usd=Decimal("170000"),
            has_any_defi_activity=True,
            has_any_defi_borrow=False,
            has_canonical_activity=True,
            segment="core",
            is_attributed=True,
            asset_symbols_json=["savUSD"],
            borrowed_usd=Decimal("120000"),
            leverage_ratio=Decimal("0.4285714285"),
            health_factor_min=Decimal("1.56"),
            risk_band="normal",
            age_days=84,
            multi_asset_flag=False,
            aum_delta_7d_usd=Decimal("42000"),
            aum_delta_7d_pct=Decimal("0.0592374491"),
        ),
        HolderWalletProductDaily(
            business_date=business_date,
            as_of_ts_utc=as_of,
            wallet_id=wallets[2].wallet_id,
            wallet_address=wallets[2].address,
            product_scope="all",
            monitored_presence_usd=Decimal("22000"),
            observed_exposure_usd=Decimal("22000"),
            wallet_held_usd=Decimal("22000"),
            canonical_deployed_usd=ZERO,
            external_fixed_yield_pt_usd=ZERO,
            external_yield_token_yt_usd=ZERO,
            external_other_defi_usd=ZERO,
            has_any_defi_activity=False,
            has_any_defi_borrow=False,
            has_canonical_activity=False,
            segment="verified",
            is_attributed=True,
            asset_symbols_json=["savUSD"],
            borrowed_usd=ZERO,
            leverage_ratio=None,
            health_factor_min=None,
            risk_band="normal",
            age_days=9,
            multi_asset_flag=False,
            aum_delta_7d_usd=Decimal("2000"),
            aum_delta_7d_pct=Decimal("0.10"),
        ),
    ]
    for wallet in wallets[3:10]:
        wallet_product_rows.append(
            HolderWalletProductDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                product_scope="all",
                monitored_presence_usd=Decimal("15000"),
                observed_exposure_usd=Decimal("15000"),
                wallet_held_usd=Decimal("15000"),
                canonical_deployed_usd=ZERO,
                external_fixed_yield_pt_usd=ZERO,
                external_yield_token_yt_usd=ZERO,
                external_other_defi_usd=ZERO,
                has_any_defi_activity=False,
                has_any_defi_borrow=False,
                has_canonical_activity=False,
                segment="verified",
                is_attributed=True,
                asset_symbols_json=["savUSD"],
                borrowed_usd=ZERO,
                leverage_ratio=None,
                health_factor_min=None,
                risk_band="normal",
                age_days=9,
                multi_asset_flag=False,
                aum_delta_7d_usd=Decimal("1000"),
                aum_delta_7d_pct=Decimal("0.0714285714"),
            )
        )
    wallet_product_rows.append(
        HolderWalletProductDaily(
            business_date=business_date,
            as_of_ts_utc=as_of,
            wallet_id=wallets[10].wallet_id,
            wallet_address=wallets[10].address,
            product_scope="all",
            monitored_presence_usd=Decimal("700000"),
            observed_exposure_usd=Decimal("700000"),
            wallet_held_usd=Decimal("434000"),
            canonical_deployed_usd=Decimal("616000"),
            external_fixed_yield_pt_usd=Decimal("168000"),
            external_yield_token_yt_usd=Decimal("12000"),
            external_other_defi_usd=Decimal("170000"),
            has_any_defi_activity=True,
            has_any_defi_borrow=False,
            has_canonical_activity=True,
            segment="core",
            is_attributed=True,
            asset_symbols_json=["savUSD", "PT-savUSD-14MAY2026"],
            borrowed_usd=Decimal("220000"),
            leverage_ratio=Decimal("0.50"),
            health_factor_min=Decimal("1.44"),
            risk_band="normal",
            age_days=84,
            multi_asset_flag=True,
            aum_delta_7d_usd=Decimal("55000"),
            aum_delta_7d_pct=Decimal("0.0852713178"),
        )
    )
    for row in list(wallet_product_rows):
        if row.product_scope == "all":
            wallet_product_rows.append(
                HolderWalletProductDaily(
                    business_date=row.business_date,
                    as_of_ts_utc=row.as_of_ts_utc,
                    wallet_id=row.wallet_id,
                    wallet_address=row.wallet_address,
                    product_scope="avusd",
                    monitored_presence_usd=row.monitored_presence_usd,
                    observed_exposure_usd=row.observed_exposure_usd,
                    wallet_held_usd=row.wallet_held_usd,
                    canonical_deployed_usd=row.canonical_deployed_usd,
                    external_fixed_yield_pt_usd=row.external_fixed_yield_pt_usd,
                    external_yield_token_yt_usd=row.external_yield_token_yt_usd,
                    external_other_defi_usd=row.external_other_defi_usd,
                    has_any_defi_activity=row.has_any_defi_activity,
                    has_any_defi_borrow=row.has_any_defi_borrow,
                    has_canonical_activity=row.has_canonical_activity,
                    segment=row.segment,
                    is_attributed=row.is_attributed,
                    asset_symbols_json=row.asset_symbols_json,
                    borrowed_usd=row.borrowed_usd,
                    leverage_ratio=row.leverage_ratio,
                    health_factor_min=row.health_factor_min,
                    risk_band=row.risk_band,
                    age_days=row.age_days,
                    multi_asset_flag=row.multi_asset_flag,
                    aum_delta_7d_usd=row.aum_delta_7d_usd,
                    aum_delta_7d_pct=row.aum_delta_7d_pct,
                )
            )
    session.add_all(wallet_product_rows)

    token_holder_rows = []
    for wallet in wallets[:11]:
        token_holder_rows.append(
            ConsumerTokenHolderDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                chain_code="avalanche",
                token_symbol="savUSD",
                token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                balance_tokens=Decimal("1000"),
                usd_value=Decimal("1150"),
                holder_class="customer",
                exclude_from_monitoring=False,
                exclude_from_customer_float=False,
                source_provider="routescan",
            )
        )
    for wallet in wallets[:3]:
        token_holder_rows.append(
            ConsumerTokenHolderDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                chain_code="avalanche",
                token_symbol="savUSD",
                token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
                balance_tokens=Decimal("500"),
                usd_value=Decimal("575"),
                holder_class="protocol",
                exclude_from_monitoring=True,
                exclude_from_customer_float=False,
                source_provider="routescan",
            )
        )
    session.add_all(token_holder_rows)

    session.add_all(
        [
            HolderProtocolDeployDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                product_scope="all",
                protocol_code="pendle",
                chain_code="ethereum",
                verified_wallet_count=1,
                core_wallet_count=1,
                whale_wallet_count=1,
                total_value_usd=Decimal("720000"),
                total_borrow_usd=Decimal("0"),
                dominant_token_symbols_json=["PT-savUSD-14MAY2026", "YT-savUSD-14MAY2026"],
                primary_use="fixed_yield",
            ),
            HolderProtocolDeployDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                product_scope="avusd",
                protocol_code="pendle",
                chain_code="ethereum",
                verified_wallet_count=1,
                core_wallet_count=1,
                whale_wallet_count=1,
                total_value_usd=Decimal("720000"),
                total_borrow_usd=Decimal("0"),
                dominant_token_symbols_json=["PT-savUSD-14MAY2026", "YT-savUSD-14MAY2026"],
                primary_use="fixed_yield",
            ),
            HolderProtocolDeployDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                product_scope="all",
                protocol_code="gearbox",
                chain_code="ethereum",
                verified_wallet_count=0,
                core_wallet_count=1,
                whale_wallet_count=0,
                total_value_usd=Decimal("410000"),
                total_borrow_usd=Decimal("160000"),
                dominant_token_symbols_json=["savUSD"],
                primary_use="collateral",
            ),
        ]
    )

    session.add_all(
        [
            HolderBehaviorDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[0].wallet_id,
                wallet_address=wallets[0].address,
                is_signoff_eligible=True,
                verified_total_avant_usd=Decimal("1300000"),
                wallet_held_avant_usd=Decimal("520000"),
                configured_deployed_avant_usd=Decimal("980000"),
                total_canonical_avant_exposure_usd=Decimal("1500000"),
                wallet_family_usd_usd=Decimal("1300000"),
                wallet_family_btc_usd=ZERO,
                wallet_family_eth_usd=ZERO,
                deployed_family_usd_usd=Decimal("980000"),
                deployed_family_btc_usd=ZERO,
                deployed_family_eth_usd=ZERO,
                total_family_usd_usd=Decimal("2280000"),
                total_family_btc_usd=ZERO,
                total_family_eth_usd=ZERO,
                family_usd_usd=Decimal("2280000"),
                family_btc_usd=ZERO,
                family_eth_usd=ZERO,
                wallet_base_usd=Decimal("0"),
                wallet_staked_usd=Decimal("1300000"),
                wallet_boosted_usd=ZERO,
                deployed_base_usd=ZERO,
                deployed_staked_usd=Decimal("980000"),
                deployed_boosted_usd=ZERO,
                total_base_usd=ZERO,
                total_staked_usd=Decimal("2280000"),
                total_boosted_usd=ZERO,
                base_usd=ZERO,
                staked_usd=Decimal("2280000"),
                boosted_usd=ZERO,
                family_count=1,
                wrapper_count=1,
                multi_asset_flag=False,
                multi_wrapper_flag=False,
                idle_avant_usd=Decimal("520000"),
                idle_eligible_same_chain_usd=Decimal("520000"),
                avant_collateral_usd=Decimal("980000"),
                borrowed_usd=Decimal("640000"),
                leveraged_flag=True,
                borrow_against_avant_flag=True,
                leverage_ratio=Decimal("0.6530612245"),
                health_factor_min=Decimal("1.31"),
                risk_band="watch",
                protocol_count=2,
                market_count=2,
                chain_count=1,
                behavior_tags_json=["staker", "multi_market_user"],
                whale_rank_by_assets=1,
                whale_rank_by_borrow=1,
                total_avant_usd_delta_7d=Decimal("180000"),
                borrowed_usd_delta_7d=Decimal("24000"),
                avant_collateral_usd_delta_7d=Decimal("70000"),
            ),
            HolderBehaviorDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[1].wallet_id,
                wallet_address=wallets[1].address,
                is_signoff_eligible=True,
                verified_total_avant_usd=Decimal("480000"),
                wallet_held_avant_usd=Decimal("300000"),
                configured_deployed_avant_usd=Decimal("280000"),
                total_canonical_avant_exposure_usd=Decimal("580000"),
                wallet_family_usd_usd=Decimal("580000"),
                wallet_family_btc_usd=ZERO,
                wallet_family_eth_usd=ZERO,
                deployed_family_usd_usd=Decimal("280000"),
                deployed_family_btc_usd=ZERO,
                deployed_family_eth_usd=ZERO,
                total_family_usd_usd=Decimal("860000"),
                total_family_btc_usd=ZERO,
                total_family_eth_usd=ZERO,
                family_usd_usd=Decimal("860000"),
                family_btc_usd=ZERO,
                family_eth_usd=ZERO,
                wallet_base_usd=ZERO,
                wallet_staked_usd=Decimal("300000"),
                wallet_boosted_usd=ZERO,
                deployed_base_usd=ZERO,
                deployed_staked_usd=Decimal("280000"),
                deployed_boosted_usd=ZERO,
                total_base_usd=ZERO,
                total_staked_usd=Decimal("580000"),
                total_boosted_usd=ZERO,
                base_usd=ZERO,
                staked_usd=Decimal("580000"),
                boosted_usd=ZERO,
                family_count=1,
                wrapper_count=1,
                multi_asset_flag=False,
                multi_wrapper_flag=False,
                idle_avant_usd=Decimal("300000"),
                idle_eligible_same_chain_usd=Decimal("300000"),
                avant_collateral_usd=Decimal("280000"),
                borrowed_usd=Decimal("120000"),
                leveraged_flag=True,
                borrow_against_avant_flag=True,
                leverage_ratio=Decimal("0.4285714285"),
                health_factor_min=Decimal("1.56"),
                risk_band="normal",
                protocol_count=1,
                market_count=1,
                chain_count=1,
                behavior_tags_json=["staker"],
                whale_rank_by_assets=2,
                whale_rank_by_borrow=2,
                total_avant_usd_delta_7d=Decimal("42000"),
                borrowed_usd_delta_7d=Decimal("8000"),
                avant_collateral_usd_delta_7d=Decimal("22000"),
            ),
            HolderBehaviorDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[2].wallet_id,
                wallet_address=wallets[2].address,
                is_signoff_eligible=True,
                verified_total_avant_usd=Decimal("22000"),
                wallet_held_avant_usd=Decimal("22000"),
                configured_deployed_avant_usd=ZERO,
                total_canonical_avant_exposure_usd=Decimal("22000"),
                wallet_family_usd_usd=Decimal("22000"),
                wallet_family_btc_usd=ZERO,
                wallet_family_eth_usd=ZERO,
                deployed_family_usd_usd=ZERO,
                deployed_family_btc_usd=ZERO,
                deployed_family_eth_usd=ZERO,
                total_family_usd_usd=Decimal("22000"),
                total_family_btc_usd=ZERO,
                total_family_eth_usd=ZERO,
                family_usd_usd=Decimal("22000"),
                family_btc_usd=ZERO,
                family_eth_usd=ZERO,
                wallet_base_usd=ZERO,
                wallet_staked_usd=Decimal("22000"),
                wallet_boosted_usd=ZERO,
                deployed_base_usd=ZERO,
                deployed_staked_usd=ZERO,
                deployed_boosted_usd=ZERO,
                total_base_usd=ZERO,
                total_staked_usd=Decimal("22000"),
                total_boosted_usd=ZERO,
                base_usd=ZERO,
                staked_usd=Decimal("22000"),
                boosted_usd=ZERO,
                family_count=1,
                wrapper_count=1,
                multi_asset_flag=False,
                multi_wrapper_flag=False,
                idle_avant_usd=Decimal("22000"),
                idle_eligible_same_chain_usd=Decimal("22000"),
                avant_collateral_usd=ZERO,
                borrowed_usd=ZERO,
                leveraged_flag=False,
                borrow_against_avant_flag=False,
                leverage_ratio=None,
                health_factor_min=None,
                risk_band="normal",
                protocol_count=0,
                market_count=0,
                chain_count=1,
                behavior_tags_json=["idle_whale"],
                whale_rank_by_assets=None,
                whale_rank_by_borrow=None,
                total_avant_usd_delta_7d=Decimal("2000"),
                borrowed_usd_delta_7d=ZERO,
                avant_collateral_usd_delta_7d=ZERO,
            ),
        ]
    )

    session.add_all(
        [
            ConsumerDebankTokenDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[0].wallet_id,
                wallet_address=wallets[0].address,
                chain_code="ethereum",
                protocol_code="pendle",
                token_symbol="PT-savUSD-14MAY2026",
                leg_type="supply",
                in_config_surface=False,
                usd_value=Decimal("280000"),
            ),
            ConsumerDebankTokenDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[0].wallet_id,
                wallet_address=wallets[0].address,
                chain_code="ethereum",
                protocol_code="pendle",
                token_symbol="YT-savUSD-14MAY2026",
                leg_type="supply",
                in_config_surface=False,
                usd_value=Decimal("42000"),
            ),
            ConsumerDebankTokenDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[1].wallet_id,
                wallet_address=wallets[1].address,
                chain_code="ethereum",
                protocol_code="gearbox",
                token_symbol="savUSD",
                leg_type="supply",
                in_config_surface=False,
                usd_value=Decimal("170000"),
            ),
        ]
    )

    session.add_all(
        [
            ConsumerDebankWalletDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[0].wallet_id,
                wallet_address=wallets[0].address,
                in_seed_set=True,
                in_verified_cohort=True,
                in_signoff_cohort=True,
                seed_sources_json=["seed"],
                discovery_sources_json=["routescan"],
                fetch_succeeded=True,
                has_any_activity=True,
                has_any_borrow=True,
                has_configured_surface_activity=True,
                protocol_count=2,
                chain_count=1,
                configured_protocol_count=1,
                total_supply_usd=Decimal("322000"),
                total_borrow_usd=Decimal("100000"),
                configured_surface_supply_usd=Decimal("100000"),
                configured_surface_borrow_usd=Decimal("50000"),
            ),
            ConsumerDebankWalletDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[1].wallet_id,
                wallet_address=wallets[1].address,
                in_seed_set=True,
                in_verified_cohort=True,
                in_signoff_cohort=True,
                seed_sources_json=["seed"],
                discovery_sources_json=["routescan"],
                fetch_succeeded=True,
                has_any_activity=True,
                has_any_borrow=False,
                has_configured_surface_activity=False,
                protocol_count=1,
                chain_count=1,
                configured_protocol_count=0,
                total_supply_usd=Decimal("170000"),
                total_borrow_usd=ZERO,
                configured_surface_supply_usd=ZERO,
                configured_surface_borrow_usd=ZERO,
            ),
            ConsumerDebankWalletDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                wallet_id=wallets[2].wallet_id,
                wallet_address=wallets[2].address,
                in_seed_set=False,
                in_verified_cohort=True,
                in_signoff_cohort=True,
                seed_sources_json=None,
                discovery_sources_json=["routescan"],
                fetch_succeeded=True,
                has_any_activity=False,
                has_any_borrow=False,
                has_configured_surface_activity=False,
                protocol_count=0,
                chain_count=0,
                configured_protocol_count=0,
                total_supply_usd=ZERO,
                total_borrow_usd=ZERO,
                configured_surface_supply_usd=ZERO,
                configured_surface_borrow_usd=ZERO,
            ),
        ]
    )

    session.add_all(
        [
            HolderProtocolGapDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                protocol_code="pendle",
                wallet_count=2,
                signoff_wallet_count=2,
                total_supply_usd=Decimal("322000"),
                total_borrow_usd=ZERO,
                in_config_surface=False,
                gap_priority=1,
            ),
            HolderProtocolGapDaily(
                business_date=business_date,
                as_of_ts_utc=as_of,
                protocol_code="gearbox",
                wallet_count=1,
                signoff_wallet_count=1,
                total_supply_usd=Decimal("170000"),
                total_borrow_usd=Decimal("160000"),
                in_config_surface=False,
                gap_priority=2,
            ),
        ]
    )

    session.add(
        ConsumerMarketDemandDaily(
            business_date=business_date,
            as_of_ts_utc=as_of,
            market_id=market.market_id,
            protocol_code="morpho",
            chain_code="ethereum",
            collateral_family="usd",
            holder_count=3,
            collateral_wallet_count=2,
            leveraged_wallet_count=1,
            avant_collateral_usd=Decimal("1260000"),
            borrowed_usd=Decimal("760000"),
            idle_eligible_same_chain_usd=Decimal("520000"),
            p50_leverage_ratio=Decimal("0.64"),
            p90_leverage_ratio=Decimal("0.91"),
            top10_collateral_share=Decimal("0.67"),
            utilization=Decimal("0.88"),
            available_liquidity_usd=Decimal("32000"),
            cap_headroom_usd=Decimal("28000"),
            capacity_pressure_score=4,
            needs_capacity_review=True,
            near_limit_wallet_count=1,
            avant_collateral_usd_delta_7d=Decimal("120000"),
            collateral_wallet_count_delta_7d=1,
        )
    )
    session.commit()


def test_consumer_summary_supports_product_scope(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    session = _api_session(client)
    _seed_holder_dashboard_rows(session, meta)

    response = client.get("/consumer/summary", params={"product": "avusd"})
    assert response.status_code == 200
    payload = response.json()

    assert payload["business_date"] == str(meta.business_date)
    assert payload["product"] == "avusd"
    assert payload["product_label"] == "avUSD"
    assert payload["kpis"]["monitored_holder_count"] == 11
    assert payload["kpis"]["attributed_holder_count"] == 11
    assert payload["kpis"]["verified_holder_count"] == 8
    assert payload["kpis"]["core_holder_count"] == 2
    assert payload["kpis"]["whale_holder_count"] == 1
    assert payload["coverage"]["raw_holder_rows"] == 14
    assert payload["coverage"]["excluded_holder_rows"] == 3
    assert [cohort["segment"] for cohort in payload["cohorts"]] == ["verified", "core", "whale"]
    assert payload["cohorts"][0]["holder_count"] == 8
    assert payload["cohorts"][1]["holder_count"] == 2
    assert payload["cohorts"][2]["holder_count"] == 1


def test_consumer_behavior_and_funnel_endpoints_return_segment_views(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    session = _api_session(client)
    _seed_holder_dashboard_rows(session, meta)

    behavior = client.get("/consumer/behavior-comparison", params={"product": "all"})
    assert behavior.status_code == 200
    rows = behavior.json()["rows"]
    assert [row["segment"] for row in rows] == ["verified", "core", "whale"]
    assert rows[0]["holder_count"] == 8
    assert Decimal(rows[1]["conviction_gap_pct"]) == ZERO

    funnel = client.get("/consumer/adoption-funnel", params={"product": "all"})
    assert funnel.status_code == 200
    cohorts = funnel.json()["cohorts"]
    assert len(cohorts) == 3
    assert cohorts[1]["segment"] == "core"
    assert cohorts[1]["holder_count"] == 2
    assert cohorts[1]["defi_active_holder_count"] == 2


def test_consumer_deployments_and_top_wallets_return_new_contract(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    session = _api_session(client)
    _seed_holder_dashboard_rows(session, meta)

    deployments = client.get("/consumer/deployments", params={"product": "all"})
    assert deployments.status_code == 200
    deploy_payload = deployments.json()
    assert deploy_payload["product"] == "all"
    assert deploy_payload["total_deployed_value_usd"] == "1130000.000000000000000000"
    assert deploy_payload["deployments"][0]["protocol_code"] == "pendle"
    assert deploy_payload["deployments"][0]["primary_use"] == "fixed_yield"

    top_wallets = client.get(
        "/consumer/top-wallets",
        params={"product": "all", "rank": "assets", "limit": 2},
    )
    assert top_wallets.status_code == 200
    wallet_rows = top_wallets.json()["wallets"]
    assert len(wallet_rows) == 2
    assert wallet_rows[0]["segment"] == "whale"
    assert wallet_rows[0]["deployment_state"] in {"Borrowed", "Collateralized", "Deployed"}
    assert "PT-savUSD-14MAY2026" in wallet_rows[0]["asset_symbols"]
    assert wallet_rows[0]["total_value_usd"] == "2602000.000000000000000000"
    assert wallet_rows[0]["external_deployed_usd"] == "322000.000000000000000000"


def test_consumer_risk_signals_and_visibility_endpoints(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    session = _api_session(client)
    _seed_holder_dashboard_rows(session, meta)

    risk = client.get("/consumer/risk-signals", params={"product": "all"})
    assert risk.status_code == 200
    risk_payload = risk.json()
    assert len(risk_payload["capacity_signals"]) == 1
    assert len(risk_payload["cohort_profiles"]) == 2
    assert risk_payload["cohort_profiles"][0]["segment"] == "verified"

    visibility = client.get("/consumer/visibility/summary")
    assert visibility.status_code == 200
    visibility_payload = visibility.json()
    assert visibility_payload["visibility_wallets"] == 3
    assert visibility_payload["visibility_gap_wallet_count"] == 1

    protocol_gaps = client.get("/consumer/visibility/protocol-gaps")
    assert protocol_gaps.status_code == 200
    protocol_rows = protocol_gaps.json()["protocols"]
    assert protocol_rows[0]["protocol_code"] == "pendle"


def test_consumer_capacity_endpoint_still_serves_market_rows(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, meta = api_client
    session = _api_session(client)
    _seed_holder_dashboard_rows(session, meta)

    response = client.get("/consumer/markets/capacity")
    assert response.status_code == 200
    payload = response.json()
    assert payload["total_count"] == 1
    assert payload["markets"][0]["capacity_pressure_score"] == 4


def test_consumer_summary_empty_state_returns_zeroed_contract(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client

    response = client.get("/consumer/summary", params={"product": "all"})
    assert response.status_code == 200
    payload = response.json()

    assert payload["product"] == "all"
    assert payload["kpis"]["monitored_holder_count"] == 0
    assert payload["cohorts"] == []
