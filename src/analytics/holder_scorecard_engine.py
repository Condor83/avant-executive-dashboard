"""Persisted CEO-grade holder scorecard and protocol gap rollups."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from analytics.holder_dashboard_engine import _resolve_debank_date, build_holder_dashboard_context
from core.config import AvantTokensConfig, ConsumerThresholdsConfig
from core.consumer_debank_visibility import is_excluded_visibility_protocol
from core.db.models import (
    ConsumerCohortDaily,
    ConsumerDebankProtocolDaily,
    ConsumerDebankWalletDaily,
    ConsumerMarketDemandDaily,
    HolderBehaviorDaily,
    HolderProtocolGapDaily,
    HolderScorecardDaily,
)

ZERO = Decimal("0")


@dataclass(frozen=True)
class HolderScorecardBuildSummary:
    business_date: date
    scorecard_rows_written: int
    protocol_gap_rows_written: int


def _pct(count: int, denominator: int) -> Decimal | None:
    if denominator <= 0:
        return None
    return Decimal(count) / Decimal(denominator)


def _share(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator <= ZERO:
        return None
    return numerator / denominator


class HolderScorecardEngine:
    """Build persisted CEO-grade holder summary tables from stored analytics."""

    def __init__(
        self,
        session: Session,
        *,
        thresholds: ConsumerThresholdsConfig,
        avant_tokens: AvantTokensConfig | None = None,
    ) -> None:
        self.session = session
        self.thresholds = thresholds
        self.avant_tokens = avant_tokens

    def compute_daily(
        self,
        *,
        business_date: date,
        write_protocol_gaps: bool = True,
    ) -> HolderScorecardBuildSummary:
        holder_rows = self.session.scalars(
            select(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
        ).all()
        if write_protocol_gaps:
            self.session.execute(
                delete(HolderProtocolGapDaily).where(
                    HolderProtocolGapDaily.business_date == business_date
                )
            )
        self.session.execute(
            delete(HolderScorecardDaily).where(HolderScorecardDaily.business_date == business_date)
        )
        if not holder_rows:
            return HolderScorecardBuildSummary(
                business_date=business_date,
                scorecard_rows_written=0,
                protocol_gap_rows_written=0,
            )
        dashboard_context = (
            build_holder_dashboard_context(
                session=self.session,
                business_date=business_date,
                avant_tokens=self.avant_tokens,
                thresholds=self.thresholds,
            )
            if self.avant_tokens is not None
            else None
        )
        wallet_metrics_by_id = (
            {wallet.wallet_id: wallet for wallet in dashboard_context.wallets}
            if dashboard_context is not None
            else {}
        )

        cohort_count = (
            self.session.scalar(
                select(func.count())
                .select_from(ConsumerCohortDaily)
                .where(ConsumerCohortDaily.business_date == business_date)
            )
            or 0
        )
        signoff_wallet_ids = {
            int(row.wallet_id)
            for row in self.session.scalars(
                select(ConsumerCohortDaily).where(
                    ConsumerCohortDaily.business_date == business_date,
                    ConsumerCohortDaily.is_signoff_eligible.is_(True),
                )
            ).all()
        }
        signoff_rows = [row for row in holder_rows if row.wallet_id in signoff_wallet_ids]
        signoff_wallet_metrics = [
            wallet_metrics_by_id[wallet_id]
            for wallet_id in sorted(signoff_wallet_ids)
            if wallet_id in wallet_metrics_by_id
        ]
        tracked_holders = (
            len(signoff_wallet_metrics) if dashboard_context is not None else len(signoff_rows)
        )
        markets_needing_capacity_review = (
            self.session.scalar(
                select(func.count())
                .select_from(ConsumerMarketDemandDaily)
                .where(
                    ConsumerMarketDemandDaily.business_date == business_date,
                    ConsumerMarketDemandDaily.needs_capacity_review.is_(True),
                )
            )
            or 0
        )

        debank_date = _resolve_debank_date(self.session, business_date)
        visibility_rows = (
            self.session.scalars(
                select(ConsumerDebankWalletDaily).where(
                    ConsumerDebankWalletDaily.business_date == debank_date
                )
            ).all()
            if debank_date is not None
            else []
        )
        visibility_gap_wallet_count = sum(
            1
            for row in visibility_rows
            if row.has_any_activity and not row.has_configured_surface_activity
        )
        as_of_timestamps = [row.as_of_ts_utc for row in holder_rows]
        as_of_timestamps.extend(row.as_of_ts_utc for row in visibility_rows)
        if dashboard_context is not None and dashboard_context.as_of_ts_utc is not None:
            as_of_timestamps.append(dashboard_context.as_of_ts_utc)
        as_of_ts_utc = max(as_of_timestamps)

        total_wallet_held = sum((row.wallet_held_avant_usd for row in signoff_rows), ZERO)
        total_configured_deployed = sum(
            (row.configured_deployed_avant_usd for row in signoff_rows),
            ZERO,
        )
        total_canonical = sum(
            (row.total_canonical_avant_exposure_usd for row in signoff_rows),
            ZERO,
        )
        if dashboard_context is not None:
            total_observed = sum(
                (wallet.observed_by_scope.get("all", ZERO) for wallet in signoff_wallet_metrics),
                ZERO,
            )
            total_base = sum(
                (wallet.base_by_scope.get("all", ZERO) for wallet in signoff_wallet_metrics),
                ZERO,
            )
            total_staked = sum(
                (wallet.staked_by_scope.get("all", ZERO) for wallet in signoff_wallet_metrics),
                ZERO,
            )
            total_boosted = sum(
                (wallet.boosted_by_scope.get("all", ZERO) for wallet in signoff_wallet_metrics),
                ZERO,
            )
            ranked_signoff_wallets = sorted(
                signoff_wallet_metrics,
                key=lambda wallet: (
                    -wallet.observed_by_scope.get("all", ZERO),
                    wallet.wallet_address,
                ),
            )
        else:
            total_observed = total_canonical
            total_base = sum((row.total_base_usd for row in signoff_rows), ZERO)
            total_staked = sum((row.total_staked_usd for row in signoff_rows), ZERO)
            total_boosted = sum((row.total_boosted_usd for row in signoff_rows), ZERO)
            ranked_signoff_wallets = sorted(
                signoff_rows,
                key=lambda row: (-row.total_canonical_avant_exposure_usd, row.wallet_address),
            )

        def concentration_share(limit: int) -> Decimal | None:
            return _share(
                sum(
                    (
                        (
                            wallet.observed_by_scope.get("all", ZERO)
                            if dashboard_context is not None
                            else wallet.total_canonical_avant_exposure_usd
                        )
                        for wallet in ranked_signoff_wallets[:limit]
                    ),
                    ZERO,
                ),
                total_observed,
            )

        prior_rows = self.session.scalars(
            select(HolderBehaviorDaily).where(
                HolderBehaviorDaily.business_date == business_date - timedelta(days=7)
            )
        ).all()
        whale_threshold = self.thresholds.whales.wallet_usd_threshold
        if self.avant_tokens is not None:
            prior_dashboard_context = build_holder_dashboard_context(
                session=self.session,
                business_date=business_date - timedelta(days=7),
                avant_tokens=self.avant_tokens,
                thresholds=self.thresholds,
            )
            current_whales = [
                wallet
                for wallet in (dashboard_context.wallets if dashboard_context is not None else [])
                if wallet.observed_by_scope.get("all", ZERO) >= whale_threshold
            ]
            current_whales.sort(
                key=lambda wallet: (
                    -wallet.observed_by_scope.get("all", ZERO),
                    wallet.wallet_address,
                )
            )
            prior_whales = [
                wallet
                for wallet in prior_dashboard_context.wallets
                if wallet.observed_by_scope.get("all", ZERO) >= whale_threshold
            ]
            prior_whales.sort(
                key=lambda wallet: (
                    -wallet.observed_by_scope.get("all", ZERO),
                    wallet.wallet_address,
                )
            )
            current_whale_ids = {wallet.wallet_id for wallet in current_whales}
            prior_whale_ids = {wallet.wallet_id for wallet in prior_whales}
        else:
            current_whales = [
                row
                for row in holder_rows
                if row.total_canonical_avant_exposure_usd >= whale_threshold
            ]
            current_whales.sort(
                key=lambda row: (-row.total_canonical_avant_exposure_usd, row.wallet_address)
            )
            prior_whales = [
                row
                for row in prior_rows
                if row.total_canonical_avant_exposure_usd >= whale_threshold
            ]
            prior_whales.sort(
                key=lambda row: (-row.total_canonical_avant_exposure_usd, row.wallet_address)
            )
            current_whale_ids = {row.wallet_id for row in current_whales}
            prior_whale_ids = {row.wallet_id for row in prior_whales}
        prior_by_wallet = {row.wallet_id: row for row in prior_rows}
        current_by_wallet = {row.wallet_id: row for row in holder_rows}
        whale_borrow_up_count = 0
        whale_collateral_up_count = 0
        for wallet in current_whales:
            current_row = current_by_wallet.get(wallet.wallet_id)
            prior = prior_by_wallet.get(wallet.wallet_id)
            if current_row is None or prior is None:
                continue
            if current_row.borrowed_usd > prior.borrowed_usd:
                whale_borrow_up_count += 1
            if current_row.configured_deployed_avant_usd > prior.configured_deployed_avant_usd:
                whale_collateral_up_count += 1

        if dashboard_context is not None:
            single_asset_count = sum(
                1
                for wallet in signoff_wallet_metrics
                if sum(
                    1
                    for scope in ("avusd", "aveth", "avbtc")
                    if wallet.observed_by_scope.get(scope, ZERO)
                    >= self.thresholds.classification_dust_floor_usd
                )
                <= 1
            )
            multi_asset_count = sum(
                1
                for wallet in signoff_wallet_metrics
                if sum(
                    1
                    for scope in ("avusd", "aveth", "avbtc")
                    if wallet.observed_by_scope.get(scope, ZERO)
                    >= self.thresholds.classification_dust_floor_usd
                )
                >= 2
            )
            single_wrapper_count = sum(
                1
                for wallet in signoff_wallet_metrics
                if sum(
                    1
                    for value in (
                        wallet.base_by_scope.get("all", ZERO),
                        wallet.staked_by_scope.get("all", ZERO),
                        wallet.boosted_by_scope.get("all", ZERO),
                    )
                    if value >= self.thresholds.classification_dust_floor_usd
                )
                <= 1
            )
            multi_wrapper_count = sum(
                1
                for wallet in signoff_wallet_metrics
                if sum(
                    1
                    for value in (
                        wallet.base_by_scope.get("all", ZERO),
                        wallet.staked_by_scope.get("all", ZERO),
                        wallet.boosted_by_scope.get("all", ZERO),
                    )
                    if value >= self.thresholds.classification_dust_floor_usd
                )
                >= 2
            )
        else:
            single_asset_count = sum(1 for row in signoff_rows if not row.multi_asset_flag)
            multi_asset_count = sum(1 for row in signoff_rows if row.multi_asset_flag)
            single_wrapper_count = sum(1 for row in signoff_rows if not row.multi_wrapper_flag)
            multi_wrapper_count = sum(1 for row in signoff_rows if row.multi_wrapper_flag)

        scorecard_row = {
            "business_date": business_date,
            "as_of_ts_utc": as_of_ts_utc,
            "tracked_holders": tracked_holders,
            "top10_holder_share": concentration_share(10),
            "top25_holder_share": concentration_share(25),
            "top100_holder_share": concentration_share(100),
            "wallet_held_avant_usd": total_wallet_held,
            "configured_deployed_avant_usd": total_configured_deployed,
            "total_canonical_avant_exposure_usd": total_canonical,
            "base_share": _share(total_base, total_observed),
            "staked_share": _share(total_staked, total_observed),
            "boosted_share": _share(total_boosted, total_observed),
            "single_asset_pct": _pct(single_asset_count, tracked_holders),
            "multi_asset_pct": _pct(multi_asset_count, tracked_holders),
            "single_wrapper_pct": _pct(single_wrapper_count, tracked_holders),
            "multi_wrapper_pct": _pct(multi_wrapper_count, tracked_holders),
            "configured_collateral_users_pct": _pct(
                sum(1 for row in signoff_rows if row.configured_deployed_avant_usd > ZERO),
                tracked_holders,
            ),
            "configured_leveraged_pct": _pct(
                sum(1 for row in signoff_rows if row.leveraged_flag),
                tracked_holders,
            ),
            "whale_enter_count_7d": len(current_whale_ids - prior_whale_ids),
            "whale_exit_count_7d": len(prior_whale_ids - current_whale_ids),
            "whale_borrow_up_count_7d": whale_borrow_up_count,
            "whale_collateral_up_count_7d": whale_collateral_up_count,
            "markets_needing_capacity_review": int(markets_needing_capacity_review),
            "dq_verified_holder_pct": _pct(tracked_holders, int(cohort_count)),
            "visibility_gap_wallet_count": visibility_gap_wallet_count,
        }
        self.session.execute(insert(HolderScorecardDaily).values(scorecard_row))

        protocol_rows = (
            self.session.scalars(
                select(ConsumerDebankProtocolDaily).where(
                    ConsumerDebankProtocolDaily.business_date == debank_date
                )
            ).all()
            if debank_date is not None
            else []
        )
        signoff_visibility_wallet_ids = {
            row.wallet_id for row in visibility_rows if row.wallet_id in signoff_wallet_ids
        }
        if not signoff_visibility_wallet_ids:
            signoff_visibility_wallet_ids = {row.wallet_id for row in signoff_rows}

        protocol_gap_rows: list[dict[str, object]] = []
        if write_protocol_gaps:
            protocol_totals: dict[str, dict[str, object]] = {}
            for row in protocol_rows:
                if is_excluded_visibility_protocol(row.protocol_code):
                    continue
                bucket = protocol_totals.setdefault(
                    row.protocol_code,
                    {
                        "wallet_ids": set(),
                        "signoff_wallet_ids": set(),
                        "total_supply_usd": ZERO,
                        "total_borrow_usd": ZERO,
                        "in_config_surface": False,
                    },
                )
                wallet_ids = bucket["wallet_ids"]
                assert isinstance(wallet_ids, set)
                wallet_ids.add(row.wallet_id)
                if row.wallet_id in signoff_visibility_wallet_ids:
                    signoff_wallet_ids = bucket["signoff_wallet_ids"]
                    assert isinstance(signoff_wallet_ids, set)
                    signoff_wallet_ids.add(row.wallet_id)
                bucket["total_supply_usd"] = (
                    Decimal(str(bucket["total_supply_usd"])) + row.supply_usd
                )
                bucket["total_borrow_usd"] = (
                    Decimal(str(bucket["total_borrow_usd"])) + row.borrow_usd
                )
                bucket["in_config_surface"] = bool(bucket["in_config_surface"]) or bool(
                    row.in_config_surface
                )

            ordered_protocols = sorted(
                protocol_totals.items(),
                key=lambda item: (
                    -len(item[1]["signoff_wallet_ids"]),
                    -len(item[1]["wallet_ids"]),
                    -Decimal(str(item[1]["total_borrow_usd"])),
                    -Decimal(str(item[1]["total_supply_usd"])),
                    item[0],
                ),
            )
            protocol_gap_rows = [
                {
                    "business_date": business_date,
                    "as_of_ts_utc": as_of_ts_utc,
                    "protocol_code": protocol_code,
                    "wallet_count": len(bucket["wallet_ids"]),
                    "signoff_wallet_count": len(bucket["signoff_wallet_ids"]),
                    "total_supply_usd": Decimal(str(bucket["total_supply_usd"])),
                    "total_borrow_usd": Decimal(str(bucket["total_borrow_usd"])),
                    "in_config_surface": bool(bucket["in_config_surface"]),
                    "gap_priority": index + 1,
                }
                for index, (protocol_code, bucket) in enumerate(ordered_protocols)
            ]
            if protocol_gap_rows:
                self.session.execute(insert(HolderProtocolGapDaily).values(protocol_gap_rows))

        return HolderScorecardBuildSummary(
            business_date=business_date,
            scorecard_rows_written=1,
            protocol_gap_rows_written=len(protocol_gap_rows),
        )
