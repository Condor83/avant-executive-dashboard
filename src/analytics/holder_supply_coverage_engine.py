"""Daily supply-coverage rollup for the configured holder scorecard token."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal

from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import AvantTokensConfig, ConsumerThresholdsConfig
from core.customer_cohort import DEFAULT_CONSUMER_CHAIN_IDS, active_avant_tokens
from core.db.models import (
    ConsumerCohortDaily,
    ConsumerDebankTokenDaily,
    ConsumerTokenHolderDaily,
    HolderSupplyCoverageDaily,
)
from core.holder_supply import HolderSupplyTarget

ZERO = Decimal("0")
SENIOR_TOKEN_SYMBOLS = {"savUSD", "savETH", "savBTC"}


@dataclass(frozen=True)
class HolderSupplyCoverageBuildSummary:
    business_date: date
    rows_written: int


def _share(numerator: Decimal, denominator: Decimal) -> Decimal | None:
    if denominator <= ZERO:
        return None
    return numerator / denominator


def resolve_supply_coverage_targets(
    *,
    avant_tokens: AvantTokensConfig,
    business_date: date,
) -> list[HolderSupplyTarget]:
    """Resolve all active senior-product supply-coverage targets from the token registry."""

    chain_scope = set(DEFAULT_CONSUMER_CHAIN_IDS)
    targets: list[HolderSupplyTarget] = []
    for token in active_avant_tokens(
        avant_tokens,
        business_date=business_date,
        chain_scope=chain_scope,
    ):
        if token.symbol not in SENIOR_TOKEN_SYMBOLS:
            continue
        chain_id = DEFAULT_CONSUMER_CHAIN_IDS.get(token.chain_code)
        if chain_id is None:
            continue
        targets.append(
            HolderSupplyTarget(
                chain_code=token.chain_code,
                chain_id=chain_id,
                token_symbol=token.symbol,
                token_address=token.token_address,
                token_decimals=token.decimals,
            )
        )
    return sorted(targets, key=lambda target: (target.token_symbol, target.chain_code))


class HolderSupplyCoverageEngine:
    """Build a persisted supply-coverage row for the configured scorecard token."""

    def __init__(
        self,
        session: Session,
        *,
        avant_tokens: AvantTokensConfig,
        thresholds: ConsumerThresholdsConfig,
    ) -> None:
        self.session = session
        self.avant_tokens = avant_tokens
        self.thresholds = thresholds

    def _strategy_deployed_supply_usd(
        self,
        *,
        business_date: date,
        token_symbol: str,
    ) -> Decimal:
        result = self.session.execute(
            text(
                """
                with latest_positions as (
                    select distinct on (ps.position_key)
                        ps.snapshot_id,
                        ps.position_key,
                        ps.as_of_ts_utc,
                        ps.wallet_id,
                        ps.market_id,
                        ps.supplied_amount,
                        ps.supplied_usd,
                        ps.collateral_usd
                    from position_snapshots ps
                    where date(timezone('America/Denver', ps.as_of_ts_utc)) = :business_date
                    order by ps.position_key, ps.as_of_ts_utc desc, ps.snapshot_id desc
                ),
                strategy_positions as (
                    select
                        lp.snapshot_id,
                        lp.as_of_ts_utc,
                        m.market_kind,
                        lp.supplied_amount,
                        lp.supplied_usd,
                        lp.collateral_usd,
                        bt.symbol as base_symbol,
                        ct.symbol as collateral_symbol,
                        m.base_asset_token_id
                    from latest_positions lp
                    join wallets w on w.wallet_id = lp.wallet_id
                    join markets m on m.market_id = lp.market_id
                    left join tokens bt on bt.token_id = m.base_asset_token_id
                    left join tokens ct on ct.token_id = m.collateral_token_id
                    where w.wallet_type = 'strategy'
                      and (bt.symbol = :token_symbol or ct.symbol = :token_symbol)
                ),
                priced as (
                    select
                        sp.*,
                        pr.price_usd as base_price,
                        row_number() over (
                            partition by sp.snapshot_id
                            order by pr.ts_utc desc
                        ) as rn
                    from strategy_positions sp
                    left join prices pr
                        on pr.token_id = sp.base_asset_token_id
                       and pr.ts_utc <= sp.as_of_ts_utc
                )
                select coalesce(
                    sum(
                        case
                            when base_symbol = :token_symbol
                                 and market_kind <> 'wallet_balance_token'
                                then coalesce(supplied_usd, 0)
                            when collateral_symbol = :token_symbol
                                 and collateral_usd is not null
                                then collateral_usd
                            when collateral_symbol = :token_symbol
                                 and market_kind in ('consumer_market', 'market')
                                then coalesce(supplied_usd, 0)
                            when collateral_symbol = :token_symbol
                                 and market_kind in (
                                     'liquidity_book_pool',
                                     'concentrated_liquidity_pool'
                                 )
                                 and base_price is not null
                                then greatest(
                                    coalesce(supplied_usd, 0)
                                    - (coalesce(supplied_amount, 0) * coalesce(base_price, 0)),
                                    0
                                )
                            else 0
                        end
                    ),
                    0
                ) as strategy_deployed_supply_usd
                from priced
                where rn = 1
                """
            ),
            {
                "business_date": business_date,
                "token_symbol": token_symbol,
            },
        ).scalar_one()
        return result if isinstance(result, Decimal) else Decimal(str(result))

    def compute_daily(self, *, business_date: date) -> HolderSupplyCoverageBuildSummary:
        cohort_wallet_ids = {
            row.wallet_id
            for row in self.session.scalars(
                select(ConsumerCohortDaily).where(
                    ConsumerCohortDaily.business_date == business_date
                )
            ).all()
        }
        signoff_wallet_ids = {
            row.wallet_id
            for row in self.session.scalars(
                select(ConsumerCohortDaily).where(
                    ConsumerCohortDaily.business_date == business_date,
                    ConsumerCohortDaily.is_signoff_eligible.is_(True),
                )
            ).all()
        }

        rows_written = 0
        for target in resolve_supply_coverage_targets(
            avant_tokens=self.avant_tokens,
            business_date=business_date,
        ):
            holder_rows = self.session.scalars(
                select(ConsumerTokenHolderDaily).where(
                    ConsumerTokenHolderDaily.business_date == business_date,
                    ConsumerTokenHolderDaily.chain_code == target.chain_code,
                    ConsumerTokenHolderDaily.token_symbol == target.token_symbol,
                )
            ).all()
            self.session.execute(
                delete(HolderSupplyCoverageDaily).where(
                    HolderSupplyCoverageDaily.business_date == business_date,
                    HolderSupplyCoverageDaily.chain_code == target.chain_code,
                    HolderSupplyCoverageDaily.token_symbol == target.token_symbol,
                )
            )
            if not holder_rows:
                continue

            debank_rows = self.session.scalars(
                select(ConsumerDebankTokenDaily).where(
                    ConsumerDebankTokenDaily.business_date == business_date,
                    ConsumerDebankTokenDaily.token_symbol == target.token_symbol,
                )
            ).all()

            raw_holder_wallet_count = len(holder_rows)
            monitoring_rows = [row for row in holder_rows if not row.exclude_from_monitoring]
            monitoring_wallet_ids = {row.wallet_id for row in monitoring_rows}
            core_ids_for_target = monitoring_wallet_ids & cohort_wallet_ids
            signoff_ids_for_target = monitoring_wallet_ids & signoff_wallet_ids

            gross_supply_usd = sum((row.usd_value for row in holder_rows), ZERO)
            strategy_supply_usd = sum(
                (row.usd_value for row in holder_rows if row.holder_class == "strategy"),
                ZERO,
            )
            strategy_deployed_supply_usd = self._strategy_deployed_supply_usd(
                business_date=business_date,
                token_symbol=target.token_symbol,
            )
            internal_supply_usd = sum(
                (row.usd_value for row in holder_rows if row.holder_class == "internal"),
                ZERO,
            )
            explicit_excluded_supply_usd = sum(
                (
                    row.usd_value
                    for row in holder_rows
                    if row.exclude_from_customer_float
                    and row.holder_class not in {"strategy", "internal"}
                ),
                ZERO,
            )
            net_customer_float_usd = max(
                gross_supply_usd
                - strategy_supply_usd
                - strategy_deployed_supply_usd
                - internal_supply_usd
                - explicit_excluded_supply_usd,
                ZERO,
            )
            direct_holder_supply_usd = sum((row.usd_value for row in monitoring_rows), ZERO)
            core_direct_holder_supply_usd = sum(
                (row.usd_value for row in monitoring_rows if row.wallet_id in core_ids_for_target),
                ZERO,
            )
            signoff_direct_holder_supply_usd = sum(
                (
                    row.usd_value
                    for row in monitoring_rows
                    if row.wallet_id in signoff_ids_for_target
                ),
                ZERO,
            )

            same_chain_deployed_supply_usd = ZERO
            cross_chain_supply_usd = ZERO
            core_same_chain_deployed_supply_usd = ZERO
            signoff_same_chain_deployed_supply_usd = ZERO
            core_cross_chain_supply_usd = ZERO
            signoff_cross_chain_supply_usd = ZERO
            wallets_with_same_chain_deployed_supply: set[int] = set()
            wallets_with_cross_chain_supply: set[int] = set()
            for row in debank_rows:
                if row.leg_type == "borrow":
                    continue
                if row.chain_code == target.chain_code:
                    same_chain_deployed_supply_usd += row.usd_value
                    wallets_with_same_chain_deployed_supply.add(row.wallet_id)
                    if row.wallet_id in core_ids_for_target:
                        core_same_chain_deployed_supply_usd += row.usd_value
                    if row.wallet_id in signoff_ids_for_target:
                        signoff_same_chain_deployed_supply_usd += row.usd_value
                else:
                    cross_chain_supply_usd += row.usd_value
                    wallets_with_cross_chain_supply.add(row.wallet_id)
                    if row.wallet_id in core_ids_for_target:
                        core_cross_chain_supply_usd += row.usd_value
                    if row.wallet_id in signoff_ids_for_target:
                        signoff_cross_chain_supply_usd += row.usd_value

            covered_supply_usd = (
                direct_holder_supply_usd + same_chain_deployed_supply_usd + cross_chain_supply_usd
            )
            core_covered_supply_usd = (
                core_direct_holder_supply_usd
                + core_same_chain_deployed_supply_usd
                + core_cross_chain_supply_usd
            )
            signoff_covered_supply_usd = (
                signoff_direct_holder_supply_usd
                + signoff_same_chain_deployed_supply_usd
                + signoff_cross_chain_supply_usd
            )
            as_of_ts_utc = max(
                [row.as_of_ts_utc for row in holder_rows]
                + [row.as_of_ts_utc for row in debank_rows]
            )

            row = {
                "business_date": business_date,
                "as_of_ts_utc": as_of_ts_utc,
                "chain_code": target.chain_code,
                "token_symbol": target.token_symbol,
                "token_address": target.token_address,
                "raw_holder_wallet_count": raw_holder_wallet_count,
                "monitoring_wallet_count": len(monitoring_wallet_ids),
                "core_wallet_count": len(core_ids_for_target),
                "signoff_wallet_count": len(signoff_ids_for_target),
                "wallets_with_same_chain_deployed_supply": len(
                    wallets_with_same_chain_deployed_supply
                ),
                "wallets_with_cross_chain_supply": len(wallets_with_cross_chain_supply),
                "gross_supply_usd": gross_supply_usd,
                "strategy_supply_usd": strategy_supply_usd,
                "strategy_deployed_supply_usd": strategy_deployed_supply_usd,
                "internal_supply_usd": internal_supply_usd,
                "explicit_excluded_supply_usd": explicit_excluded_supply_usd,
                "net_customer_float_usd": net_customer_float_usd,
                "direct_holder_supply_usd": direct_holder_supply_usd,
                "core_direct_holder_supply_usd": core_direct_holder_supply_usd,
                "signoff_direct_holder_supply_usd": signoff_direct_holder_supply_usd,
                "same_chain_deployed_supply_usd": same_chain_deployed_supply_usd,
                "cross_chain_supply_usd": cross_chain_supply_usd,
                "core_same_chain_deployed_supply_usd": core_same_chain_deployed_supply_usd,
                "signoff_same_chain_deployed_supply_usd": signoff_same_chain_deployed_supply_usd,
                "covered_supply_usd": covered_supply_usd,
                "core_covered_supply_usd": core_covered_supply_usd,
                "signoff_covered_supply_usd": signoff_covered_supply_usd,
                "covered_supply_pct": _share(covered_supply_usd, net_customer_float_usd),
                "core_covered_supply_pct": _share(core_covered_supply_usd, net_customer_float_usd),
                "signoff_covered_supply_pct": _share(
                    signoff_covered_supply_usd,
                    net_customer_float_usd,
                ),
            }
            self.session.execute(insert(HolderSupplyCoverageDaily).values(row))
            rows_written += 1

        return HolderSupplyCoverageBuildSummary(
            business_date=business_date,
            rows_written=rows_written,
        )
