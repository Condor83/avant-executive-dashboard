"""Daily customer capacity pressure rows for configured consumer markets."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, aliased

from core.config import AvantTokensConfig, ConsumerThresholdsConfig, canonical_address
from core.db.models import (
    Chain,
    ConsumerMarketDemandDaily,
    HolderBehaviorDaily,
    Market,
    MarketHealthDaily,
    MarketSnapshot,
    PositionSnapshot,
    Protocol,
    Token,
    Wallet,
)

ZERO = Decimal("0")
COLLATERAL_TOKEN = aliased(Token)


@dataclass(frozen=True)
class ConsumerCapacityBuildSummary:
    business_date: date
    rows_written: int


def _economic_collateral_usd(*, supplied_usd: Decimal, collateral_usd: Decimal | None) -> Decimal:
    if collateral_usd is not None and collateral_usd > ZERO:
        return collateral_usd
    return supplied_usd


def _percentile(values: list[Decimal], percentile: Decimal) -> Decimal | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    weight = Decimal(str(position - lower))
    return ordered[lower] + (ordered[upper] - ordered[lower]) * weight


class ConsumerCapacityEngine:
    """Build persisted customer market demand and capacity pressure rows."""

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

    def _family_by_chain_address(self, *, business_date: date) -> dict[tuple[str, str], str]:
        mapping: dict[tuple[str, str], str] = {}
        for token in self.avant_tokens.tokens:
            if token.active_from is not None and business_date < token.active_from:
                continue
            if token.active_to is not None and business_date > token.active_to:
                continue
            mapping[(token.chain_code, canonical_address(token.token_address))] = token.asset_family
        return mapping

    def compute_daily(self, *, business_date: date) -> ConsumerCapacityBuildSummary:
        holder_rows = self.session.scalars(
            select(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
        ).all()
        if not holder_rows:
            self.session.query(ConsumerMarketDemandDaily).filter(
                ConsumerMarketDemandDaily.business_date == business_date
            ).delete()
            return ConsumerCapacityBuildSummary(business_date=business_date, rows_written=0)

        as_of_ts_utc = max(row.as_of_ts_utc for row in holder_rows)
        signoff_wallet_ids = {
            int(row.wallet_id) for row in holder_rows if bool(row.is_signoff_eligible)
        }
        family_by_chain_address = self._family_by_chain_address(business_date=business_date)

        balance_rows = self.session.execute(
            select(
                PositionSnapshot.wallet_id,
                Chain.chain_code,
                Market.market_address,
                func.sum(PositionSnapshot.supplied_usd),
            )
            .join(Market, Market.market_id == PositionSnapshot.market_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
            .where(
                PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
                Wallet.wallet_type == "customer",
                Market.market_kind == "wallet_balance_token",
            )
            .group_by(PositionSnapshot.wallet_id, Chain.chain_code, Market.market_address)
        ).all()
        family_balance_by_wallet: dict[tuple[int, str, str], Decimal] = defaultdict(lambda: ZERO)
        for wallet_id, chain_code, market_address, total_usd in balance_rows:
            family = family_by_chain_address.get(
                (str(chain_code), canonical_address(str(market_address)))
            )
            if family is None:
                continue
            family_balance_by_wallet[(int(wallet_id), str(chain_code), family)] += Decimal(
                str(total_usd)
            )

        position_rows = self.session.execute(
            select(
                PositionSnapshot.wallet_id,
                PositionSnapshot.market_id,
                Chain.chain_code,
                Market.protocol_id,
                Market.display_name,
                COLLATERAL_TOKEN.address_or_mint,
                PositionSnapshot.supplied_usd,
                PositionSnapshot.collateral_usd,
                PositionSnapshot.borrowed_usd,
                PositionSnapshot.health_factor,
            )
            .join(Market, Market.market_id == PositionSnapshot.market_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
            .outerjoin(COLLATERAL_TOKEN, COLLATERAL_TOKEN.token_id == Market.collateral_token_id)
            .where(
                PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
                Wallet.wallet_type == "customer",
                Market.market_kind == "consumer_market",
            )
        ).all()

        usage_by_market: dict[int, list[dict[str, object]]] = defaultdict(list)
        for (
            wallet_id,
            market_id,
            chain_code,
            _protocol_id,
            _display_name,
            collateral_address,
            supplied_usd,
            collateral_usd,
            borrowed_usd,
            health_factor,
        ) in position_rows:
            family = family_by_chain_address.get(
                (str(chain_code), canonical_address(str(collateral_address or ""))),
                "unknown",
            )
            effective_collateral = _economic_collateral_usd(
                supplied_usd=Decimal(str(supplied_usd)),
                collateral_usd=(
                    Decimal(str(collateral_usd)) if collateral_usd is not None else None
                ),
            )
            usage_by_market[int(market_id)].append(
                {
                    "wallet_id": int(wallet_id),
                    "chain_code": str(chain_code),
                    "collateral_family": family,
                    "collateral_usd": effective_collateral,
                    "borrowed_usd": Decimal(str(borrowed_usd)),
                    "health_factor": Decimal(str(health_factor))
                    if health_factor is not None
                    else None,
                }
            )

        market_rows = self.session.execute(
            select(
                Market.market_id,
                Protocol.protocol_code,
                Chain.chain_code,
                Market.display_name,
                Market.market_address,
                COLLATERAL_TOKEN.address_or_mint,
            )
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .outerjoin(COLLATERAL_TOKEN, COLLATERAL_TOKEN.token_id == Market.collateral_token_id)
            .where(Market.market_kind == "consumer_market")
        ).all()
        market_health_rows = self.session.execute(
            select(
                MarketHealthDaily.market_id,
                MarketHealthDaily.utilization,
                MarketHealthDaily.available_liquidity_usd,
            ).where(MarketHealthDaily.business_date == business_date)
        ).all()
        health_by_market = {
            int(market_id): (
                Decimal(str(utilization)) if utilization is not None else None,
                Decimal(str(available_liquidity_usd))
                if available_liquidity_usd is not None
                else None,
            )
            for market_id, utilization, available_liquidity_usd in market_health_rows
        }
        snapshot_caps_rows = self.session.execute(
            select(
                MarketSnapshot.market_id,
                MarketSnapshot.total_supply_usd,
                MarketSnapshot.caps_json,
            ).where(MarketSnapshot.as_of_ts_utc == as_of_ts_utc)
        ).all()
        cap_headroom_by_market: dict[int, Decimal | None] = {}
        for market_id, total_supply_usd, caps_json in snapshot_caps_rows:
            headroom: Decimal | None = None
            if isinstance(caps_json, dict):
                raw_value = caps_json.get("supply_cap_usd")
                if raw_value is None:
                    raw_value = caps_json.get("supply_cap")
                try:
                    if raw_value is not None:
                        headroom = max(
                            Decimal(str(raw_value)) - Decimal(str(total_supply_usd)), ZERO
                        )
                except Exception:
                    headroom = None
            cap_headroom_by_market[int(market_id)] = headroom

        prior_rows = self.session.scalars(
            select(ConsumerMarketDemandDaily).where(
                ConsumerMarketDemandDaily.business_date == business_date - timedelta(days=7)
            )
        ).all()
        prior_by_market = {int(row.market_id): row for row in prior_rows}

        output_rows: list[dict[str, object]] = []
        for (
            market_id,
            protocol_code,
            chain_code,
            _display_name,
            _market_address,
            collateral_address,
        ) in market_rows:
            usage_rows = usage_by_market.get(int(market_id), [])
            family = family_by_chain_address.get(
                (str(chain_code), canonical_address(str(collateral_address or ""))),
                "unknown",
            )
            relevant_signoff_wallets = {
                wallet_id
                for wallet_id in signoff_wallet_ids
                if family_balance_by_wallet.get((wallet_id, str(chain_code), family), ZERO)
                >= self.thresholds.classification_dust_floor_usd
            }
            idle_eligible_same_chain_usd = sum(
                (
                    family_balance_by_wallet.get((wallet_id, str(chain_code), family), ZERO)
                    for wallet_id in relevant_signoff_wallets
                ),
                ZERO,
            )

            per_wallet_collateral: dict[int, Decimal] = defaultdict(lambda: ZERO)
            per_wallet_borrow: dict[int, Decimal] = defaultdict(lambda: ZERO)
            near_limit_wallet_count = 0
            collateral_wallet_count = 0
            leveraged_wallet_count = 0
            for row in usage_rows:
                wallet_id = int(row["wallet_id"])
                per_wallet_collateral[wallet_id] += Decimal(str(row["collateral_usd"]))
                per_wallet_borrow[wallet_id] += Decimal(str(row["borrowed_usd"]))
                if Decimal(str(row["collateral_usd"])) > ZERO:
                    collateral_wallet_count += 1
                if Decimal(str(row["borrowed_usd"])) > self.thresholds.leveraged_borrow_usd_floor:
                    leveraged_wallet_count += 1
                health_factor = row["health_factor"]
                if (
                    health_factor is not None
                    and Decimal(str(health_factor))
                    < self.thresholds.capacity.near_limit_health_factor_threshold
                ):
                    near_limit_wallet_count += 1

            leverage_ratios = [
                borrow / collateral
                for wallet_id, collateral in per_wallet_collateral.items()
                if collateral > ZERO
                for borrow in [per_wallet_borrow.get(wallet_id, ZERO)]
            ]
            total_collateral = sum(per_wallet_collateral.values(), ZERO)
            total_borrow = sum(per_wallet_borrow.values(), ZERO)
            top_wallets = sorted(per_wallet_collateral.values(), reverse=True)[:10]
            top10_share = (
                sum(top_wallets, ZERO) / total_collateral if total_collateral > ZERO else None
            )

            utilization, available_liquidity_usd = health_by_market.get(
                int(market_id), (None, None)
            )
            cap_headroom_usd = cap_headroom_by_market.get(int(market_id))
            score = 0
            if (
                utilization is not None
                and utilization >= self.thresholds.capacity.utilization_threshold
            ):
                score += 1
            if (
                top10_share is not None
                and top10_share >= self.thresholds.capacity.top10_collateral_concentration_threshold
            ):
                score += 1

            prior = prior_by_market.get(int(market_id))
            collateral_delta_7d = (
                total_collateral - prior.avant_collateral_usd if prior is not None else None
            )
            wallet_count_delta_7d = (
                collateral_wallet_count - prior.collateral_wallet_count
                if prior is not None
                else None
            )
            collateral_growth_7d = (
                collateral_delta_7d / prior.avant_collateral_usd
                if prior is not None and prior.avant_collateral_usd > ZERO
                else (Decimal("1") if total_collateral > ZERO and prior is not None else None)
            )
            wallet_growth_7d = (
                Decimal(wallet_count_delta_7d) / Decimal(prior.collateral_wallet_count)
                if prior is not None
                and prior.collateral_wallet_count > 0
                and wallet_count_delta_7d is not None
                else (Decimal("1") if collateral_wallet_count > 0 and prior is not None else None)
            )
            if (
                collateral_growth_7d is not None
                and collateral_growth_7d >= self.thresholds.capacity.collateral_growth_7d_threshold
            ):
                score += 1
            if (
                wallet_growth_7d is not None
                and wallet_growth_7d
                >= self.thresholds.capacity.collateral_wallet_growth_7d_threshold
            ):
                score += 1
            if near_limit_wallet_count > 0:
                score += 1
            if cap_headroom_usd is not None and cap_headroom_usd <= ZERO:
                score += 1

            needs_capacity_review = score >= self.thresholds.capacity.review_score_threshold or (
                collateral_delta_7d is not None
                and collateral_delta_7d > ZERO
                and any(
                    (
                        utilization is not None
                        and utilization >= self.thresholds.capacity.utilization_threshold,
                        top10_share is not None
                        and top10_share
                        >= self.thresholds.capacity.top10_collateral_concentration_threshold,
                    )
                )
            )

            output_rows.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": as_of_ts_utc,
                    "market_id": int(market_id),
                    "protocol_code": str(protocol_code),
                    "chain_code": str(chain_code),
                    "collateral_family": family,
                    "holder_count": len(relevant_signoff_wallets),
                    "collateral_wallet_count": collateral_wallet_count,
                    "leveraged_wallet_count": leveraged_wallet_count,
                    "avant_collateral_usd": total_collateral,
                    "borrowed_usd": total_borrow,
                    "idle_eligible_same_chain_usd": idle_eligible_same_chain_usd,
                    "p50_leverage_ratio": _percentile(leverage_ratios, Decimal("0.5")),
                    "p90_leverage_ratio": _percentile(leverage_ratios, Decimal("0.9")),
                    "top10_collateral_share": top10_share,
                    "utilization": utilization,
                    "available_liquidity_usd": available_liquidity_usd,
                    "cap_headroom_usd": cap_headroom_usd,
                    "capacity_pressure_score": score,
                    "needs_capacity_review": needs_capacity_review,
                    "near_limit_wallet_count": near_limit_wallet_count,
                    "avant_collateral_usd_delta_7d": collateral_delta_7d,
                    "collateral_wallet_count_delta_7d": wallet_count_delta_7d,
                }
            )

        self.session.query(ConsumerMarketDemandDaily).filter(
            ConsumerMarketDemandDaily.business_date == business_date
        ).delete()
        if output_rows:
            self.session.execute(insert(ConsumerMarketDemandDaily).values(output_rows))
        return ConsumerCapacityBuildSummary(
            business_date=business_date, rows_written=len(output_rows)
        )
