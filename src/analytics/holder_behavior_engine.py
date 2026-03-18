"""Daily customer holder behavior rollups from verified cohort snapshots."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, timedelta
from decimal import Decimal

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import AvantTokensConfig, ConsumerThresholdsConfig, canonical_address
from core.db.models import (
    Chain,
    ConsumerHolderUniverseDaily,
    HolderBehaviorDaily,
    Market,
    PositionSnapshot,
    Protocol,
    Token,
    Wallet,
)

ZERO = Decimal("0")


@dataclass(frozen=True)
class HolderBehaviorBuildSummary:
    business_date: date
    rows_written: int


@dataclass(frozen=True)
class _WalletBalanceRow:
    wallet_id: int
    wallet_address: str
    chain_code: str
    symbol: str
    asset_family: str
    wrapper_class: str
    usd_value: Decimal


@dataclass(frozen=True)
class _ConsumerPositionRow:
    wallet_id: int
    wallet_address: str
    market_id: int
    protocol_code: str
    chain_code: str
    collateral_symbol: str
    collateral_family: str
    collateral_wrapper_class: str
    collateral_usd: Decimal
    borrowed_usd: Decimal
    health_factor: Decimal | None


def _economic_collateral_usd(
    *,
    supplied_usd: Decimal,
    collateral_usd: Decimal | None,
) -> Decimal:
    if collateral_usd is not None and collateral_usd > ZERO:
        return collateral_usd
    return supplied_usd


def _risk_band(
    *,
    leverage_ratio: Decimal | None,
    health_factor: Decimal | None,
    thresholds: ConsumerThresholdsConfig,
) -> str:
    if (
        health_factor is not None
        and health_factor < thresholds.risk_bands.critical_health_factor_lt
    ):
        return "critical"
    if leverage_ratio is not None and leverage_ratio >= Decimal("1"):
        return "critical"
    if (
        health_factor is not None
        and health_factor < thresholds.risk_bands.elevated_health_factor_lt
    ):
        return "elevated"
    if (
        leverage_ratio is not None
        and leverage_ratio >= thresholds.risk_bands.elevated_leverage_ratio_gte
    ):
        return "elevated"
    if health_factor is not None and health_factor < thresholds.risk_bands.watch_health_factor_lt:
        return "watch"
    if (
        leverage_ratio is not None
        and leverage_ratio >= thresholds.risk_bands.watch_leverage_ratio_gte
    ):
        return "watch"
    return "normal"


class HolderBehaviorEngine:
    """Build persisted customer holder behavior rows from stored snapshots."""

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

    def _registry_by_chain_address(
        self, *, business_date: date
    ) -> dict[tuple[str, str], tuple[str, str, str]]:
        registry: dict[tuple[str, str], tuple[str, str, str]] = {}
        for token in self.avant_tokens.tokens:
            if token.active_from is not None and business_date < token.active_from:
                continue
            if token.active_to is not None and business_date > token.active_to:
                continue
            registry[(token.chain_code, canonical_address(token.token_address))] = (
                token.asset_family,
                token.wrapper_class,
                token.symbol,
            )
        return registry

    def _load_wallet_balance_rows(
        self,
        *,
        wallet_ids: list[int],
        as_of_ts_utc,
        business_date: date,
    ) -> list[_WalletBalanceRow]:
        registry = self._registry_by_chain_address(business_date=business_date)
        rows = self.session.execute(
            select(
                PositionSnapshot.wallet_id,
                Wallet.address,
                Chain.chain_code,
                Market.market_address,
                PositionSnapshot.supplied_usd,
            )
            .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
            .join(Market, Market.market_id == PositionSnapshot.market_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .where(
                PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
                PositionSnapshot.wallet_id.in_(wallet_ids),
                Wallet.wallet_type == "customer",
                Market.market_kind == "wallet_balance_token",
            )
        ).all()
        output: list[_WalletBalanceRow] = []
        for wallet_id, wallet_address, chain_code, market_address, supplied_usd in rows:
            registry_row = registry.get((chain_code, canonical_address(market_address)))
            if registry_row is None:
                continue
            asset_family, wrapper_class, symbol = registry_row
            output.append(
                _WalletBalanceRow(
                    wallet_id=int(wallet_id),
                    wallet_address=str(wallet_address),
                    chain_code=str(chain_code),
                    symbol=symbol,
                    asset_family=asset_family,
                    wrapper_class=wrapper_class,
                    usd_value=Decimal(str(supplied_usd)),
                )
            )
        return output

    def _load_consumer_position_rows(
        self,
        *,
        wallet_ids: list[int],
        as_of_ts_utc,
        business_date: date,
    ) -> list[_ConsumerPositionRow]:
        registry = self._registry_by_chain_address(business_date=business_date)
        collateral_token = Token.__table__.alias("collateral_token")
        rows = self.session.execute(
            select(
                PositionSnapshot.wallet_id,
                Wallet.address,
                PositionSnapshot.market_id,
                Protocol.protocol_code,
                Chain.chain_code,
                collateral_token.c.address_or_mint,
                PositionSnapshot.supplied_usd,
                PositionSnapshot.collateral_usd,
                PositionSnapshot.borrowed_usd,
                PositionSnapshot.health_factor,
            )
            .join(Wallet, Wallet.wallet_id == PositionSnapshot.wallet_id)
            .join(Market, Market.market_id == PositionSnapshot.market_id)
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .outerjoin(collateral_token, collateral_token.c.token_id == Market.collateral_token_id)
            .where(
                PositionSnapshot.as_of_ts_utc == as_of_ts_utc,
                PositionSnapshot.wallet_id.in_(wallet_ids),
                Wallet.wallet_type == "customer",
                Market.market_kind == "consumer_market",
            )
        ).all()
        output: list[_ConsumerPositionRow] = []
        for (
            wallet_id,
            wallet_address,
            market_id,
            protocol_code,
            chain_code,
            collateral_token_address,
            supplied_usd,
            collateral_usd,
            borrowed_usd,
            health_factor,
        ) in rows:
            registry_row = registry.get(
                (str(chain_code), canonical_address(str(collateral_token_address or "")))
            )
            collateral_family = "unknown"
            collateral_wrapper_class = "base"
            collateral_symbol = "unknown"
            if registry_row is not None:
                collateral_family = registry_row[0]
                collateral_wrapper_class = registry_row[1]
                collateral_symbol = registry_row[2]
            effective_collateral_usd = _economic_collateral_usd(
                supplied_usd=Decimal(str(supplied_usd)),
                collateral_usd=(
                    Decimal(str(collateral_usd)) if collateral_usd is not None else None
                ),
            )
            output.append(
                _ConsumerPositionRow(
                    wallet_id=int(wallet_id),
                    wallet_address=str(wallet_address),
                    market_id=int(market_id),
                    protocol_code=str(protocol_code),
                    chain_code=str(chain_code),
                    collateral_symbol=collateral_symbol,
                    collateral_family=collateral_family,
                    collateral_wrapper_class=collateral_wrapper_class,
                    collateral_usd=effective_collateral_usd,
                    borrowed_usd=Decimal(str(borrowed_usd)),
                    health_factor=(
                        Decimal(str(health_factor)) if health_factor is not None else None
                    ),
                )
            )
        return output

    def compute_daily(self, *, business_date: date) -> HolderBehaviorBuildSummary:
        cohort_rows = self.session.scalars(
            select(ConsumerHolderUniverseDaily).where(
                ConsumerHolderUniverseDaily.business_date == business_date
            )
        ).all()
        if not cohort_rows:
            self.session.execute(
                delete(HolderBehaviorDaily).where(
                    HolderBehaviorDaily.business_date == business_date
                )
            )
            return HolderBehaviorBuildSummary(business_date=business_date, rows_written=0)

        as_of_ts_utc = max(row.as_of_ts_utc for row in cohort_rows)
        cohort_by_wallet = {int(row.wallet_id): row for row in cohort_rows}
        wallet_ids = sorted(cohort_by_wallet)
        balance_rows = self._load_wallet_balance_rows(
            wallet_ids=wallet_ids,
            as_of_ts_utc=as_of_ts_utc,
            business_date=business_date,
        )
        consumer_rows = self._load_consumer_position_rows(
            wallet_ids=wallet_ids,
            as_of_ts_utc=as_of_ts_utc,
            business_date=business_date,
        )

        balances_by_wallet: dict[int, list[_WalletBalanceRow]] = defaultdict(list)
        for row in balance_rows:
            balances_by_wallet[row.wallet_id].append(row)

        consumer_by_wallet: dict[int, list[_ConsumerPositionRow]] = defaultdict(list)
        for row in consumer_rows:
            consumer_by_wallet[row.wallet_id].append(row)

        prior_rows = self.session.scalars(
            select(HolderBehaviorDaily).where(
                HolderBehaviorDaily.business_date == business_date - timedelta(days=7)
            )
        ).all()
        prior_by_wallet = {int(row.wallet_id): row for row in prior_rows}

        interim_rows: list[dict[str, object]] = []
        for wallet_id in wallet_ids:
            cohort = cohort_by_wallet[wallet_id]
            wallet_balance_rows = balances_by_wallet.get(wallet_id, [])
            wallet_consumer_rows = consumer_by_wallet.get(wallet_id, [])

            wallet_family_totals: dict[str, Decimal] = defaultdict(
                lambda: ZERO,
                {
                    "usd": Decimal(str(cohort.verified_family_usd_usd)),
                    "btc": Decimal(str(cohort.verified_family_btc_usd)),
                    "eth": Decimal(str(cohort.verified_family_eth_usd)),
                },
            )
            wallet_wrapper_totals: dict[str, Decimal] = defaultdict(
                lambda: ZERO,
                {
                    "base": Decimal(str(cohort.verified_base_usd)),
                    "staked": Decimal(str(cohort.verified_staked_usd)),
                    "boosted": Decimal(str(cohort.verified_boosted_usd)),
                },
            )
            chain_family_totals: dict[tuple[str, str], Decimal] = defaultdict(lambda: ZERO)
            for row in wallet_balance_rows:
                chain_family_totals[(row.chain_code, row.asset_family)] += row.usd_value

            wallet_staked_family: dict[str, Decimal] = {
                "usd": Decimal(str(cohort.verified_staked_usd_usd)),
                "eth": Decimal(str(cohort.verified_staked_eth_usd)),
                "btc": Decimal(str(cohort.verified_staked_btc_usd)),
            }
            deployed_family_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
            deployed_wrapper_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
            deployed_staked_family: dict[str, Decimal] = defaultdict(lambda: ZERO)
            protocol_codes: set[str] = set()
            market_ids_seen: set[int] = set()
            chain_codes: set[str] = {row.chain_code for row in wallet_balance_rows}
            borrowed_usd = ZERO
            configured_deployed_avant_usd = ZERO
            health_factors = []
            for row in wallet_consumer_rows:
                protocol_codes.add(row.protocol_code)
                market_ids_seen.add(row.market_id)
                chain_codes.add(row.chain_code)
                borrowed_usd += row.borrowed_usd
                configured_deployed_avant_usd += row.collateral_usd
                deployed_family_totals[row.collateral_family] += row.collateral_usd
                deployed_wrapper_totals[row.collateral_wrapper_class] += row.collateral_usd
                if row.collateral_wrapper_class == "staked":
                    deployed_staked_family[row.collateral_family] += row.collateral_usd
                if row.health_factor is not None:
                    health_factors.append(row.health_factor)

            total_family_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
            total_wrapper_totals: dict[str, Decimal] = defaultdict(lambda: ZERO)
            for family in {"usd", "btc", "eth"}:
                total_family_totals[family] = wallet_family_totals.get(
                    family, ZERO
                ) + deployed_family_totals.get(family, ZERO)
            for wrapper_class in {"base", "staked", "boosted"}:
                total_wrapper_totals[wrapper_class] = wallet_wrapper_totals.get(
                    wrapper_class, ZERO
                ) + deployed_wrapper_totals.get(wrapper_class, ZERO)

            dust_floor = self.thresholds.classification_dust_floor_usd
            family_count = sum(1 for total in total_family_totals.values() if total >= dust_floor)
            wrapper_count = sum(1 for total in total_wrapper_totals.values() if total >= dust_floor)
            wallet_held_avant_usd = Decimal(str(cohort.verified_total_avant_usd))
            total_canonical_avant_exposure_usd = (
                wallet_held_avant_usd + configured_deployed_avant_usd
            )
            idle_avant_usd = wallet_held_avant_usd
            idle_eligible_same_chain_usd = sum(chain_family_totals.values(), ZERO)
            leverage_ratio = (
                borrowed_usd / configured_deployed_avant_usd
                if configured_deployed_avant_usd > ZERO
                else None
            )
            health_factor_min = min(health_factors) if health_factors else None
            risk_band = _risk_band(
                leverage_ratio=leverage_ratio,
                health_factor=health_factor_min,
                thresholds=self.thresholds,
            )
            prior = prior_by_wallet.get(wallet_id)

            interim_rows.append(
                {
                    "business_date": business_date,
                    "as_of_ts_utc": as_of_ts_utc,
                    "wallet_id": wallet_id,
                    "wallet_address": cohort.wallet_address,
                    "is_signoff_eligible": cohort.is_signoff_eligible,
                    "verified_total_avant_usd": wallet_held_avant_usd,
                    "wallet_held_avant_usd": wallet_held_avant_usd,
                    "configured_deployed_avant_usd": configured_deployed_avant_usd,
                    "total_canonical_avant_exposure_usd": total_canonical_avant_exposure_usd,
                    "wallet_family_usd_usd": wallet_family_totals.get("usd", ZERO),
                    "wallet_family_btc_usd": wallet_family_totals.get("btc", ZERO),
                    "wallet_family_eth_usd": wallet_family_totals.get("eth", ZERO),
                    "deployed_family_usd_usd": deployed_family_totals.get("usd", ZERO),
                    "deployed_family_btc_usd": deployed_family_totals.get("btc", ZERO),
                    "deployed_family_eth_usd": deployed_family_totals.get("eth", ZERO),
                    "total_family_usd_usd": total_family_totals.get("usd", ZERO),
                    "total_family_btc_usd": total_family_totals.get("btc", ZERO),
                    "total_family_eth_usd": total_family_totals.get("eth", ZERO),
                    "family_usd_usd": total_family_totals.get("usd", ZERO),
                    "family_btc_usd": total_family_totals.get("btc", ZERO),
                    "family_eth_usd": total_family_totals.get("eth", ZERO),
                    "wallet_base_usd": wallet_wrapper_totals.get("base", ZERO),
                    "wallet_staked_usd": wallet_wrapper_totals.get("staked", ZERO),
                    "wallet_boosted_usd": wallet_wrapper_totals.get("boosted", ZERO),
                    "deployed_base_usd": deployed_wrapper_totals.get("base", ZERO),
                    "deployed_staked_usd": deployed_wrapper_totals.get("staked", ZERO),
                    "deployed_boosted_usd": deployed_wrapper_totals.get("boosted", ZERO),
                    "total_base_usd": total_wrapper_totals.get("base", ZERO),
                    "total_staked_usd": total_wrapper_totals.get("staked", ZERO),
                    "total_boosted_usd": total_wrapper_totals.get("boosted", ZERO),
                    "base_usd": total_wrapper_totals.get("base", ZERO),
                    "staked_usd": total_wrapper_totals.get("staked", ZERO),
                    "boosted_usd": total_wrapper_totals.get("boosted", ZERO),
                    "wallet_staked_usd_usd": wallet_staked_family.get("usd", ZERO),
                    "wallet_staked_eth_usd": wallet_staked_family.get("eth", ZERO),
                    "wallet_staked_btc_usd": wallet_staked_family.get("btc", ZERO),
                    "deployed_staked_usd_usd": deployed_staked_family.get("usd", ZERO),
                    "deployed_staked_eth_usd": deployed_staked_family.get("eth", ZERO),
                    "deployed_staked_btc_usd": deployed_staked_family.get("btc", ZERO),
                    "family_count": family_count,
                    "wrapper_count": wrapper_count,
                    "multi_asset_flag": family_count >= 2,
                    "multi_wrapper_flag": wrapper_count >= 2,
                    "idle_avant_usd": idle_avant_usd,
                    "idle_eligible_same_chain_usd": idle_eligible_same_chain_usd,
                    "avant_collateral_usd": configured_deployed_avant_usd,
                    "borrowed_usd": borrowed_usd,
                    "leveraged_flag": borrowed_usd > self.thresholds.leveraged_borrow_usd_floor,
                    "borrow_against_avant_flag": (
                        borrowed_usd > self.thresholds.leveraged_borrow_usd_floor
                        and configured_deployed_avant_usd > ZERO
                    ),
                    "leverage_ratio": leverage_ratio,
                    "health_factor_min": health_factor_min,
                    "risk_band": risk_band,
                    "protocol_count": len(protocol_codes),
                    "market_count": len(market_ids_seen),
                    "chain_count": len(chain_codes),
                    "behavior_tags_json": [],
                    "whale_rank_by_assets": None,
                    "whale_rank_by_borrow": None,
                    "total_avant_usd_delta_7d": (
                        total_canonical_avant_exposure_usd
                        - prior.total_canonical_avant_exposure_usd
                        if prior is not None
                        else None
                    ),
                    "borrowed_usd_delta_7d": (
                        borrowed_usd - prior.borrowed_usd if prior is not None else None
                    ),
                    "avant_collateral_usd_delta_7d": (
                        configured_deployed_avant_usd - prior.configured_deployed_avant_usd
                        if prior is not None
                        else None
                    ),
                }
            )

        ranked_by_assets = sorted(
            interim_rows,
            key=lambda row: (
                -Decimal(str(row["total_canonical_avant_exposure_usd"])),
                str(row["wallet_address"]),
            ),
        )
        ranked_by_borrow = sorted(
            interim_rows,
            key=lambda row: (-Decimal(str(row["borrowed_usd"])), str(row["wallet_address"])),
        )
        asset_rank_map = {
            str(row["wallet_address"]): index + 1
            for index, row in enumerate(ranked_by_assets[: self.thresholds.whales.top_assets_count])
        }
        borrow_rank_map = {
            str(row["wallet_address"]): index + 1
            for index, row in enumerate(ranked_by_borrow[: self.thresholds.whales.top_borrow_count])
        }

        final_rows: list[dict[str, object]] = []
        for row in interim_rows:
            tags: list[str] = []
            total_avant = Decimal(str(row["total_canonical_avant_exposure_usd"]))
            idle_avant = Decimal(str(row["idle_avant_usd"]))
            if total_avant > ZERO and idle_avant / total_avant >= Decimal("0.75"):
                tags.append("idle_whale")
            if (
                Decimal(str(row["total_staked_usd"]))
                >= self.thresholds.classification_dust_floor_usd
            ):
                tags.append("staker")
            if (
                Decimal(str(row["total_boosted_usd"]))
                >= self.thresholds.classification_dust_floor_usd
            ):
                tags.append("boosted_holder")
            if row["leveraged_flag"] and row["market_count"] == 1:
                tags.append("single_market_levered")
            if row["market_count"] >= 2:
                tags.append("multi_market_user")
            if (
                row["health_factor_min"] is not None
                and Decimal(str(row["health_factor_min"]))
                < self.thresholds.capacity.near_limit_health_factor_threshold
            ):
                tags.append("near_limit")
            row["behavior_tags_json"] = tags
            row["whale_rank_by_assets"] = asset_rank_map.get(str(row["wallet_address"]))
            row["whale_rank_by_borrow"] = borrow_rank_map.get(str(row["wallet_address"]))
            final_rows.append(row)

        self.session.execute(
            delete(HolderBehaviorDaily).where(HolderBehaviorDaily.business_date == business_date)
        )
        if final_rows:
            self.session.execute(insert(HolderBehaviorDaily).values(final_rows))
        return HolderBehaviorBuildSummary(business_date=business_date, rows_written=len(final_rows))
