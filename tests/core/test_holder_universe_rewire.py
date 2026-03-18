"""DB-backed holder-universe rewire tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from core.config import (
    AvantToken,
    AvantTokensConfig,
    ConsumerCapacityThresholds,
    ConsumerMarketsConfig,
    ConsumerRiskBandThresholds,
    ConsumerThresholdsConfig,
    ConsumerWhaleThresholds,
    HolderExclusionsConfig,
    HolderProtocolMapConfig,
    HolderUniverseConfig,
    MarketsConfig,
    WalletProductsConfig,
)
from core.customer_cohort import (
    DebankHolderWalletSummary,
    EvmBatchRpcClient,
    HolderBalance,
    build_holder_protocol_wallet_scopes,
    build_verified_customer_cohort,
    scan_holder_candidate_debank_activity,
    write_consumer_debank_visibility_daily,
)
from core.db.models import (
    Chain,
    ConsumerCohortDaily,
    ConsumerDebankProtocolDaily,
    ConsumerDebankTokenDaily,
    ConsumerHolderUniverseDaily,
    Token,
    Wallet,
)
from core.pricing import PriceFetchResult, PriceOracle
from core.types import PriceQuote


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


class _StubRouteScanClient:
    def __init__(self, holders_by_token: dict[str, list[HolderBalance]]) -> None:
        self.holders_by_token = holders_by_token

    def get_erc20_holders(
        self,
        *,
        chain_id: str,
        token_address: str,
        limit: int = 200,
        max_rows: int | None = None,
    ) -> list[HolderBalance]:
        del chain_id, limit, max_rows
        return list(self.holders_by_token.get(token_address, []))


class _StubRpcClient(EvmBatchRpcClient):
    def __init__(
        self,
        *,
        balances_by_token: dict[str, dict[str, int]] | None = None,
        converted_by_vault: dict[str, dict[str, int]] | None = None,
    ) -> None:
        self.balances_by_token = balances_by_token or {}
        self.converted_by_vault = converted_by_vault or {}

    def read_erc20_balances(
        self,
        *,
        chain_code: str,
        token_address: str,
        wallet_addresses: list[str],
    ) -> dict[str, int | None]:
        del chain_code
        balances = self.balances_by_token.get(token_address, {})
        return {
            wallet_address: balances.get(wallet_address, 0) for wallet_address in wallet_addresses
        }

    def convert_to_assets(
        self,
        *,
        chain_code: str,
        vault_address: str,
        shares_by_wallet: dict[str, int],
    ) -> dict[str, int | None]:
        del chain_code, shares_by_wallet
        balances = self.converted_by_vault.get(vault_address, {})
        return {wallet_address: balances.get(wallet_address) for wallet_address in shares_by_wallet}


class _StubPriceOracle(PriceOracle):
    def __init__(self, quotes: list[PriceQuote]) -> None:
        self._quotes = quotes

    def fetch_prices(
        self,
        requests,
        *,
        as_of_ts_utc: datetime,
    ) -> PriceFetchResult:
        del requests, as_of_ts_utc
        return PriceFetchResult(quotes=self._quotes, issues=[])

    def close(self) -> None:
        return None


class _StubDebankClient:
    def __init__(self, payloads_by_wallet: dict[str, list[dict[str, object]]]) -> None:
        self.payloads_by_wallet = payloads_by_wallet

    def get_user_complex_protocols(
        self,
        wallet_address: str,
        *,
        chain_ids: list[str] | None = None,
    ) -> list[dict[str, object]]:
        del chain_ids
        return list(self.payloads_by_wallet.get(wallet_address, []))


def test_build_verified_customer_cohort_uses_same_day_direct_and_market_evidence_only(
    postgres_database_url: str,
    tmp_path: Path,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 9)
    prior_date = date(2026, 3, 8)
    as_of_ts_utc = datetime(2026, 3, 10, 6, 0, tzinfo=UTC)

    direct_wallet = "0x1111111111111111111111111111111111111111"
    direct_and_position_wallet = "0x2222222222222222222222222222222222222222"
    position_only_wallet = "0x3333333333333333333333333333333333333333"
    debank_only_wallet = "0x4444444444444444444444444444444444444444"
    excluded_wallet = "0x5555555555555555555555555555555555555555"

    savusd_address = "0x06d47f3fb376649c3a9dafe069b3d6e35572219e"
    euler_vault_address = "0xbac3983342b805e66f8756e265b3b0ddf4b685fc"

    with Session(engine) as session:
        chain = Chain(chain_code="avalanche")
        session.add(chain)
        session.flush()
        token = Token(
            chain_id=chain.chain_id,
            address_or_mint=savusd_address,
            symbol="savUSD",
            decimals=18,
        )
        session.add(token)
        session.flush()
        token_id = int(token.token_id)
        session.commit()

    for name in ("markets.yaml", "consumer_markets.yaml", "wallet_products.yaml"):
        (tmp_path / name).write_text("{}", encoding="utf-8")

    markets_config = MarketsConfig(
        aave_v3={},
        spark={},
        morpho={},
        euler_v2={},
        dolomite={},
        kamino={},
        zest={},
        wallet_balances={},
        traderjoe_lp={},
        stakedao={},
        etherex={},
    )
    wallet_products_config = WalletProductsConfig(assignments=[])
    consumer_markets_config = ConsumerMarketsConfig.model_validate(
        {
            "markets": [
                {
                    "protocol": "euler_v2",
                    "chain": "avalanche",
                    "name": "savUSD (deposit)",
                    "market_address": euler_vault_address,
                    "collateral_token": {
                        "symbol": "savUSD",
                        "address": savusd_address,
                        "decimals": 18,
                    },
                    "borrow_token": {
                        "symbol": "savUSD",
                        "address": savusd_address,
                        "decimals": 18,
                    },
                }
            ]
        }
    )
    avant_tokens = AvantTokensConfig(
        tokens=[
            AvantToken(
                chain_code="avalanche",
                token_address=savusd_address,
                symbol="savUSD",
                asset_family="usd",
                wrapper_class="staked",
                decimals=18,
                pricing_policy="direct_price",
            )
        ]
    )
    thresholds = ConsumerThresholdsConfig(
        verified_min_total_avant_usd=Decimal("100"),
        cohort_min_total_avant_usd=Decimal("100"),
        classification_dust_floor_usd=Decimal("100"),
        leveraged_borrow_usd_floor=Decimal("0.00000001"),
        capacity=ConsumerCapacityThresholds(
            utilization_threshold=Decimal("0.85"),
            top10_collateral_concentration_threshold=Decimal("0.5"),
            collateral_growth_7d_threshold=Decimal("0.2"),
            collateral_wallet_growth_7d_threshold=Decimal("0.2"),
            near_limit_health_factor_threshold=Decimal("1.25"),
            review_score_threshold=2,
        ),
        risk_bands=ConsumerRiskBandThresholds(
            watch_health_factor_lt=Decimal("1.5"),
            elevated_health_factor_lt=Decimal("1.25"),
            critical_health_factor_lt=Decimal("1.05"),
            elevated_leverage_ratio_gte=Decimal("0.8"),
            watch_leverage_ratio_gte=Decimal("0.5"),
        ),
        whales=ConsumerWhaleThresholds(wallet_usd_threshold=Decimal("1000000")),
    )
    holder_universe = HolderUniverseConfig.model_validate(
        {
            "chains": [{"chain_code": "avalanche", "chain_id": "43114"}],
            "force_include_wallets": [debank_only_wallet, excluded_wallet],
        }
    )
    holder_exclusions = HolderExclusionsConfig.model_validate(
        {
            "exclusions": [
                {
                    "address": excluded_wallet,
                    "label": "Excluded bridge",
                    "classification": "protocol",
                    "exclude_from_monitoring": True,
                    "exclude_from_customer_float": False,
                }
            ]
        }
    )
    holder_protocol_map = HolderProtocolMapConfig.model_validate(
        {
            "defaults": {"surface": "visibility_only", "primary_use": "other"},
            "protocols": [
                {
                    "protocol_code": "euler_v2",
                    "surface": "canonical_supported",
                    "primary_use": "collateral",
                }
            ],
        }
    )
    price_quotes = [
        PriceQuote(
            token_id=token_id,
            chain_code="avalanche",
            address_or_mint=savusd_address,
            price_usd=Decimal("1"),
        )
    ]
    routescan_client = _StubRouteScanClient(
        {
            savusd_address: [
                HolderBalance(address=direct_wallet, balance_raw=150 * 10**18),
                HolderBalance(address=direct_and_position_wallet, balance_raw=125 * 10**18),
                HolderBalance(address=excluded_wallet, balance_raw=600 * 10**18),
            ],
            euler_vault_address: [
                HolderBalance(address=direct_and_position_wallet, balance_raw=10**18),
                HolderBalance(address=position_only_wallet, balance_raw=10**18),
                HolderBalance(address=excluded_wallet, balance_raw=10**18),
            ],
        }
    )
    rpc_client = _StubRpcClient(
        balances_by_token={
            savusd_address: {
                direct_wallet: 150 * 10**18,
                direct_and_position_wallet: 125 * 10**18,
            }
        },
        converted_by_vault={
            euler_vault_address: {
                direct_and_position_wallet: 200 * 10**18,
                position_only_wallet: 250 * 10**18,
                excluded_wallet: 180 * 10**18,
            }
        },
    )
    price_oracle = _StubPriceOracle(quotes=price_quotes)
    debank_client = _StubDebankClient(
        payloads_by_wallet={
            debank_only_wallet: [
                {
                    "id": "yearn",
                    "chain": "avalanche",
                    "portfolio_item_list": [
                        {
                            "detail": {
                                "supply_token_list": [{"symbol": "savUSD", "usd_value": "500"}]
                            }
                        }
                    ],
                }
            ],
            direct_wallet: [],
            direct_and_position_wallet: [],
            position_only_wallet: [],
        }
    )

    with Session(engine) as session:
        summary = build_verified_customer_cohort(
            session=session,
            business_date=business_date,
            markets_config=markets_config,
            consumer_markets_config=consumer_markets_config,
            wallet_products_config=wallet_products_config,
            avant_tokens=avant_tokens,
            thresholds=thresholds,
            holder_universe=holder_universe,
            holder_exclusions=holder_exclusions,
            holder_protocol_map=holder_protocol_map,
            routescan_client=routescan_client,
            rpc_client=rpc_client,
            price_oracle=price_oracle,
            debank_client=debank_client,
            config_dir=tmp_path,
            rpc_urls={},
        )
        session.commit()

        assert summary.verified_wallet_count == 3
        assert summary.cohort_wallet_count == 3

        universe_rows = {
            row.wallet_address: row
            for row in session.scalars(
                select(ConsumerHolderUniverseDaily).where(
                    ConsumerHolderUniverseDaily.business_date == business_date
                )
            ).all()
        }
        assert set(universe_rows) == {
            direct_wallet,
            direct_and_position_wallet,
            position_only_wallet,
        }
        assert universe_rows[direct_wallet].verified_total_avant_usd == Decimal("150")
        assert universe_rows[direct_and_position_wallet].verified_total_avant_usd == Decimal("125")
        assert universe_rows[position_only_wallet].verified_total_avant_usd == Decimal("0")
        assert (
            "routescan_holder"
            in (universe_rows[direct_wallet].discovery_sources_json or {})["sources"]
        )
        assert (
            "market_position"
            in (universe_rows[direct_and_position_wallet].discovery_sources_json or {})["sources"]
        )
        assert (
            "market_position:euler_v2:avalanche:" + euler_vault_address
            in (universe_rows[position_only_wallet].discovery_sources_json or {})["sources"]
        )

        cohort_rows = {
            row.wallet_address: row
            for row in session.scalars(
                select(ConsumerCohortDaily).where(
                    ConsumerCohortDaily.business_date == business_date
                )
            ).all()
        }
        assert set(cohort_rows) == {
            direct_wallet,
            direct_and_position_wallet,
            position_only_wallet,
        }
        assert cohort_rows[direct_and_position_wallet].verified_total_avant_usd == Decimal("325")
        assert cohort_rows[position_only_wallet].verified_total_avant_usd == Decimal("250")

        debank_protocol_rows = session.scalars(
            select(ConsumerDebankProtocolDaily).where(
                ConsumerDebankProtocolDaily.business_date == business_date
            )
        ).all()
        assert debank_protocol_rows == []

        protocol_scope = build_holder_protocol_wallet_scopes(
            session=session,
            business_date=business_date,
            wallet_addresses=universe_rows,
            holder_protocol_map=holder_protocol_map,
        )
        assert protocol_scope == {
            "euler_v2": {"avalanche": sorted([direct_and_position_wallet, position_only_wallet])}
        }


def test_write_consumer_debank_visibility_daily_batches_large_token_inserts(
    postgres_database_url: str,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    business_date = date(2026, 3, 11)
    as_of_ts_utc = datetime(2026, 3, 12, 6, 0, tzinfo=UTC)
    wallet_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

    with Session(engine) as session:
        session.add(Wallet(address=wallet_address, wallet_type="customer"))
        session.commit()

        wallet_id = session.scalar(select(Wallet.wallet_id).where(Wallet.address == wallet_address))
        assert wallet_id is not None

        token_rows = [
            {
                "business_date": business_date,
                "as_of_ts_utc": as_of_ts_utc,
                "wallet_id": wallet_id,
                "wallet_address": wallet_address,
                "chain_code": "ethereum",
                "protocol_code": f"proto_{index}",
                "token_symbol": f"TKN{index}",
                "leg_type": "supply",
                "in_config_surface": False,
                "usd_value": Decimal("1"),
            }
            for index in range(7000)
        ]

        wallet_count, protocol_count, token_count = write_consumer_debank_visibility_daily(
            session=session,
            business_date=business_date,
            as_of_ts_utc=as_of_ts_utc,
            wallet_ids={wallet_address: wallet_id},
            candidate_sources={wallet_address: {"legacy_seed"}},
            holder_wallets=[],
            cohort_wallets=[],
            wallet_summaries={
                wallet_address: DebankHolderWalletSummary(
                    fetch_succeeded=True,
                    fetch_error_message=None,
                    has_any_activity=True,
                    has_any_borrow=False,
                    has_configured_surface_activity=False,
                    protocol_count=7000,
                    chain_count=1,
                    configured_protocol_count=0,
                    total_supply_usd=Decimal("7000"),
                    total_borrow_usd=Decimal("0"),
                    configured_surface_supply_usd=Decimal("0"),
                    configured_surface_borrow_usd=Decimal("0"),
                    avasset_supply_total_usd=Decimal("0"),
                )
            },
            protocol_rows=[],
            token_rows=token_rows,
        )
        session.commit()

        assert wallet_count == 1
        assert protocol_count == 0
        assert token_count == 7000
        assert (
            session.query(ConsumerDebankTokenDaily)
            .filter(ConsumerDebankTokenDaily.business_date == business_date)
            .count()
            == 7000
        )


def test_scan_holder_candidate_debank_activity_aggregates_duplicate_token_legs() -> None:
    business_date = date(2026, 3, 11)
    as_of_ts_utc = datetime(2026, 3, 12, 6, 0, tzinfo=UTC)
    wallet_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    avant_tokens = AvantTokensConfig(
        tokens=[
            AvantToken(
                chain_code="avalanche",
                token_address="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
                symbol="savUSD",
                asset_family="usd",
                wrapper_class="staked",
                decimals=18,
                pricing_policy="direct_price",
            )
        ]
    )
    holder_protocol_map = HolderProtocolMapConfig.model_validate(
        {
            "defaults": {"surface": "visibility_only", "primary_use": "other"},
            "protocols": [],
        }
    )
    debank_client = _StubDebankClient(
        {
            wallet_address: [
                {
                    "chain": "avalanche",
                    "id": "avax_blackholexyz",
                    "portfolio_item_list": [
                        {
                            "detail": {
                                "supply_token_list": [
                                    {"optimized_symbol": "WAVAX", "usd_value": "100"},
                                    {"optimized_symbol": "WAVAX", "usd_value": "25"},
                                ]
                            }
                        }
                    ],
                }
            ]
        }
    )

    wallet_summaries, protocol_rows, token_rows, issues = scan_holder_candidate_debank_activity(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_ids={wallet_address: 1},
        candidate_sources={wallet_address: {"routescan_holder:avalanche:savUSD"}},
        avant_tokens=avant_tokens,
        holder_protocol_map=holder_protocol_map,
        debank_client=debank_client,
        min_leg_usd=Decimal("1"),
        max_concurrency=1,
    )

    assert issues == []
    assert protocol_rows == [
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of_ts_utc,
            "wallet_id": 1,
            "wallet_address": wallet_address,
            "chain_code": "avalanche",
            "protocol_code": "avax_blackholexyz",
            "in_config_surface": False,
            "supply_usd": Decimal("125"),
            "borrow_usd": Decimal("0"),
        }
    ]
    assert token_rows == [
        {
            "business_date": business_date,
            "as_of_ts_utc": as_of_ts_utc,
            "wallet_id": 1,
            "wallet_address": wallet_address,
            "chain_code": "avalanche",
            "protocol_code": "avax_blackholexyz",
            "token_symbol": "WAVAX",
            "leg_type": "supply",
            "in_config_surface": False,
            "usd_value": Decimal("125"),
        }
    ]
    assert wallet_summaries[wallet_address].total_supply_usd == Decimal("125")
