"""Holder dashboard engine tests — staked_pct sourced from HolderBehaviorDaily."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from analytics.holder_dashboard_engine import (
    _avant_symbol_registry,
    _build_wallet_metrics,
    _resolve_debank_date,
)
from core.config import load_avant_tokens_config
from core.db.models import (
    ConsumerDebankTokenDaily,
    ConsumerDebankWalletDaily,
    HolderBehaviorDaily,
    Wallet,
)

ZERO = Decimal("0")


def _registry_for_tests(
    *, business_date: date
) -> tuple[dict[str, tuple[str, str]], list[tuple[str, tuple[str, str]]]]:
    avant_tokens = load_avant_tokens_config("config/avant_tokens.yaml")
    return _avant_symbol_registry(avant_tokens, business_date=business_date)


def _make_holder_row(
    *,
    business_date: date,
    as_of_ts_utc: datetime,
    wallet_id: int,
    wallet_address: str,
    wallet_family_usd: str = "100000",
    deployed_family_usd: str = "0",
    wallet_staked_usd_usd: str = "0",
    deployed_staked_usd_usd: str = "0",
) -> HolderBehaviorDaily:
    wallet_held = Decimal(wallet_family_usd)
    deployed = Decimal(deployed_family_usd)
    total = wallet_held + deployed
    return HolderBehaviorDaily(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_id=wallet_id,
        wallet_address=wallet_address,
        is_signoff_eligible=True,
        verified_total_avant_usd=wallet_held,
        wallet_held_avant_usd=wallet_held,
        configured_deployed_avant_usd=deployed,
        total_canonical_avant_exposure_usd=total,
        wallet_family_usd_usd=wallet_held,
        wallet_family_btc_usd=ZERO,
        wallet_family_eth_usd=ZERO,
        deployed_family_usd_usd=deployed,
        deployed_family_btc_usd=ZERO,
        deployed_family_eth_usd=ZERO,
        total_family_usd_usd=total,
        total_family_btc_usd=ZERO,
        total_family_eth_usd=ZERO,
        family_usd_usd=total,
        family_btc_usd=ZERO,
        family_eth_usd=ZERO,
        wallet_base_usd=wallet_held - Decimal(wallet_staked_usd_usd),
        wallet_staked_usd=Decimal(wallet_staked_usd_usd),
        wallet_boosted_usd=ZERO,
        deployed_base_usd=ZERO,
        deployed_staked_usd=Decimal(deployed_staked_usd_usd),
        deployed_boosted_usd=ZERO,
        total_base_usd=wallet_held - Decimal(wallet_staked_usd_usd),
        total_staked_usd=Decimal(wallet_staked_usd_usd) + Decimal(deployed_staked_usd_usd),
        total_boosted_usd=ZERO,
        base_usd=wallet_held - Decimal(wallet_staked_usd_usd),
        staked_usd=Decimal(wallet_staked_usd_usd) + Decimal(deployed_staked_usd_usd),
        boosted_usd=ZERO,
        wallet_staked_usd_usd=Decimal(wallet_staked_usd_usd),
        wallet_staked_eth_usd=ZERO,
        wallet_staked_btc_usd=ZERO,
        deployed_staked_usd_usd=Decimal(deployed_staked_usd_usd),
        deployed_staked_eth_usd=ZERO,
        deployed_staked_btc_usd=ZERO,
        family_count=1,
        wrapper_count=1
        + (1 if Decimal(wallet_staked_usd_usd) + Decimal(deployed_staked_usd_usd) > 0 else 0),
        multi_asset_flag=False,
        multi_wrapper_flag=False,
        idle_avant_usd=wallet_held,
        idle_eligible_same_chain_usd=wallet_held,
        avant_collateral_usd=deployed,
        borrowed_usd=ZERO,
        leveraged_flag=False,
        borrow_against_avant_flag=False,
        leverage_ratio=None,
        health_factor_min=None,
        risk_band="normal",
        protocol_count=0,
        market_count=0,
        chain_count=1,
        behavior_tags_json=[],
        whale_rank_by_assets=None,
        whale_rank_by_borrow=None,
        total_avant_usd_delta_7d=None,
        borrowed_usd_delta_7d=None,
        avant_collateral_usd_delta_7d=None,
    )


def test_staked_pct_uses_behavior_daily_not_position_snapshots() -> None:
    """staked_by_scope should come from HolderBehaviorDaily per-family staked fields,
    NOT from PositionSnapshot wallet_balance_token records.

    This test passes NO balance_rows and NO consumer_rows (simulating missing
    PositionSnapshot data) but sets non-zero wallet_staked_usd_usd on the
    HolderBehaviorDaily row. The resulting WalletDashboardMetrics.staked_by_scope
    should reflect the behavior-daily value, not zero.
    """
    bd = date(2026, 3, 11)
    ts = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)

    holder = _make_holder_row(
        business_date=bd,
        as_of_ts_utc=ts,
        wallet_id=1,
        wallet_address="0xaaaa",
        wallet_family_usd="100000",
        wallet_staked_usd_usd="70000",
    )

    wallets = _build_wallet_metrics(
        business_date=bd,
        holder_rows=[holder],
        visibility_rows=[],
        token_rows=[],
        balance_rows=[],
        consumer_rows=[],
        first_seen_by_wallet={1: bd},
        registry_by_symbol=_registry_for_tests(business_date=bd)[0],
        registry_symbols_sorted=_registry_for_tests(business_date=bd)[1],
    )

    assert len(wallets) == 1
    w = wallets[0]
    assert w.staked_by_scope["avusd"] == Decimal("70000")
    assert w.staked_by_scope["all"] == Decimal("70000")
    assert w.staked_by_scope["aveth"] == ZERO
    assert w.staked_by_scope["avbtc"] == ZERO


def test_staked_includes_deployed_staked() -> None:
    """Deployed staked (e.g. savUSD used as Morpho collateral) should be
    included in staked_by_scope alongside wallet-held staked."""
    bd = date(2026, 3, 11)
    ts = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)

    holder = _make_holder_row(
        business_date=bd,
        as_of_ts_utc=ts,
        wallet_id=1,
        wallet_address="0xbbbb",
        wallet_family_usd="50000",
        deployed_family_usd="50000",
        wallet_staked_usd_usd="30000",
        deployed_staked_usd_usd="50000",
    )

    wallets = _build_wallet_metrics(
        business_date=bd,
        holder_rows=[holder],
        visibility_rows=[],
        token_rows=[],
        balance_rows=[],
        consumer_rows=[],
        first_seen_by_wallet={1: bd},
        registry_by_symbol=_registry_for_tests(business_date=bd)[0],
        registry_symbols_sorted=_registry_for_tests(business_date=bd)[1],
    )

    assert len(wallets) == 1
    w = wallets[0]
    assert w.staked_by_scope["avusd"] == Decimal("80000")
    assert w.staked_by_scope["all"] == Decimal("80000")


def test_external_avant_wrapper_rows_affect_observed_wrapper_mix() -> None:
    bd = date(2026, 3, 11)
    ts = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)
    registry_by_symbol, registry_symbols_sorted = _registry_for_tests(business_date=bd)

    holder = _make_holder_row(
        business_date=bd,
        as_of_ts_utc=ts,
        wallet_id=1,
        wallet_address="0xcccc",
        wallet_family_usd="100000",
        wallet_staked_usd_usd="20000",
    )
    token_rows = [
        ConsumerDebankTokenDaily(
            business_date=bd,
            as_of_ts_utc=ts,
            wallet_id=1,
            wallet_address="0xcccc",
            chain_code="ethereum",
            protocol_code="yearn",
            token_symbol="savUSD",
            leg_type="supply",
            in_config_surface=False,
            usd_value=Decimal("50000"),
        ),
        ConsumerDebankTokenDaily(
            business_date=bd,
            as_of_ts_utc=ts,
            wallet_id=1,
            wallet_address="0xcccc",
            chain_code="ethereum",
            protocol_code="yearn",
            token_symbol="wbravUSDC",
            leg_type="supply",
            in_config_surface=False,
            usd_value=Decimal("30000"),
        ),
        ConsumerDebankTokenDaily(
            business_date=bd,
            as_of_ts_utc=ts,
            wallet_id=1,
            wallet_address="0xcccc",
            chain_code="ethereum",
            protocol_code="pendle2",
            token_symbol="PT-savUSD",
            leg_type="supply",
            in_config_surface=False,
            usd_value=Decimal("10000"),
        ),
        ConsumerDebankTokenDaily(
            business_date=bd,
            as_of_ts_utc=ts,
            wallet_id=1,
            wallet_address="0xcccc",
            chain_code="ethereum",
            protocol_code="pendle2",
            token_symbol="YT-savUSD",
            leg_type="supply",
            in_config_surface=False,
            usd_value=Decimal("5000"),
        ),
    ]

    wallets = _build_wallet_metrics(
        business_date=bd,
        holder_rows=[holder],
        visibility_rows=[],
        token_rows=token_rows,
        balance_rows=[],
        consumer_rows=[],
        first_seen_by_wallet={1: bd},
        registry_by_symbol=registry_by_symbol,
        registry_symbols_sorted=registry_symbols_sorted,
    )

    assert len(wallets) == 1
    wallet = wallets[0]
    assert wallet.observed_by_scope["all"] == Decimal("195000")
    assert wallet.staked_by_scope["avusd"] == Decimal("70000")
    assert wallet.staked_by_scope["all"] == Decimal("70000")
    assert wallet.boosted_by_scope["avusd"] == Decimal("30000")
    assert wallet.boosted_by_scope["all"] == Decimal("30000")
    assert wallet.fixed_yield_by_scope["all"] == Decimal("10000")
    assert wallet.yield_token_by_scope["all"] == Decimal("5000")
    assert wallet.other_defi_by_scope["all"] == Decimal("80000")


# ---------------------------------------------------------------------------
# _resolve_debank_date fallback tests (DB-backed)
# ---------------------------------------------------------------------------


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def test_resolve_debank_date_returns_exact_when_data_exists(
    postgres_database_url: str,
) -> None:
    """When rows exist for the target date, return that date."""
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    target = date(2026, 3, 11)
    ts = datetime(2026, 3, 11, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        wallet = Wallet(address="0xaaa1", wallet_type="customer")
        session.add(wallet)
        session.flush()
        session.add(
            ConsumerDebankWalletDaily(
                business_date=target,
                as_of_ts_utc=ts,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
            )
        )
        session.flush()

        assert _resolve_debank_date(session, target) == target


def test_resolve_debank_date_falls_back_to_most_recent(
    postgres_database_url: str,
) -> None:
    """When no rows for target date, return the most recent earlier date."""
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    target = date(2026, 3, 11)
    stale = date(2026, 3, 9)
    ts = datetime(2026, 3, 9, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        wallet = Wallet(address="0xaaa2", wallet_type="customer")
        session.add(wallet)
        session.flush()
        session.add(
            ConsumerDebankWalletDaily(
                business_date=stale,
                as_of_ts_utc=ts,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
            )
        )
        session.flush()

        assert _resolve_debank_date(session, target) == stale


def test_resolve_debank_date_returns_none_when_no_data(
    postgres_database_url: str,
) -> None:
    """When no DeBank data exists at all, return None."""
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    with Session(engine) as session:
        assert _resolve_debank_date(session, date(2026, 3, 11)) is None


def test_resolve_debank_date_ignores_data_beyond_lookback(
    postgres_database_url: str,
) -> None:
    """Data older than DEBANK_LOOKBACK_DAYS should not be used."""
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    target = date(2026, 3, 11)
    old = date(2026, 3, 2)  # 9 days before — outside 7-day lookback
    ts = datetime(2026, 3, 2, 12, 0, tzinfo=UTC)

    with Session(engine) as session:
        wallet = Wallet(address="0xaaa3", wallet_type="customer")
        session.add(wallet)
        session.flush()
        session.add(
            ConsumerDebankWalletDaily(
                business_date=old,
                as_of_ts_utc=ts,
                wallet_id=wallet.wallet_id,
                wallet_address=wallet.address,
            )
        )
        session.flush()

        assert _resolve_debank_date(session, target) is None
