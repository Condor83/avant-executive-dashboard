"""Tests for consumer DeBank visibility unioning and snapshot writes."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.yield_engine import denver_business_bounds_utc
from core.config import ConsumerMarketsConfig
from core.consumer_debank_visibility import (
    ConsumerVisibilityWalletScope,
    load_consumer_seed_wallet_sources,
    merge_consumer_visibility_wallet_scopes,
    sync_consumer_debank_visibility,
)
from core.db.models import (
    ConsumerCohortDaily,
    ConsumerDebankProtocolDaily,
    ConsumerDebankWalletDaily,
    DataQuality,
    Wallet,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


class _StubDebankClient:
    def __init__(
        self,
        *,
        payloads_by_wallet: dict[str, list[dict[str, object]]] | None = None,
        errors_by_wallet: dict[str, str] | None = None,
    ) -> None:
        self.payloads_by_wallet = payloads_by_wallet or {}
        self.errors_by_wallet = errors_by_wallet or {}

    def get_user_complex_protocols(
        self,
        wallet_address: str,
        *,
        chain_ids: list[str] | None = None,
    ) -> list[dict[str, object]]:
        del chain_ids
        error = self.errors_by_wallet.get(wallet_address)
        if error is not None:
            raise RuntimeError(error)
        return self.payloads_by_wallet.get(wallet_address, [])


def test_load_consumer_seed_wallet_sources_dedupes_and_tracks_file_provenance(
    tmp_path: Path,
) -> None:
    (tmp_path / "consumer_wallets_alpha.yaml").write_text(
        """
cohort:
  wallet_addresses:
    - "0x1111111111111111111111111111111111111111"
    - "not-an-evm-address"
  wallets:
    - address: "0x2222222222222222222222222222222222222222"
""".strip(),
        encoding="utf-8",
    )
    (tmp_path / "consumer_wallets_beta.yaml").write_text(
        """
cohort:
  wallet_addresses:
    - "0x1111111111111111111111111111111111111111"
  wallets:
    - address: "0x3333333333333333333333333333333333333333"
""".strip(),
        encoding="utf-8",
    )

    result = load_consumer_seed_wallet_sources(tmp_path)

    assert result == {
        "0x1111111111111111111111111111111111111111": {
            "consumer_wallets_alpha.yaml",
            "consumer_wallets_beta.yaml",
        },
        "0x2222222222222222222222222222222222222222": {"consumer_wallets_alpha.yaml"},
        "0x3333333333333333333333333333333333333333": {"consumer_wallets_beta.yaml"},
    }


def test_merge_consumer_visibility_wallet_scopes_combines_seed_and_cohort() -> None:
    business_date = date(2026, 3, 9)
    _, as_of_ts_utc = denver_business_bounds_utc(business_date)

    cohort_row = ConsumerCohortDaily(
        business_date=business_date,
        as_of_ts_utc=as_of_ts_utc,
        wallet_id=1,
        wallet_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        verified_total_avant_usd=Decimal("100000"),
        discovery_sources_json={"sources": ["routescan:ethereum:savUSD", "legacy_seed"]},
        is_signoff_eligible=True,
        exclusion_reason=None,
    )

    result = merge_consumer_visibility_wallet_scopes(
        seed_wallet_sources={
            "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": {"consumer_wallets_savusd_50k.yaml"},
        },
        cohort_rows=[cohort_row],
    )

    assert result == {
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": ConsumerVisibilityWalletScope(
            wallet_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            seed_sources=(),
            discovery_sources=("legacy_seed", "routescan:ethereum:savUSD"),
            in_seed_set=False,
            in_verified_cohort=True,
            in_signoff_cohort=True,
        ),
        "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb": ConsumerVisibilityWalletScope(
            wallet_address="0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            seed_sources=("consumer_wallets_savusd_50k.yaml",),
            discovery_sources=(),
            in_seed_set=True,
            in_verified_cohort=False,
            in_signoff_cohort=False,
        ),
    }


def test_sync_consumer_debank_visibility_writes_wallet_and_protocol_rows(
    postgres_database_url: str,
    tmp_path: Path,
) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)

    business_date = date(2026, 3, 9)
    _, as_of_ts_utc = denver_business_bounds_utc(business_date)
    cohort_wallet_address = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    seed_wallet_address = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

    (tmp_path / "consumer_wallets_union.yaml").write_text(
        f"""
cohort:
  wallet_addresses:
    - "{seed_wallet_address}"
""".strip(),
        encoding="utf-8",
    )

    client = _StubDebankClient(
        payloads_by_wallet={
            cohort_wallet_address: [
                {
                    "id": "morpho",
                    "chain": "eth",
                    "portfolio_item_list": [
                        {
                            "detail": {
                                "supply_token_list": [{"symbol": "savUSD", "usd_value": "150"}],
                                "borrow_token_list": [{"symbol": "USDC", "usd_value": "50"}],
                            }
                        }
                    ],
                }
            ]
        },
        errors_by_wallet={seed_wallet_address: "rate limited"},
    )
    consumer_markets_config = ConsumerMarketsConfig.model_validate(
        {
            "markets": [
                {
                    "protocol": "morpho",
                    "chain": "ethereum",
                    "name": "Morpho savUSD / USDC",
                    "market_address": "0x1234567890123456789012345678901234567890",
                    "collateral_token": {
                        "symbol": "savUSD",
                        "address": "0x1111111111111111111111111111111111111111",
                        "decimals": 18,
                    },
                    "borrow_token": {
                        "symbol": "USDC",
                        "address": "0x2222222222222222222222222222222222222222",
                        "decimals": 6,
                    },
                }
            ]
        }
    )

    with Session(engine) as session:
        cohort_wallet = Wallet(address=cohort_wallet_address, wallet_type="customer")
        session.add(cohort_wallet)
        session.flush()
        session.add(
            ConsumerCohortDaily(
                business_date=business_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=cohort_wallet.wallet_id,
                wallet_address=cohort_wallet_address,
                verified_total_avant_usd=Decimal("100000"),
                discovery_sources_json={"sources": ["routescan:ethereum:savUSD"]},
                is_signoff_eligible=True,
                exclusion_reason=None,
            )
        )
        session.flush()

        summary = sync_consumer_debank_visibility(
            session=session,
            client=client,
            business_date=business_date,
            consumer_markets_config=consumer_markets_config,
            config_dir=tmp_path,
            min_leg_usd=Decimal("1"),
            max_concurrency=1,
        )
        session.commit()

        assert summary.union_wallet_count == 2
        assert summary.seed_wallet_count == 1
        assert summary.verified_cohort_wallet_count == 1
        assert summary.signoff_cohort_wallet_count == 1
        assert summary.new_discovered_not_in_seed_count == 1
        assert summary.fetched_wallet_count == 1
        assert summary.fetch_error_count == 1
        assert summary.active_wallet_count == 1
        assert summary.borrow_wallet_count == 1
        assert summary.configured_surface_wallet_count == 1
        assert summary.wallet_rows_written == 2
        assert summary.protocol_rows_written == 1
        assert summary.issues_written == 1

        wallet_rows = {
            row.wallet_address: row
            for row in session.scalars(select(ConsumerDebankWalletDaily)).all()
        }
        assert set(wallet_rows) == {cohort_wallet_address, seed_wallet_address}

        cohort_row = wallet_rows[cohort_wallet_address]
        assert cohort_row.in_seed_set is False
        assert cohort_row.in_verified_cohort is True
        assert cohort_row.in_signoff_cohort is True
        assert cohort_row.fetch_succeeded is True
        assert cohort_row.has_any_activity is True
        assert cohort_row.has_any_borrow is True
        assert cohort_row.has_configured_surface_activity is True
        assert cohort_row.protocol_count == 1
        assert cohort_row.chain_count == 1
        assert cohort_row.configured_protocol_count == 1
        assert cohort_row.total_supply_usd == Decimal("150")
        assert cohort_row.total_borrow_usd == Decimal("50")
        assert cohort_row.discovery_sources_json == ["routescan:ethereum:savUSD"]

        seed_row = wallet_rows[seed_wallet_address]
        assert seed_row.in_seed_set is True
        assert seed_row.in_verified_cohort is False
        assert seed_row.fetch_succeeded is False
        assert seed_row.fetch_error_message == "rate limited"

        protocol_rows = session.scalars(select(ConsumerDebankProtocolDaily)).all()
        assert len(protocol_rows) == 1
        assert protocol_rows[0].wallet_address == cohort_wallet_address
        assert protocol_rows[0].chain_code == "ethereum"
        assert protocol_rows[0].protocol_code == "morpho"
        assert protocol_rows[0].in_config_surface is True
        assert protocol_rows[0].supply_usd == Decimal("150")
        assert protocol_rows[0].borrow_usd == Decimal("50")

        seed_wallet = session.scalar(select(Wallet).where(Wallet.address == seed_wallet_address))
        assert seed_wallet is not None
        assert seed_wallet.wallet_type == "customer"

        dq_rows = session.scalars(
            select(DataQuality).where(DataQuality.stage == "sync_consumer_debank_visibility")
        ).all()
        assert len(dq_rows) == 1
        assert dq_rows[0].wallet_address == seed_wallet_address
        assert dq_rows[0].error_type == "debank_wallet_fetch_failed"
