"""Tests for DeBank coverage normalization and matching logic."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from sqlalchemy.orm import Session

import core.debank_coverage as debank_coverage
from core.config import MarketsConfig
from core.debank_coverage import (
    LegKey,
    PreflightStatus,
    normalize_token_symbol,
    run_debank_coverage_audit,
)


class _StubClient:
    def __init__(self, payload_by_wallet: dict[str, list[dict[str, object]]]) -> None:
        self.payload_by_wallet = payload_by_wallet

    def get_user_complex_protocols(
        self,
        wallet_address: str,
        *,
        chain_ids: list[str] | None = None,
    ) -> list[dict[str, object]]:
        del chain_ids
        return self.payload_by_wallet.get(wallet_address, [])


def _markets_config_with_spark() -> MarketsConfig:
    return MarketsConfig.model_validate(
        {
            "aave_v3": {},
            "spark": {
                "ethereum": {
                    "pool": "0x1111111111111111111111111111111111111111",
                    "pool_data_provider": "0x2222222222222222222222222222222222222222",
                    "wallets": [],
                    "markets": [],
                }
            },
            "morpho": {},
            "euler_v2": {},
            "dolomite": {},
            "kamino": {},
            "zest": {},
            "wallet_balances": {},
        }
    )


def test_normalize_token_symbol_aliases() -> None:
    assert normalize_token_symbol("WETH") == "ETH"
    assert normalize_token_symbol("USDC.e") == "USDC"
    assert normalize_token_symbol("USD₮0") == "USDT0"
    assert normalize_token_symbol("wbravUSDC") == "USDC"


def test_db_leg_token_symbol_prefers_morpho_loan_token_for_no_debt_supply() -> None:
    symbol = debank_coverage._db_leg_token_symbol(
        protocol_code="morpho",
        leg_type="supply",
        base_symbol="FRXUSD",
        collateral_symbol="SAVUSD",
        metadata_json={"loan_token": "frxUSD", "collateral_token": "savUSD"},
        supplied_usd=Decimal("100"),
        borrowed_usd=Decimal("0"),
    )

    assert symbol == "frxUSD"


def test_db_leg_token_symbol_prefers_euler_collateral_for_consumer_supply() -> None:
    symbol = debank_coverage._db_leg_token_symbol(
        protocol_code="euler_v2",
        leg_type="supply",
        base_symbol="USDC",
        collateral_symbol="savUSD",
        metadata_json={
            "kind": "consumer_market",
            "borrow_token_symbol": "USDC",
            "collateral_token_symbol": "savUSD",
        },
        supplied_usd=Decimal("100"),
        borrowed_usd=Decimal("50"),
    )

    assert symbol == "savUSD"


def test_flatten_payload_legs_normalizes_aliases_and_filters_rewards() -> None:
    payload: list[dict[str, object]] = [
        {
            "id": "spark",
            "chain": "eth",
            "portfolio_item_list": [
                {
                    "detail": {
                        "supply_token_list": [{"symbol": "USDC.e", "usd_value": "100"}],
                        "borrow_token_list": [{"symbol": "USD₮0", "usd_value": "40"}],
                        "reward_token_list": [{"symbol": "SPARK", "usd_value": "5"}],
                    }
                }
            ],
        }
    ]

    legs, in_scope = debank_coverage._flatten_debank_payload_legs(
        wallet_address="0x1111111111111111111111111111111111111111",
        payload=payload,
        configured_chains={"ethereum"},
        configured_protocols={"spark"},
        min_leg_usd=Decimal("1"),
    )

    supply_key = LegKey(
        wallet_address="0x1111111111111111111111111111111111111111",
        chain_code="ethereum",
        protocol_code="spark",
        leg_type="supply",
        token_symbol="USDC",
    )
    borrow_key = LegKey(
        wallet_address="0x1111111111111111111111111111111111111111",
        chain_code="ethereum",
        protocol_code="spark",
        leg_type="borrow",
        token_symbol="USDT0",
    )

    assert set(legs.keys()) == {supply_key, borrow_key}
    assert legs[supply_key] == Decimal("100")
    assert legs[borrow_key] == Decimal("40")
    assert in_scope[supply_key] is True
    assert in_scope[borrow_key] is True


def test_run_debank_coverage_audit_applies_tolerance_and_config_surface(monkeypatch) -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    as_of_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)

    payload: list[dict[str, object]] = [
        {
            "id": "spark",
            "chain": "eth",
            "portfolio_item_list": [
                {
                    "detail": {
                        "supply_token_list": [{"symbol": "WETH", "usd_value": "100"}],
                    }
                }
            ],
        }
    ]
    client = _StubClient({wallet: payload})

    monkeypatch.setattr(
        debank_coverage,
        "_strategy_wallets_from_db",
        lambda session: ([wallet], [wallet]),
    )
    monkeypatch.setattr(
        debank_coverage,
        "_resolve_snapshot_as_of",
        lambda session, requested_as_of: as_of_ts,
    )
    monkeypatch.setattr(
        debank_coverage,
        "_preflight_status",
        lambda session, as_of_ts_utc, configured_protocols: PreflightStatus(
            missing_protocol_dimensions=[],
            zero_snapshot_protocols=[],
            snapshot_counts_by_protocol={"spark": 1},
        ),
    )
    monkeypatch.setattr(
        debank_coverage,
        "_load_db_legs",
        lambda session, as_of_ts_utc, min_leg_usd: {
            LegKey(
                wallet_address=wallet,
                chain_code="ethereum",
                protocol_code="spark",
                leg_type="supply",
                token_symbol="ETH",
            ): Decimal("100.90")
        },
    )

    result = run_debank_coverage_audit(
        session=cast(Session, object()),  # DB access is monkeypatched in this unit test.
        client=client,
        markets_config=_markets_config_with_spark(),
        as_of_ts_utc=None,
        min_leg_usd=Decimal("1"),
        match_tolerance_usd=Decimal("1"),
        max_concurrency=1,
    )

    assert result.as_of_ts_utc == as_of_ts
    assert result.totals_all.total_legs == 1
    assert result.totals_all.matched_legs == 1
    assert result.totals_all.coverage_pct == Decimal("100")
    assert result.totals_configured_surface.total_legs == 1
    assert not result.unmatched_rows


def test_run_debank_coverage_audit_remaps_debank_symbol_to_db_canonical_when_close(
    monkeypatch,
) -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    as_of_ts = datetime(2026, 3, 4, 0, 0, tzinfo=UTC)

    payload: list[dict[str, object]] = [
        {
            "id": "aave_v3",
            "chain": "eth",
            "portfolio_item_list": [
                {
                    "detail": {
                        "supply_token_list": [{"symbol": "USDE", "usd_value": "1000"}],
                    }
                }
            ],
        }
    ]
    client = _StubClient({wallet: payload})

    monkeypatch.setattr(
        debank_coverage,
        "_strategy_wallets_from_db",
        lambda session: ([wallet], [wallet]),
    )
    monkeypatch.setattr(
        debank_coverage,
        "_resolve_snapshot_as_of",
        lambda session, requested_as_of: as_of_ts,
    )
    monkeypatch.setattr(
        debank_coverage,
        "_preflight_status",
        lambda session, as_of_ts_utc, configured_protocols: PreflightStatus(
            missing_protocol_dimensions=[],
            zero_snapshot_protocols=[],
            snapshot_counts_by_protocol={"aave_v3": 1},
        ),
    )
    monkeypatch.setattr(
        debank_coverage,
        "_load_db_legs",
        lambda session, as_of_ts_utc, min_leg_usd: {
            LegKey(
                wallet_address=wallet,
                chain_code="ethereum",
                protocol_code="aave_v3",
                leg_type="supply",
                token_symbol="SUSDE",
            ): Decimal("1000.5")
        },
    )

    result = run_debank_coverage_audit(
        session=cast(Session, object()),
        client=client,
        markets_config=_markets_config_with_spark(),
        as_of_ts_utc=None,
        min_leg_usd=Decimal("1"),
        match_tolerance_usd=Decimal("1"),
        max_concurrency=1,
    )

    assert result.totals_all.total_legs == 1
    assert result.totals_all.matched_legs == 1
    assert not result.unmatched_rows


def test_run_debank_coverage_audit_does_not_remap_when_notional_far(monkeypatch) -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    as_of_ts = datetime(2026, 3, 4, 0, 0, tzinfo=UTC)

    payload: list[dict[str, object]] = [
        {
            "id": "aave_v3",
            "chain": "eth",
            "portfolio_item_list": [
                {
                    "detail": {
                        "supply_token_list": [{"symbol": "USDE", "usd_value": "1000"}],
                    }
                }
            ],
        }
    ]
    client = _StubClient({wallet: payload})

    monkeypatch.setattr(
        debank_coverage,
        "_strategy_wallets_from_db",
        lambda session: ([wallet], [wallet]),
    )
    monkeypatch.setattr(
        debank_coverage,
        "_resolve_snapshot_as_of",
        lambda session, requested_as_of: as_of_ts,
    )
    monkeypatch.setattr(
        debank_coverage,
        "_preflight_status",
        lambda session, as_of_ts_utc, configured_protocols: PreflightStatus(
            missing_protocol_dimensions=[],
            zero_snapshot_protocols=[],
            snapshot_counts_by_protocol={"aave_v3": 1},
        ),
    )
    monkeypatch.setattr(
        debank_coverage,
        "_load_db_legs",
        lambda session, as_of_ts_utc, min_leg_usd: {
            LegKey(
                wallet_address=wallet,
                chain_code="ethereum",
                protocol_code="aave_v3",
                leg_type="supply",
                token_symbol="SUSDE",
            ): Decimal("1300")
        },
    )

    result = run_debank_coverage_audit(
        session=cast(Session, object()),
        client=client,
        markets_config=_markets_config_with_spark(),
        as_of_ts_utc=None,
        min_leg_usd=Decimal("1"),
        match_tolerance_usd=Decimal("1"),
        max_concurrency=1,
    )

    assert result.totals_all.total_legs == 1
    assert result.totals_all.matched_legs == 0
    assert len(result.unmatched_rows) == 1
    assert result.unmatched_rows[0].key.token_symbol == "USDE"
