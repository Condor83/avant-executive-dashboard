"""Customer cohort filtering and threshold conversion tests."""

from __future__ import annotations

from decimal import Decimal

from core.customer_cohort import (
    HolderBalance,
    build_customer_wallet_cohort,
    minimum_balance_raw_for_usd_threshold,
)


def test_minimum_balance_raw_for_usd_threshold() -> None:
    assert (
        minimum_balance_raw_for_usd_threshold(
            threshold_usd=Decimal("50000"),
            token_price_usd=Decimal("1"),
            token_decimals=18,
        )
        == 50_000 * 10**18
    )
    # Ceiling behavior prevents under-threshold inclusion when price is fractional.
    assert (
        minimum_balance_raw_for_usd_threshold(
            threshold_usd=Decimal("10"),
            token_price_usd=Decimal("3"),
            token_decimals=2,
        )
        == 334
    )


def test_build_customer_wallet_cohort_filters_and_sorts() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    wallet_c = "0xcccccccccccccccccccccccccccccccccccccccc"
    wallet_d = "0xdddddddddddddddddddddddddddddddddddddddd"
    wallet_e = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"

    holders = [
        HolderBalance(address=wallet_a, balance_raw=120),
        HolderBalance(address=wallet_a, balance_raw=90),  # dedupe keeps max
        HolderBalance(address=wallet_b, balance_raw=140),  # strategy excluded
        HolderBalance(address=wallet_c, balance_raw=130),  # protocol excluded
        HolderBalance(address=wallet_d, balance_raw=150),  # contract excluded
        HolderBalance(address=wallet_e, balance_raw=125),
        HolderBalance(address="0x0000000000000000000000000000000000000001", balance_raw=80),
    ]

    result = build_customer_wallet_cohort(
        holders=holders,
        minimum_balance_raw=100,
        strategy_wallets={wallet_b},
        protocol_wallets={wallet_c},
        contract_wallets={wallet_d},
    )

    assert result.fetched_rows == 7
    assert result.unique_rows == 6
    assert result.threshold_rows == 5
    assert result.strategy_excluded == 1
    assert result.protocol_excluded == 1
    assert result.contract_excluded == 1
    assert [wallet.address for wallet in result.wallets] == [wallet_e, wallet_a]
    assert [wallet.balance_raw for wallet in result.wallets] == [125, 120]
