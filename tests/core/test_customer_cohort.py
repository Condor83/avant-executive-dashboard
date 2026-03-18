"""Customer cohort filtering and threshold conversion tests."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

from core.config import (
    AvantToken,
    AvantTokensConfig,
    ConsumerCapacityThresholds,
    ConsumerRiskBandThresholds,
    ConsumerThresholdsConfig,
    ConsumerWhaleThresholds,
    EulerChainConfig,
    EulerVault,
    MarketsConfig,
    MorphoChainConfig,
)
from core.customer_cohort import (
    EvmBatchRpcClient,
    HolderBalance,
    build_customer_snapshot_markets_config,
    build_customer_wallet_cohort,
    minimum_balance_raw_for_usd_threshold,
    verify_customer_wallet_balances,
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


def test_verify_customer_wallet_balances_supports_direct_and_convert_paths() -> None:
    class StubRpcClient(EvmBatchRpcClient):
        def __init__(self) -> None:
            pass

        def read_erc20_balances(
            self,
            *,
            chain_code: str,
            token_address: str,
            wallet_addresses: list[str],
        ) -> dict[str, int | None]:
            del chain_code, wallet_addresses
            if token_address == "0xshare":
                return {"0xaaaa000000000000000000000000000000000000": 2 * 10**18}
            return {"0xaaaa000000000000000000000000000000000000": 5 * 10**17}

        def convert_to_assets(
            self,
            *,
            chain_code: str,
            vault_address: str,
            shares_by_wallet: dict[str, int],
        ) -> dict[str, int | None]:
            del chain_code, shares_by_wallet
            assert vault_address == "0xshare"
            return {"0xaaaa000000000000000000000000000000000000": 4_000_000}

    thresholds = ConsumerThresholdsConfig(
        verified_min_total_avant_usd=Decimal("1"),
        cohort_min_total_avant_usd=Decimal("1"),
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
        whales=ConsumerWhaleThresholds(),
    )
    avant_tokens = AvantTokensConfig(
        tokens=[
            AvantToken(
                chain_code="ethereum",
                token_address="0xdirect",
                symbol="savUSD",
                asset_family="usd",
                wrapper_class="staked",
                decimals=18,
                pricing_policy="direct_price",
            ),
            AvantToken(
                chain_code="ethereum",
                token_address="0xshare",
                symbol="wbravUSDC",
                asset_family="usd",
                wrapper_class="boosted",
                decimals=18,
                pricing_policy="convert_to_assets",
                underlying_token_address="0xusdc",
            ),
        ]
    )

    wallets, issues = verify_customer_wallet_balances(
        business_date=date(2026, 3, 9),
        as_of_ts_utc=datetime(2026, 3, 10, tzinfo=UTC),
        candidate_sources={"0xaaaa000000000000000000000000000000000000": {"legacy_seed"}},
        avant_tokens=avant_tokens,
        thresholds=thresholds,
        rpc_client=StubRpcClient(),
        price_map={
            ("ethereum", "0xdirect"): Decimal("1"),
            ("ethereum", "0xusdc"): Decimal("1"),
        },
        token_decimals_by_key={("ethereum", "0xusdc"): 6},
    )

    assert len(issues) == 0
    assert len(wallets) == 1
    wallet = wallets[0]
    assert wallet.wallet_address == "0xaaaa000000000000000000000000000000000000"
    assert wallet.verified_total_avant_usd == Decimal("4.5")
    assert wallet.is_signoff_eligible is True


def test_build_customer_snapshot_markets_config_reuses_registry_and_wallets() -> None:
    markets = MarketsConfig(
        aave_v3={},
        spark={},
        morpho={
            "ethereum": MorphoChainConfig(
                morpho="0xmorpho",
                wallets=["0x1111111111111111111111111111111111111111"],
                markets=[],
                vaults=[],
            )
        },
        euler_v2={
            "ethereum": EulerChainConfig(
                wallets=["0x1111111111111111111111111111111111111111"],
                vaults=[
                    EulerVault(
                        address="0xvault",
                        symbol="eUSDC",
                        asset_address="0xusdc",
                        asset_symbol="USDC",
                        asset_decimals=6,
                    )
                ],
            )
        },
        dolomite={},
        kamino={},
        zest={},
        wallet_balances={},
        traderjoe_lp={},
        stakedao={},
        etherex={},
    )
    avant_tokens = AvantTokensConfig(
        tokens=[
            AvantToken(
                chain_code="ethereum",
                token_address="0xdirect",
                symbol="savUSD",
                asset_family="usd",
                wrapper_class="staked",
                decimals=18,
                pricing_policy="direct_price",
            )
        ]
    )

    config = build_customer_snapshot_markets_config(
        markets_config=markets,
        avant_tokens=avant_tokens,
        business_date=date(2026, 3, 9),
        wallet_addresses=["0xaaaa000000000000000000000000000000000000"],
    )

    assert list(config.wallet_balances) == ["ethereum"]
    assert config.wallet_balances["ethereum"].wallets == [
        "0xaaaa000000000000000000000000000000000000"
    ]
    assert config.wallet_balances["ethereum"].tokens[0].symbol == "savUSD"
    assert config.morpho["ethereum"].wallets == ["0xaaaa000000000000000000000000000000000000"]
    assert config.euler_v2["ethereum"].wallets == ["0xaaaa000000000000000000000000000000000000"]


def test_build_customer_snapshot_markets_config_respects_protocol_wallet_scopes() -> None:
    markets = MarketsConfig(
        aave_v3={},
        spark={},
        morpho={
            "ethereum": MorphoChainConfig(
                morpho="0xmorpho",
                wallets=["0x1111111111111111111111111111111111111111"],
                markets=[],
                vaults=[],
            )
        },
        euler_v2={
            "ethereum": EulerChainConfig(
                wallets=["0x1111111111111111111111111111111111111111"],
                vaults=[
                    EulerVault(
                        address="0xvault",
                        symbol="eUSDC",
                        asset_address="0xusdc",
                        asset_symbol="USDC",
                        asset_decimals=6,
                    )
                ],
            )
        },
        dolomite={},
        kamino={},
        zest={},
        wallet_balances={},
        traderjoe_lp={},
        stakedao={},
        etherex={},
    )
    avant_tokens = AvantTokensConfig(
        tokens=[
            AvantToken(
                chain_code="ethereum",
                token_address="0xdirect",
                symbol="savUSD",
                asset_family="usd",
                wrapper_class="staked",
                decimals=18,
                pricing_policy="direct_price",
            )
        ]
    )

    config = build_customer_snapshot_markets_config(
        markets_config=markets,
        avant_tokens=avant_tokens,
        business_date=date(2026, 3, 9),
        wallet_addresses=[
            "0xaaaa000000000000000000000000000000000000",
            "0xbbbb000000000000000000000000000000000000",
        ],
        protocol_wallets_by_adapter={
            "morpho": {"ethereum": ["0xaaaa000000000000000000000000000000000000"]}
        },
    )

    assert config.wallet_balances["ethereum"].wallets == [
        "0xaaaa000000000000000000000000000000000000",
        "0xbbbb000000000000000000000000000000000000",
    ]
    assert config.morpho["ethereum"].wallets == [
        "0xaaaa000000000000000000000000000000000000"
    ]
    assert config.euler_v2 == {}
