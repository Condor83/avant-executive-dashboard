"""Portfolio served endpoint filter and sort tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from core.db.models import (
    Market,
    PortfolioPositionCurrent,
    PortfolioPositionDaily,
    Position,
    Product,
    Protocol,
    Token,
    Wallet,
    YieldDaily,
)
from tests.api.conftest import SeedMetadata


def test_positions_current_returns_served_rows(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, meta = api_client
    data = client.get("/portfolio/positions/current").json()

    assert data["business_date"] == str(meta.business_date)
    assert data["total_count"] == 4
    assert len(data["positions"]) == 4
    assert data["positions"][0]["supply_leg"]["symbol"] is not None
    assert any("Aave V3-Ethereum" in row["display_name"] for row in data["positions"])


def test_protocol_filter_narrows_positions(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, _ = api_client
    all_rows = client.get("/portfolio/positions/current").json()
    filtered = client.get("/portfolio/positions/current?protocol_code=aave_v3").json()

    assert filtered["total_count"] == 2
    assert filtered["total_count"] < all_rows["total_count"]
    for row in filtered["positions"]:
        assert row["protocol_code"] == "aave_v3"


def test_product_filter_narrows_positions(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, _ = api_client
    filtered = client.get("/portfolio/positions/current?product_code=stablecoin_senior").json()

    assert filtered["total_count"] == 2
    for row in filtered["positions"]:
        assert row["product_code"] == "stablecoin_senior"


def test_sort_by_gross_yield_mtd_asc(api_client: tuple[TestClient, SeedMetadata]) -> None:
    client, _ = api_client
    data = client.get(
        "/portfolio/positions/current?sort_by=gross_yield_mtd_usd&sort_dir=asc"
    ).json()
    yields = [float(row["yield_mtd"]["gross_yield_usd"]) for row in data["positions"]]

    assert yields == sorted(yields)


def test_position_history_returns_position_and_series(
    api_client: tuple[TestClient, SeedMetadata],
) -> None:
    client, _ = api_client
    data = client.get("/portfolio/positions/pos-w1-m1/history?days=30").json()

    assert data["position"]["position_key"] == "pos-w1-m1"
    assert len(data["history"]) == 1
    assert float(data["history"][0]["net_equity_usd"]) > 0


def test_positions_current_pairs_split_reserve_rows(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    wallet_address = "0x1111111111111111111111111111111111111111"
    paired_prefix = "paired-reserve:aave_v3:ethereum:"

    wallet = session.scalar(select(Wallet).where(Wallet.address == wallet_address))
    product = session.scalar(select(Product).where(Product.product_code == "stablecoin_senior"))
    protocol = session.scalar(select(Protocol).where(Protocol.protocol_code == "aave_v3"))
    market_usdc = session.scalar(select(Market).where(Market.native_market_key == "aave-usdc"))
    market_wbtc = session.scalar(select(Market).where(Market.native_market_key == "aave-wbtc"))
    assert market_usdc is not None
    assert market_wbtc is not None
    token_usdc = session.scalar(
        select(Token).where(Token.chain_id == market_usdc.chain_id, Token.symbol == "USDC")
    )
    token_wbtc = session.scalar(
        select(Token).where(Token.chain_id == market_wbtc.chain_id, Token.symbol == "WBTC")
    )
    assert wallet is not None
    assert product is not None
    assert protocol is not None
    assert token_usdc is not None
    assert token_wbtc is not None

    as_of_ts_utc = session.scalar(select(PortfolioPositionCurrent.as_of_ts_utc).limit(1))
    assert as_of_ts_utc is not None

    rows = [
        {
            "position_key": "pos-w1-loop-supply",
            "display_name": "USDC Loop Supply",
            "market_id": market_usdc.market_id,
            "supply_token_id": token_usdc.token_id,
            "borrow_token_id": None,
            "supply_amount": Decimal("1500"),
            "supply_usd": Decimal("1500"),
            "supply_apy": Decimal("0.05"),
            "borrow_amount": Decimal("0"),
            "borrow_usd": Decimal("0"),
            "borrow_apy": Decimal("0"),
            "reward_apy": Decimal("0.01"),
            "net_equity_usd": Decimal("1500"),
            "gross_yield_daily_usd": Decimal("15"),
            "net_yield_daily_usd": Decimal("15"),
            "gross_yield_mtd_usd": Decimal("150"),
            "net_yield_mtd_usd": Decimal("150"),
            "avg_equity_usd": Decimal("300"),
        },
        {
            "position_key": "pos-w1-loop-borrow-usdc",
            "display_name": "USDC Loop Debt",
            "market_id": market_usdc.market_id,
            "supply_token_id": token_usdc.token_id,
            "borrow_token_id": token_usdc.token_id,
            "supply_amount": Decimal("0"),
            "supply_usd": Decimal("0"),
            "supply_apy": Decimal("0"),
            "borrow_amount": Decimal("600"),
            "borrow_usd": Decimal("600"),
            "borrow_apy": Decimal("0.04"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("-600"),
            "gross_yield_daily_usd": Decimal("6"),
            "net_yield_daily_usd": Decimal("6"),
            "gross_yield_mtd_usd": Decimal("60"),
            "net_yield_mtd_usd": Decimal("60"),
            "avg_equity_usd": Decimal("120"),
        },
        {
            "position_key": "pos-w1-loop-borrow-wbtc",
            "display_name": "WBTC Loop Debt",
            "market_id": market_wbtc.market_id,
            "supply_token_id": token_usdc.token_id,
            "borrow_token_id": token_wbtc.token_id,
            "supply_amount": Decimal("0"),
            "supply_usd": Decimal("0"),
            "supply_apy": Decimal("0"),
            "borrow_amount": Decimal("400"),
            "borrow_usd": Decimal("400"),
            "borrow_apy": Decimal("0.03"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("-400"),
            "gross_yield_daily_usd": Decimal("4"),
            "net_yield_daily_usd": Decimal("4"),
            "gross_yield_mtd_usd": Decimal("40"),
            "net_yield_mtd_usd": Decimal("40"),
            "avg_equity_usd": Decimal("80"),
        },
    ]

    now = datetime.now(UTC)
    positions: list[Position] = []
    for row in rows:
        position = Position(
            position_key=row["position_key"],
            wallet_id=wallet.wallet_id,
            product_id=product.product_id,
            protocol_id=protocol.protocol_id,
            chain_id=market_usdc.chain_id,
            market_id=row["market_id"],
            exposure_class="core_lending",
            status="open",
            display_name=row["display_name"],
            opened_at_utc=now,
            last_seen_at_utc=now,
        )
        positions.append(position)
        session.add(position)
    session.flush()

    for position, row in zip(positions, rows, strict=True):
        session.add(
            PortfolioPositionCurrent(
                position_id=position.position_id,
                business_date=meta.business_date,
                as_of_ts_utc=as_of_ts_utc,
                wallet_id=wallet.wallet_id,
                product_id=product.product_id,
                protocol_id=protocol.protocol_id,
                chain_id=market_usdc.chain_id,
                market_exposure_id=None,
                scope_segment="strategy_only",
                supply_token_id=row["supply_token_id"],
                borrow_token_id=row["borrow_token_id"],
                supply_amount=row["supply_amount"],
                supply_usd=row["supply_usd"],
                supply_apy=row["supply_apy"],
                borrow_amount=row["borrow_amount"],
                borrow_usd=row["borrow_usd"],
                borrow_apy=row["borrow_apy"],
                reward_apy=row["reward_apy"],
                net_equity_usd=row["net_equity_usd"],
                leverage_ratio=None,
                health_factor=Decimal("1.40"),
                gross_yield_daily_usd=row["gross_yield_daily_usd"],
                net_yield_daily_usd=row["net_yield_daily_usd"],
                gross_yield_mtd_usd=row["gross_yield_mtd_usd"],
                net_yield_mtd_usd=row["net_yield_mtd_usd"],
                strategy_fee_daily_usd=Decimal("0"),
                avant_gop_daily_usd=Decimal("0"),
                strategy_fee_mtd_usd=Decimal("0"),
                avant_gop_mtd_usd=Decimal("0"),
                gross_roe=None,
                net_roe=None,
            )
        )
        session.add(
            PortfolioPositionDaily(
                business_date=meta.business_date,
                position_id=position.position_id,
                as_of_ts_utc=as_of_ts_utc,
                market_exposure_id=None,
                scope_segment="strategy_only",
                supply_usd=row["supply_usd"],
                borrow_usd=row["borrow_usd"],
                net_equity_usd=row["net_equity_usd"],
                leverage_ratio=None,
                health_factor=Decimal("1.40"),
                gross_yield_usd=row["gross_yield_daily_usd"],
                net_yield_usd=row["net_yield_daily_usd"],
                strategy_fee_usd=Decimal("0"),
                avant_gop_usd=Decimal("0"),
                gross_roe=None,
                net_roe=None,
            )
        )
        session.add(
            YieldDaily(
                business_date=meta.business_date,
                wallet_id=wallet.wallet_id,
                product_id=product.product_id,
                protocol_id=protocol.protocol_id,
                market_id=row["market_id"],
                row_key=f"position:{row['position_key']}",
                position_key=row["position_key"],
                gross_yield_usd=row["gross_yield_daily_usd"],
                strategy_fee_usd=Decimal("0"),
                avant_gop_usd=Decimal("0"),
                net_yield_usd=row["net_yield_daily_usd"],
                avg_equity_usd=row["avg_equity_usd"],
                gross_roe=None,
                post_strategy_fee_roe=None,
                net_roe=None,
                avant_gop_roe=None,
                method="apy_prorated_sod_eod",
                confidence_score=None,
            )
        )
    session.commit()

    data = client.get(f"/portfolio/positions/current?wallet_address={wallet_address}").json()

    assert data["total_count"] == 3
    paired = next(row for row in data["positions"] if row["position_key"].startswith(paired_prefix))
    assert paired["display_name"] == "USDC/USDC + WBTC Aave V3-Ethereum"
    assert paired["supply_leg"]["symbol"] == "USDC"
    assert len(paired["borrow_legs"]) == 2
    assert {leg["symbol"] for leg in paired["borrow_legs"]} == {"USDC", "WBTC"}
    assert Decimal(paired["net_equity_usd"]) == Decimal("500")
    assert Decimal(paired["leverage_ratio"]) == Decimal("3")
    assert Decimal(paired["roe"]["gross_roe_daily"]) == Decimal("0.05")
    assert Decimal(paired["roe"]["gross_roe_annualized"]) == Decimal("18.25")

    history = client.get(f"/portfolio/positions/{paired['position_key']}/history?days=30").json()
    assert len(history["history"]) == 1
    assert Decimal(history["history"][0]["borrow_usd"]) == Decimal("1000")
    assert Decimal(history["history"][0]["leverage_ratio"]) == Decimal("3")

    summary = client.get("/portfolio/summary").json()
    assert summary["open_position_count"] == 5
