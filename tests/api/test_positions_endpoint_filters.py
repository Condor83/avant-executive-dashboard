"""Portfolio served endpoint filter and sort tests."""

from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

from analytics.portfolio_views import PortfolioViewEngine
from core.db.models import (
    Chain,
    Market,
    PortfolioPositionCurrent,
    PortfolioPositionDaily,
    Position,
    PositionSnapshot,
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
    assert data["positions"][0]["supply_legs"]
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


def test_positions_current_pairs_split_dolomite_account_rows(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    wallet_address = "0x4444444444444444444444444444444444444444"

    chain_bera = session.scalar(select(Chain).where(Chain.chain_code == "bera"))
    if chain_bera is None:
        chain_bera = Chain(chain_code="bera")
        session.add(chain_bera)
        session.flush()
    protocol = session.scalar(select(Protocol).where(Protocol.protocol_code == "dolomite"))
    if protocol is None:
        protocol = Protocol(protocol_code="dolomite")
        session.add(protocol)
        session.flush()
    product = session.scalar(select(Product).where(Product.product_code == "stablecoin_senior"))
    wallet = session.scalar(select(Wallet).where(Wallet.address == wallet_address))
    if wallet is None:
        wallet = Wallet(address=wallet_address, wallet_type="strategy")
        session.add(wallet)
        session.flush()
    token_savusd = session.scalar(
        select(Token).where(Token.chain_id == chain_bera.chain_id, Token.symbol == "savUSD")
    )
    if token_savusd is None:
        token_savusd = Token(
            chain_id=chain_bera.chain_id,
            address_or_mint="0xa744fe3688291ac3a4a7ec917678783ad9946a1e",
            symbol="savUSD",
            decimals=18,
        )
        session.add(token_savusd)
        session.flush()
    token_usdce = session.scalar(
        select(Token).where(Token.chain_id == chain_bera.chain_id, Token.symbol == "USDC.e")
    )
    if token_usdce is None:
        token_usdce = Token(
            chain_id=chain_bera.chain_id,
            address_or_mint="0x549943e04f40284185054145c6e4e9568c1d3241",
            symbol="USDC.e",
            decimals=6,
        )
        session.add(token_usdce)
        session.flush()

    market_savusd = session.scalar(
        select(Market).where(Market.native_market_key == "dolomite-bera-savusd")
    )
    if market_savusd is None:
        market_savusd = Market(
            chain_id=chain_bera.chain_id,
            protocol_id=protocol.protocol_id,
            native_market_key="dolomite-bera-savusd",
            market_address="47",
            market_kind="market",
            display_name="savUSD Dolomite",
            base_asset_token_id=token_savusd.token_id,
        )
        session.add(market_savusd)
        session.flush()
    market_usdce = session.scalar(
        select(Market).where(Market.native_market_key == "dolomite-bera-usdce")
    )
    if market_usdce is None:
        market_usdce = Market(
            chain_id=chain_bera.chain_id,
            protocol_id=protocol.protocol_id,
            native_market_key="dolomite-bera-usdce",
            market_address="2",
            market_kind="market",
            display_name="USDC.e Dolomite",
            base_asset_token_id=token_usdce.token_id,
        )
        session.add(market_usdce)
        session.flush()

    assert product is not None
    as_of_ts_utc = session.scalar(select(PortfolioPositionCurrent.as_of_ts_utc).limit(1))
    assert as_of_ts_utc is not None

    rows = [
        {
            "position_key": f"dolomite:bera:{wallet_address}:123:47",
            "display_name": "savUSD Dolomite",
            "market_id": market_savusd.market_id,
            "supply_token_id": token_savusd.token_id,
            "borrow_token_id": None,
            "supply_amount": Decimal("1500"),
            "supply_usd": Decimal("1500"),
            "supply_apy": Decimal("0.08"),
            "borrow_amount": Decimal("0"),
            "borrow_usd": Decimal("0"),
            "borrow_apy": Decimal("0"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("1500"),
            "gross_yield_daily_usd": Decimal("10"),
            "net_yield_daily_usd": Decimal("10"),
            "gross_yield_mtd_usd": Decimal("100"),
            "net_yield_mtd_usd": Decimal("100"),
            "avg_equity_usd": Decimal("300"),
        },
        {
            "position_key": f"dolomite:bera:{wallet_address}:123:2",
            "display_name": "USDC.e Dolomite",
            "market_id": market_usdce.market_id,
            "supply_token_id": token_usdce.token_id,
            "borrow_token_id": token_usdce.token_id,
            "supply_amount": Decimal("0"),
            "supply_usd": Decimal("0"),
            "supply_apy": Decimal("0"),
            "borrow_amount": Decimal("1000"),
            "borrow_usd": Decimal("1000"),
            "borrow_apy": Decimal("0.04"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("-1000"),
            "gross_yield_daily_usd": Decimal("5"),
            "net_yield_daily_usd": Decimal("5"),
            "gross_yield_mtd_usd": Decimal("50"),
            "net_yield_mtd_usd": Decimal("50"),
            "avg_equity_usd": Decimal("200"),
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
            chain_id=chain_bera.chain_id,
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
                chain_id=chain_bera.chain_id,
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
                health_factor=None,
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
                health_factor=None,
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

    assert data["total_count"] == 1
    paired = data["positions"][0]
    assert paired["position_key"].startswith("paired-dolomite:dolomite:bera:")
    assert paired["display_name"] == "savUSD/USDC.e Dolomite-Bera"
    assert paired["position_kind"] == "Carry"
    assert paired["supply_leg"]["symbol"] == "savUSD"
    assert len(paired["borrow_legs"]) == 1
    assert paired["borrow_legs"][0]["symbol"] == "USDC.e"
    assert Decimal(paired["net_equity_usd"]) == Decimal("500")
    assert Decimal(paired["leverage_ratio"]) == Decimal("3")
    assert Decimal(paired["roe"]["gross_roe_daily"]) == Decimal("0.03")


def test_positions_current_pairs_spark_multi_supply_reserve_rows(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    wallet_address = "0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd"

    chain_eth = session.scalar(select(Chain).where(Chain.chain_code == "ethereum"))
    if chain_eth is None:
        chain_eth = Chain(chain_code="ethereum")
        session.add(chain_eth)
        session.flush()
    protocol = session.scalar(select(Protocol).where(Protocol.protocol_code == "spark"))
    if protocol is None:
        protocol = Protocol(protocol_code="spark")
        session.add(protocol)
        session.flush()
    product = session.scalar(select(Product).where(Product.product_code == "eth_senior"))
    if product is None:
        product = Product(product_code="eth_senior")
        session.add(product)
        session.flush()
    wallet = session.scalar(select(Wallet).where(Wallet.address == wallet_address))
    if wallet is None:
        wallet = Wallet(address=wallet_address, wallet_type="strategy")
        session.add(wallet)
        session.flush()

    def _token(symbol: str, address: str, decimals: int) -> Token:
        token = session.scalar(
            select(Token).where(Token.chain_id == chain_eth.chain_id, Token.symbol == symbol)
        )
        if token is None:
            token = Token(
                chain_id=chain_eth.chain_id,
                address_or_mint=address,
                symbol=symbol,
                decimals=decimals,
            )
            session.add(token)
            session.flush()
        return token

    token_weeth = _token("weETH", "0xcd5fe23c85820f7b72d0926fc9b05b43e359b7ee", 18)
    token_weth = _token("WETH", "0xc02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2", 18)
    token_usdc = _token("USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6)
    token_usdt = _token("USDT", "0xdAC17F958D2ee523a2206206994597C13D831ec7", 6)

    def _market(native_key: str, display_name: str, base_token: Token) -> Market:
        market = session.scalar(select(Market).where(Market.native_market_key == native_key))
        if market is None:
            market = Market(
                chain_id=chain_eth.chain_id,
                protocol_id=protocol.protocol_id,
                native_market_key=native_key,
                market_address=base_token.address_or_mint,
                market_kind="reserve",
                display_name=display_name,
                base_asset_token_id=base_token.token_id,
            )
            session.add(market)
            session.flush()
        return market

    market_weeth = _market("spark-weeth", "weETH Spark", token_weeth)
    market_weth = _market("spark-weth", "WETH Spark", token_weth)
    market_usdc = _market("spark-usdc", "USDC Spark", token_usdc)
    market_usdt = _market("spark-usdt", "USDT Spark", token_usdt)

    as_of_ts_utc = session.scalar(select(PortfolioPositionCurrent.as_of_ts_utc).limit(1))
    assert as_of_ts_utc is not None

    rows = [
        {
            "position_key": "spark:ethereum:0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd:weeth",
            "display_name": "weETH Spark",
            "market_id": market_weeth.market_id,
            "supply_token_id": token_weeth.token_id,
            "borrow_token_id": None,
            "supply_amount": Decimal("2921.7060"),
            "supply_usd": Decimal("5805196"),
            "supply_apy": Decimal("0.0249"),
            "borrow_amount": Decimal("0"),
            "borrow_usd": Decimal("0"),
            "borrow_apy": Decimal("0"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("5805196"),
            "gross_yield_daily_usd": Decimal("396"),
            "net_yield_daily_usd": Decimal("396"),
            "gross_yield_mtd_usd": Decimal("3960"),
            "net_yield_mtd_usd": Decimal("3960"),
            "avg_equity_usd": Decimal("5805196"),
        },
        {
            "position_key": "spark:ethereum:0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd:weth",
            "display_name": "WETH Spark",
            "market_id": market_weth.market_id,
            "supply_token_id": token_weth.token_id,
            "borrow_token_id": None,
            "supply_amount": Decimal("1088.7076"),
            "supply_usd": Decimal("2163175"),
            "supply_apy": Decimal("0.0175"),
            "borrow_amount": Decimal("0"),
            "borrow_usd": Decimal("0"),
            "borrow_apy": Decimal("0"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("2163175"),
            "gross_yield_daily_usd": Decimal("103"),
            "net_yield_daily_usd": Decimal("103"),
            "gross_yield_mtd_usd": Decimal("1030"),
            "net_yield_mtd_usd": Decimal("1030"),
            "avg_equity_usd": Decimal("2163175"),
        },
        {
            "position_key": "spark:ethereum:0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd:usdt",
            "display_name": "USDT Spark",
            "market_id": market_usdt.market_id,
            "supply_token_id": token_usdt.token_id,
            "borrow_token_id": token_usdt.token_id,
            "supply_amount": Decimal("0"),
            "supply_usd": Decimal("0"),
            "supply_apy": Decimal("0"),
            "borrow_amount": Decimal("3190573.1162"),
            "borrow_usd": Decimal("3190382"),
            "borrow_apy": Decimal("0.0342"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("-3190382"),
            "gross_yield_daily_usd": Decimal("-299"),
            "net_yield_daily_usd": Decimal("-299"),
            "gross_yield_mtd_usd": Decimal("-2990"),
            "net_yield_mtd_usd": Decimal("-2990"),
            "avg_equity_usd": Decimal("0"),
        },
        {
            "position_key": "spark:ethereum:0x6cc60a0b57bc882a0471980d0e2d4ad7ddf3c4bd:usdc",
            "display_name": "USDC Spark",
            "market_id": market_usdc.market_id,
            "supply_token_id": token_usdc.token_id,
            "borrow_token_id": token_usdc.token_id,
            "supply_amount": Decimal("0"),
            "supply_usd": Decimal("0"),
            "supply_apy": Decimal("0"),
            "borrow_amount": Decimal("1478483.9444"),
            "borrow_usd": Decimal("1478632"),
            "borrow_apy": Decimal("0.0502"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("-1478632"),
            "gross_yield_daily_usd": Decimal("-203"),
            "net_yield_daily_usd": Decimal("-203"),
            "gross_yield_mtd_usd": Decimal("-2030"),
            "net_yield_mtd_usd": Decimal("-2030"),
            "avg_equity_usd": Decimal("0"),
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
            chain_id=chain_eth.chain_id,
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
                chain_id=chain_eth.chain_id,
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
                health_factor=Decimal("1.39"),
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
                health_factor=Decimal("1.39"),
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

    data = client.get(
        f"/portfolio/positions/current?wallet_address={wallet_address}&protocol_code=spark"
    ).json()

    paired = next(
        row for row in data["positions"] if row["position_key"].startswith("paired-reserve:spark:")
    )
    assert paired["display_name"] == "weETH + WETH/USDT + USDC Spark-Ethereum"
    assert paired["position_kind"] == "Carry"
    assert len(paired["supply_legs"]) == 2
    assert [leg["symbol"] for leg in paired["supply_legs"]] == ["weETH", "WETH"]
    assert len(paired["borrow_legs"]) == 2
    assert {leg["symbol"] for leg in paired["borrow_legs"]} == {"USDT", "USDC"}
    assert Decimal(paired["net_equity_usd"]) == Decimal("3299357")


def test_positions_current_labels_euler_consumer_market_using_collateral_and_base_tokens(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    wallet_address = "0x5555555555555555555555555555555555555555"

    chain_avax = session.scalar(select(Chain).where(Chain.chain_code == "avalanche"))
    if chain_avax is None:
        chain_avax = Chain(chain_code="avalanche")
        session.add(chain_avax)
        session.flush()
    protocol = session.scalar(select(Protocol).where(Protocol.protocol_code == "euler_v2"))
    if protocol is None:
        protocol = Protocol(protocol_code="euler_v2")
        session.add(protocol)
        session.flush()
    product = session.scalar(select(Product).where(Product.product_code == "stablecoin_senior"))
    if product is None:
        product = Product(product_code="stablecoin_senior")
        session.add(product)
        session.flush()
    wallet = session.scalar(select(Wallet).where(Wallet.address == wallet_address))
    if wallet is None:
        wallet = Wallet(address=wallet_address, wallet_type="strategy")
        session.add(wallet)
        session.flush()

    token_usdc = session.scalar(
        select(Token).where(Token.chain_id == chain_avax.chain_id, Token.symbol == "USDC")
    )
    if token_usdc is None:
        token_usdc = Token(
            chain_id=chain_avax.chain_id,
            address_or_mint="0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e",
            symbol="USDC",
            decimals=6,
        )
        session.add(token_usdc)
        session.flush()
    token_savusd = session.scalar(
        select(Token).where(Token.chain_id == chain_avax.chain_id, Token.symbol == "savUSD")
    )
    if token_savusd is None:
        token_savusd = Token(
            chain_id=chain_avax.chain_id,
            address_or_mint="0x06d47f3fb376649c3a9dafe069b3d6e35572219e",
            symbol="savUSD",
            decimals=18,
        )
        session.add(token_savusd)
        session.flush()

    market_supply = session.scalar(select(Market).where(Market.native_market_key == "euler-eavusd"))
    if market_supply is None:
        market_supply = Market(
            chain_id=chain_avax.chain_id,
            protocol_id=protocol.protocol_id,
            native_market_key="euler-eavusd",
            market_address="0xbac3983342b805e66f8756e265b3b0ddf4b685fc",
            market_kind="vault",
            display_name="eavUSD",
            base_asset_token_id=token_savusd.token_id,
        )
        session.add(market_supply)
        session.flush()
    market_borrow = session.scalar(select(Market).where(Market.native_market_key == "euler-eusdc"))
    if market_borrow is None:
        market_borrow = Market(
            chain_id=chain_avax.chain_id,
            protocol_id=protocol.protocol_id,
            native_market_key="euler-eusdc",
            market_address="0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e",
            market_kind="vault",
            display_name="eUSDC",
            base_asset_token_id=token_usdc.token_id,
        )
        session.add(market_borrow)
        session.flush()
    consumer_market = session.scalar(
        select(Market).where(
            Market.native_market_key
            == (
                "0xbac3983342b805e66f8756e265b3b0ddf4b685fc/"
                "0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e"
            )
        )
    )
    if consumer_market is None:
        consumer_market = Market(
            chain_id=chain_avax.chain_id,
            protocol_id=protocol.protocol_id,
            native_market_key="0xbac3983342b805e66f8756e265b3b0ddf4b685fc/0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e",
            market_address="0xbac3983342b805e66f8756e265b3b0ddf4b685fc/0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e",
            market_kind="consumer_market",
            display_name="savUSD / USDC",
            base_asset_token_id=token_usdc.token_id,
            collateral_token_id=token_savusd.token_id,
        )
        session.add(consumer_market)
        session.flush()

    as_of_ts_utc = session.scalar(select(PortfolioPositionCurrent.as_of_ts_utc).limit(1))
    assert as_of_ts_utc is not None
    position_key = (
        "euler_v2:avalanche:"
        f"{wallet_address}:"
        "0xbac3983342b805e66f8756e265b3b0ddf4b685fc/0x37ca03ad51b8ff79aad35fadacba4cedf0c3e74e:acct1"
    )
    existing_position = Position(
        position_key=position_key,
        wallet_id=wallet.wallet_id,
        product_id=product.product_id,
        protocol_id=protocol.protocol_id,
        chain_id=chain_avax.chain_id,
        market_id=consumer_market.market_id,
        exposure_class="core_lending",
        status="open",
        display_name="stale euler row",
        opened_at_utc=as_of_ts_utc,
        last_seen_at_utc=as_of_ts_utc,
    )
    session.add(existing_position)
    session.flush()

    session.add(
        PositionSnapshot(
            as_of_ts_utc=as_of_ts_utc,
            block_number_or_slot="123",
            wallet_id=wallet.wallet_id,
            position_id=existing_position.position_id,
            market_id=consumer_market.market_id,
            position_key=position_key,
            supplied_amount=Decimal("840000"),
            supplied_usd=Decimal("968372.28"),
            borrowed_amount=Decimal("496419.379333"),
            borrowed_usd=Decimal("496419.199603721296168555"),
            supply_apy=Decimal("0.0745"),
            borrow_apy=Decimal("0.0937303457"),
            reward_apy=Decimal("0"),
            equity_usd=Decimal("471953.080930197103831445"),
            health_factor=None,
            ltv=None,
            source="rpc",
        )
    )
    session.commit()

    PortfolioViewEngine(session).compute_daily(
        business_date=meta.business_date,
        as_of_ts_utc=as_of_ts_utc,
    )

    data = client.get(
        f"/portfolio/positions/current?wallet_address={wallet_address}&protocol_code=euler_v2"
    ).json()

    assert data["total_count"] == 1
    row = data["positions"][0]
    assert row["display_name"] == "savUSD/USDC Euler V2-Avalanche"
    assert row["supply_legs"][0]["symbol"] == "savUSD"
    assert row["borrow_legs"][0]["symbol"] == "USDC"
    assert row["position_kind"] == "Carry"
    assert Decimal(row["yield_mtd"]["net_yield_usd"]) == Decimal("0")


def test_positions_current_groups_zest_multi_supply_carry_rows(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    wallet_address = "SPYK0B07DVM059RARCA23BARRZT5P42QH8KKP4QF"

    chain_stacks = session.scalar(select(Chain).where(Chain.chain_code == "stacks"))
    if chain_stacks is None:
        chain_stacks = Chain(chain_code="stacks")
        session.add(chain_stacks)
        session.flush()
    protocol = session.scalar(select(Protocol).where(Protocol.protocol_code == "zest"))
    if protocol is None:
        protocol = Protocol(protocol_code="zest")
        session.add(protocol)
        session.flush()
    product = session.scalar(select(Product).where(Product.product_code == "btc_senior"))
    if product is None:
        product = Product(product_code="btc_senior")
        session.add(product)
        session.flush()
    wallet = session.scalar(select(Wallet).where(Wallet.address == wallet_address))
    if wallet is None:
        wallet = Wallet(address=wallet_address, wallet_type="strategy")
        session.add(wallet)
        session.flush()

    def _token(symbol: str, address: str, decimals: int) -> Token:
        token = session.scalar(
            select(Token).where(Token.chain_id == chain_stacks.chain_id, Token.symbol == symbol)
        )
        if token is None:
            token = Token(
                chain_id=chain_stacks.chain_id,
                address_or_mint=address,
                symbol=symbol,
                decimals=decimals,
            )
            session.add(token)
            session.flush()
        return token

    token_sbtc = _token("sBTC", "SM3VDXK3WZZSA84XXFKAFAF15NNZX32CTSG82JFQ4.sbtc-token", 8)
    token_aeusdc = _token("aeUSDC", "SP3Y2ZSH8P7D50B0VBTSX11S7XSG24M1VB9YFQA4K.token-aeusdc", 6)

    def _market(native_key: str, display_name: str, base_token: Token) -> Market:
        market = session.scalar(select(Market).where(Market.native_market_key == native_key))
        if market is None:
            market = Market(
                chain_id=chain_stacks.chain_id,
                protocol_id=protocol.protocol_id,
                native_market_key=native_key,
                market_address=base_token.address_or_mint,
                market_kind="market",
                display_name=display_name,
                base_asset_token_id=base_token.token_id,
            )
            session.add(market)
            session.flush()
        return market

    market_sbtc = _market(token_sbtc.address_or_mint, "sBTC", token_sbtc)
    market_aeusdc = _market(token_aeusdc.address_or_mint, "aeUSDC", token_aeusdc)

    as_of_ts_utc = session.scalar(select(PortfolioPositionCurrent.as_of_ts_utc).limit(1))
    assert as_of_ts_utc is not None

    rows = [
        {
            "position_key": f"zest:stacks:{wallet_address}:{market_sbtc.native_market_key}",
            "display_name": "sBTC",
            "market_id": market_sbtc.market_id,
            "supply_token_id": token_sbtc.token_id,
            "borrow_token_id": token_sbtc.token_id,
            "supply_amount": Decimal("51.25977292"),
            "supply_usd": Decimal("3489659.104331594981040624"),
            "supply_apy": Decimal("0.0006449200"),
            "borrow_amount": Decimal("0.00000001"),
            "borrow_usd": Decimal("0.000680779274964372"),
            "borrow_apy": Decimal("0.0512581700"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("3489659.103650815706075660"),
            "gross_yield_daily_usd": Decimal("6.156901012616322516"),
            "net_yield_daily_usd": Decimal("5.229498360724874139"),
            "gross_yield_mtd_usd": Decimal("6.156901012616322516"),
            "net_yield_mtd_usd": Decimal("5.229498360724874139"),
            "avg_equity_usd": Decimal("3489659.103650815706075660"),
        },
        {
            "position_key": f"zest:stacks:{wallet_address}:{market_aeusdc.native_market_key}",
            "display_name": "aeUSDC",
            "market_id": market_aeusdc.market_id,
            "supply_token_id": token_aeusdc.token_id,
            "borrow_token_id": token_aeusdc.token_id,
            "supply_amount": Decimal("2341399.675108"),
            "supply_usd": Decimal("2341398.054492516257456136"),
            "supply_apy": Decimal("0.0995079800"),
            "borrow_amount": Decimal("1859162.029682"),
            "borrow_usd": Decimal("1859160.742850535846027088"),
            "borrow_apy": Decimal("0.1121483300"),
            "reward_apy": Decimal("0"),
            "net_equity_usd": Decimal("482237.311641980411429048"),
            "gross_yield_daily_usd": Decimal("-504.012533412207771343"),
            "net_yield_daily_usd": Decimal("-428.410653400376605642"),
            "gross_yield_mtd_usd": Decimal("-504.012533412207771343"),
            "net_yield_mtd_usd": Decimal("-428.410653400376605642"),
            "avg_equity_usd": Decimal("482237.311641980411429048"),
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
            chain_id=chain_stacks.chain_id,
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
                chain_id=chain_stacks.chain_id,
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
                health_factor=Decimal("1.30"),
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
                health_factor=Decimal("1.30"),
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

    data = client.get(
        f"/portfolio/positions/current?wallet_address={wallet_address}&protocol_code=zest"
    ).json()

    assert data["total_count"] == 1
    row = data["positions"][0]
    assert row["position_key"].startswith("paired-zest:zest:stacks:")
    assert row["display_name"] == "sBTC + aeUSDC/aeUSDC Zest-Stacks"
    assert row["position_kind"] == "Carry"
    assert [leg["symbol"] for leg in row["supply_legs"]] == ["sBTC", "aeUSDC"]
    assert [leg["symbol"] for leg in row["borrow_legs"]] == ["aeUSDC"]


def test_positions_current_groups_stakedao_underlyings_into_curated_vault_row(
    api_client: tuple[TestClient, SeedMetadata],
    seeded_session: tuple[Session, SeedMetadata],
) -> None:
    client, meta = api_client
    session, _ = seeded_session
    wallet_address = "0xaa0d9205ae55dcf6321df019da9265b12ba41a7f"

    chain_eth = session.scalar(select(Chain).where(Chain.chain_code == "ethereum"))
    if chain_eth is None:
        chain_eth = Chain(chain_code="ethereum")
        session.add(chain_eth)
        session.flush()
    protocol = session.scalar(select(Protocol).where(Protocol.protocol_code == "stakedao"))
    if protocol is None:
        protocol = Protocol(protocol_code="stakedao")
        session.add(protocol)
        session.flush()
    product = session.scalar(select(Product).where(Product.product_code == "stablecoin_senior"))
    if product is None:
        product = Product(product_code="stablecoin_senior")
        session.add(product)
        session.flush()
    wallet = session.scalar(select(Wallet).where(Wallet.address == wallet_address))
    if wallet is None:
        wallet = Wallet(address=wallet_address, wallet_type="strategy")
        session.add(wallet)
        session.flush()

    def _token(symbol: str, address: str, decimals: int) -> Token:
        token = session.scalar(
            select(Token).where(Token.chain_id == chain_eth.chain_id, Token.symbol == symbol)
        )
        if token is None:
            token = Token(
                chain_id=chain_eth.chain_id,
                address_or_mint=address,
                symbol=symbol,
                decimals=decimals,
            )
            session.add(token)
            session.flush()
        return token

    token_usdc = _token("USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48", 6)
    token_savusd = _token("savUSD", "0xb8D89678E75a973E74698c976716308abB8a46A4", 18)

    def _market(token: Token) -> Market:
        native_market_key = (
            f"0xad1dcc6ca212673d2dfd403a905a1ec57666d910:{token.address_or_mint.lower()}"
        )
        market = session.scalar(select(Market).where(Market.native_market_key == native_market_key))
        if market is None:
            market = Market(
                chain_id=chain_eth.chain_id,
                protocol_id=protocol.protocol_id,
                native_market_key=native_market_key,
                market_address=native_market_key,
                market_kind="vault_underlying",
                display_name=f"Structured {token.symbol}",
                base_asset_token_id=token.token_id,
            )
            session.add(market)
            session.flush()
        return market

    market_usdc = _market(token_usdc)
    market_savusd = _market(token_savusd)
    as_of_ts_utc = session.scalar(select(PortfolioPositionCurrent.as_of_ts_utc).limit(1))
    assert as_of_ts_utc is not None

    rows = [
        {
            "position_key": (
                "stakedao:ethereum:"
                f"{wallet_address}:"
                "0xad1dcc6ca212673d2dfd403a905a1ec57666d910:"
                "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
            ),
            "market_id": market_usdc.market_id,
            "supply_token_id": token_usdc.token_id,
            "supply_amount": Decimal("513508.118589"),
            "supply_usd": Decimal("513508.118589080781161246"),
        },
        {
            "position_key": (
                "stakedao:ethereum:"
                f"{wallet_address}:"
                "0xad1dcc6ca212673d2dfd403a905a1ec57666d910:"
                "0xb8d89678e75a973e74698c976716308abb8a46a4"
            ),
            "market_id": market_savusd.market_id,
            "supply_token_id": token_savusd.token_id,
            "supply_amount": Decimal("474071.293750507199246758"),
            "supply_usd": Decimal("474071.293750507199246758"),
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
            chain_id=chain_eth.chain_id,
            market_id=row["market_id"],
            exposure_class="core_lending",
            status="open",
            display_name="Stake DAO underlying",
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
                chain_id=chain_eth.chain_id,
                market_exposure_id=None,
                scope_segment="strategy_only",
                supply_token_id=row["supply_token_id"],
                borrow_token_id=None,
                supply_amount=row["supply_amount"],
                supply_usd=row["supply_usd"],
                supply_apy=Decimal("0"),
                reward_apy=Decimal("0"),
                borrow_amount=Decimal("0"),
                borrow_usd=Decimal("0"),
                borrow_apy=Decimal("0"),
                net_equity_usd=row["supply_usd"],
                leverage_ratio=Decimal("1"),
                health_factor=None,
                gross_yield_daily_usd=Decimal("0"),
                net_yield_daily_usd=Decimal("0"),
                gross_yield_mtd_usd=Decimal("0"),
                net_yield_mtd_usd=Decimal("0"),
                strategy_fee_daily_usd=Decimal("0"),
                avant_gop_daily_usd=Decimal("0"),
                strategy_fee_mtd_usd=Decimal("0"),
                avant_gop_mtd_usd=Decimal("0"),
                gross_roe=None,
                net_roe=None,
            )
        )
    session.commit()

    data = client.get(
        f"/portfolio/positions/current?wallet_address={wallet_address}&protocol_code=stakedao"
    ).json()

    assert data["total_count"] == 1
    row = data["positions"][0]
    assert row["position_key"].startswith("curated-vault:stakedao:ethereum:")
    assert row["display_name"] == "USDC + savUSD Stakedao-Ethereum"
    assert row["position_kind"] == "Curated Vault"
    assert [leg["symbol"] for leg in row["supply_legs"]] == ["USDC", "savUSD"]
    assert row["borrow_legs"] == []
    assert Decimal(row["net_equity_usd"]) == Decimal("987579.412339587980408004")
