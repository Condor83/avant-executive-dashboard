"""Seed idempotency tests."""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from core.config import (
    load_avant_tokens_config,
    load_consumer_markets_config,
    load_markets_config,
    load_wallet_products_config,
)
from core.db.models import Chain, Market, Product, Protocol, Token, Wallet, WalletProductMap
from core.seed_db import seed_database


def _dimension_counts(session: Session) -> dict[str, int]:
    return {
        "wallets": session.scalar(select(func.count()).select_from(Wallet)) or 0,
        "products": session.scalar(select(func.count()).select_from(Product)) or 0,
        "wallet_product_map": session.scalar(select(func.count()).select_from(WalletProductMap))
        or 0,
        "protocols": session.scalar(select(func.count()).select_from(Protocol)) or 0,
        "chains": session.scalar(select(func.count()).select_from(Chain)) or 0,
        "tokens": session.scalar(select(func.count()).select_from(Token)) or 0,
        "markets": session.scalar(select(func.count()).select_from(Market)) or 0,
    }


def test_seed_is_idempotent(postgres_database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", postgres_database_url)
    command.upgrade(config, "head")

    markets = load_markets_config(Path("config/markets.yaml"))
    wallet_products = load_wallet_products_config(Path("config/wallet_products.yaml"))
    consumer_markets = load_consumer_markets_config(Path("config/consumer_markets.yaml"))
    avant_tokens = load_avant_tokens_config(Path("config/avant_tokens.yaml"))

    from sqlalchemy import create_engine

    engine = create_engine(postgres_database_url)

    with Session(engine) as session:
        seed_database(
            session,
            markets=markets,
            wallet_products=wallet_products,
            consumer=consumer_markets,
            avant_tokens=avant_tokens,
        )
        session.commit()

    with Session(engine) as session:
        first_counts = _dimension_counts(session)

    with Session(engine) as session:
        seed_database(
            session,
            markets=markets,
            wallet_products=wallet_products,
            consumer=consumer_markets,
            avant_tokens=avant_tokens,
        )
        session.commit()

    with Session(engine) as session:
        second_counts = _dimension_counts(session)
        wallet_map_count = session.scalar(select(func.count()).select_from(WalletProductMap)) or 0
        distinct_wallets = session.execute(
            select(func.count()).select_from(
                select(WalletProductMap.wallet_id).distinct().subquery()
            )
        ).scalar_one()

        kamino_maple_market = session.execute(
            select(Market)
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .where(Protocol.protocol_code == "kamino")
            .where(Chain.chain_code == "solana")
            .where(Market.market_address == "6WEGfej9B9wjxRs6t4BYpb9iCXd8CpTpJ8fVSNzHCC5y")
        ).scalar_one()
        assert kamino_maple_market.collateral_token_id is not None
        assert kamino_maple_market.base_asset_token_id is not None

        base_token = session.get(Token, kamino_maple_market.base_asset_token_id)
        collateral_token = session.get(Token, kamino_maple_market.collateral_token_id)
        assert base_token is not None
        assert collateral_token is not None
        assert base_token.symbol == "PYUSD"
        assert collateral_token.symbol == "syrupUSDC"

        morpho_syrup_market = session.execute(
            select(Market)
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .where(Protocol.protocol_code == "morpho")
            .where(Chain.chain_code == "ethereum")
            .where(
                Market.market_address
                == "0x729badf297ee9f2f6b3f717b96fd355fc6ec00422284ce1968e76647b258cf44"
            )
        ).scalar_one()
        assert morpho_syrup_market.collateral_token_id is not None
        assert morpho_syrup_market.base_asset_token_id is not None

        morpho_syrup_collateral = session.get(Token, morpho_syrup_market.collateral_token_id)
        morpho_syrup_base = session.get(Token, morpho_syrup_market.base_asset_token_id)
        assert morpho_syrup_collateral is not None
        assert morpho_syrup_base is not None
        assert morpho_syrup_collateral.symbol == "syrupUSDC"
        assert morpho_syrup_base.symbol == "USDC"

        morpho_wbtc_market = session.execute(
            select(Market)
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .where(Protocol.protocol_code == "morpho")
            .where(Chain.chain_code == "ethereum")
            .where(
                Market.market_address
                == "0x704e020b95cbf452e7a30545d5f72a241c4238eebf9d1c67657fdd4a488581e0"
            )
        ).scalar_one()
        assert morpho_wbtc_market.collateral_token_id is not None
        morpho_wbtc_collateral = session.get(Token, morpho_wbtc_market.collateral_token_id)
        assert morpho_wbtc_collateral is not None
        assert morpho_wbtc_collateral.symbol == "WBTC"

        avusdx_market = session.execute(
            select(Market)
            .join(Protocol, Protocol.protocol_id == Market.protocol_id)
            .join(Chain, Chain.chain_id == Market.chain_id)
            .where(Protocol.protocol_code == "wallet_balances")
            .where(Chain.chain_code == "avalanche")
            .where(Market.market_address == "0xdd1cdfa52e7d8474d434cd016fd346701db6b3b9")
        ).scalar_one()
        assert avusdx_market.base_asset_token_id is not None
        assert avusdx_market.market_kind == "wallet_balance_token"
        assert isinstance(avusdx_market.metadata_json, dict)
        assert avusdx_market.metadata_json["asset_family"] == "usd"
        assert avusdx_market.metadata_json["wrapper_class"] == "boosted"

    assert first_counts == second_counts
    assert wallet_map_count == distinct_wallets
