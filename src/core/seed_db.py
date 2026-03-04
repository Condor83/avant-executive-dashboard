"""Seed canonical dimension tables from config files."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from core.config import (
    ConsumerMarketsConfig,
    MarketsConfig,
    WalletProductsConfig,
    canonical_address,
    load_consumer_markets_config,
    load_markets_config,
    load_wallet_products_config,
)
from core.db.models import Chain, Market, Product, Protocol, Token, Wallet, WalletProductMap
from core.db.session import get_engine


@dataclass
class TableSeedStats:
    """Simple before/after summary for a seeded table."""

    rows_seen: int
    before_count: int
    after_count: int

    @property
    def inserted(self) -> int:
        return self.after_count - self.before_count

    @property
    def skipped(self) -> int:
        return max(self.rows_seen - self.inserted, 0)


def _count_rows(session: Session, model: type[Any]) -> int:
    return session.scalar(select(func.count()).select_from(model)) or 0


def _upsert_do_nothing(
    session: Session,
    model: type[Any],
    rows: list[dict[str, Any]],
    conflict_columns: list[str],
) -> TableSeedStats:
    if not rows:
        before = _count_rows(session, model)
        return TableSeedStats(rows_seen=0, before_count=before, after_count=before)

    before = _count_rows(session, model)
    stmt = insert(model).values(rows)
    stmt = stmt.on_conflict_do_nothing(index_elements=conflict_columns)
    session.execute(stmt)
    after = _count_rows(session, model)
    return TableSeedStats(rows_seen=len(rows), before_count=before, after_count=after)


def _upsert_wallet_product_map(session: Session, rows: list[dict[str, Any]]) -> TableSeedStats:
    if not rows:
        before = _count_rows(session, WalletProductMap)
        return TableSeedStats(rows_seen=0, before_count=before, after_count=before)

    before = _count_rows(session, WalletProductMap)
    stmt = insert(WalletProductMap).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[WalletProductMap.wallet_id],
        set_={"product_id": stmt.excluded.product_id},
    )
    session.execute(stmt)
    after = _count_rows(session, WalletProductMap)
    return TableSeedStats(rows_seen=len(rows), before_count=before, after_count=after)


def _upsert_markets(session: Session, rows: list[dict[str, Any]]) -> TableSeedStats:
    if not rows:
        before = _count_rows(session, Market)
        return TableSeedStats(rows_seen=0, before_count=before, after_count=before)

    before = _count_rows(session, Market)
    stmt = insert(Market).values(rows)
    stmt = stmt.on_conflict_do_update(
        index_elements=[Market.chain_id, Market.protocol_id, Market.market_address],
        set_={
            "base_asset_token_id": stmt.excluded.base_asset_token_id,
            "collateral_token_id": stmt.excluded.collateral_token_id,
            "metadata_json": stmt.excluded.metadata_json,
        },
    )
    session.execute(stmt)
    after = _count_rows(session, Market)
    return TableSeedStats(rows_seen=len(rows), before_count=before, after_count=after)


def _dedupe(rows: list[dict[str, Any]], keys: tuple[str, ...]) -> list[dict[str, Any]]:
    deduped: dict[tuple[Any, ...], dict[str, Any]] = {}
    for row in rows:
        dedupe_key = tuple(row[key] for key in keys)
        deduped[dedupe_key] = row
    return list(deduped.values())


def _resolve_id_map(
    session: Session, model: type[Any], key_col: str, id_col: str
) -> dict[str, int]:
    key_column = getattr(model, key_col)
    id_column = getattr(model, id_col)
    rows = session.execute(select(key_column, id_column)).all()
    return {str(key): int(value) for key, value in rows}


def _normalize_token_address(value: str) -> str:
    cleaned = value.strip()
    if cleaned.startswith("0x"):
        return cleaned.lower()
    return cleaned


def _collect_chain_codes(markets: MarketsConfig, consumer: ConsumerMarketsConfig) -> set[str]:
    chains: set[str] = set()
    for section in (
        markets.aave_v3,
        markets.spark,
        markets.morpho,
        markets.euler_v2,
        markets.dolomite,
        markets.kamino,
        markets.zest,
        markets.wallet_balances,
    ):
        chains.update(section.keys())
    for market in consumer.markets:
        chains.add(market.chain)
    return chains


def _collect_protocol_codes(markets: MarketsConfig, consumer: ConsumerMarketsConfig) -> set[str]:
    protocols = {
        "aave_v3",
        "spark",
        "morpho",
        "euler_v2",
        "dolomite",
        "kamino",
        "zest",
        "wallet_balances",
    }
    for market in consumer.markets:
        protocols.add(market.protocol)
    return protocols


def _collect_wallet_rows(
    markets: MarketsConfig, wallet_products: WalletProductsConfig
) -> list[dict[str, Any]]:
    wallets: list[dict[str, Any]] = []

    def add_wallet(address: str, wallet_type: str = "strategy") -> None:
        wallets.append(
            {"address": canonical_address(address), "wallet_type": wallet_type, "label": None}
        )

    for chains in (
        markets.aave_v3,
        markets.spark,
        markets.morpho,
        markets.euler_v2,
        markets.dolomite,
        markets.kamino,
        markets.zest,
        markets.wallet_balances,
    ):
        for chain_config in chains.values():
            for wallet in chain_config.wallets:
                add_wallet(wallet, wallet_type="strategy")

    for assignment in wallet_products.assignments:
        add_wallet(assignment.wallet_address, wallet_type=assignment.wallet_type)

    return _dedupe(wallets, keys=("address",))


def _collect_token_rows(
    markets: MarketsConfig, consumer: ConsumerMarketsConfig
) -> list[dict[str, Any]]:
    token_rows: list[dict[str, Any]] = []

    def add_token(chain_code: str, address_or_mint: str, symbol: str, decimals: int) -> None:
        token_rows.append(
            {
                "chain_code": chain_code,
                "address_or_mint": _normalize_token_address(address_or_mint),
                "symbol": symbol,
                "decimals": int(decimals),
            }
        )

    for chain_code, aave_chain in markets.aave_v3.items():
        for aave_market in aave_chain.markets:
            add_token(chain_code, aave_market.asset, aave_market.symbol, aave_market.decimals)

    for chain_code, spark_chain in markets.spark.items():
        for spark_market in spark_chain.markets:
            add_token(chain_code, spark_market.asset, spark_market.symbol, spark_market.decimals)

    for chain_code, morpho_chain in markets.morpho.items():
        for morpho_vault in morpho_chain.vaults:
            if (
                morpho_vault.asset_address is None
                or morpho_vault.asset_symbol is None
                or morpho_vault.asset_decimals is None
            ):
                continue
            add_token(
                chain_code,
                morpho_vault.asset_address,
                morpho_vault.asset_symbol,
                morpho_vault.asset_decimals,
            )

    for chain_code, wallet_balance_chain in markets.wallet_balances.items():
        for token in wallet_balance_chain.tokens:
            add_token(chain_code, token.address, token.symbol, token.decimals)

    for chain_code, euler_chain in markets.euler_v2.items():
        for vault in euler_chain.vaults:
            add_token(
                chain_code,
                vault.asset_address,
                vault.asset_symbol,
                vault.asset_decimals,
            )

    for chain_code, kamino_chain in markets.kamino.items():
        for kamino_market in kamino_chain.markets:
            if kamino_market.supply_token is not None:
                add_token(
                    chain_code,
                    kamino_market.supply_token.mint,
                    kamino_market.supply_token.symbol,
                    kamino_market.supply_token.decimals,
                )
            if kamino_market.borrow_token is not None:
                add_token(
                    chain_code,
                    kamino_market.borrow_token.mint,
                    kamino_market.borrow_token.symbol,
                    kamino_market.borrow_token.decimals,
                )

    for chain_code, zest_chain in markets.zest.items():
        for zest_market in zest_chain.markets:
            add_token(
                chain_code, zest_market.asset_contract, zest_market.symbol, zest_market.decimals
            )

    for consumer_market in consumer.markets:
        add_token(
            consumer_market.chain,
            consumer_market.collateral_token.address,
            consumer_market.collateral_token.symbol,
            consumer_market.collateral_token.decimals,
        )
        add_token(
            consumer_market.chain,
            consumer_market.borrow_token.address,
            consumer_market.borrow_token.symbol,
            consumer_market.borrow_token.decimals,
        )

    return _dedupe(token_rows, keys=("chain_code", "address_or_mint"))


def _collect_market_rows(
    markets: MarketsConfig,
    consumer: ConsumerMarketsConfig,
    chain_ids: dict[str, int],
    protocol_ids: dict[str, int],
    token_ids: dict[tuple[str, str], int],
    token_ids_by_symbol: dict[tuple[str, str], int],
) -> list[dict[str, Any]]:
    market_rows: list[dict[str, Any]] = []

    def add_market(
        protocol_code: str,
        chain_code: str,
        market_address: str,
        base_asset_token_id: int | None,
        collateral_token_id: int | None,
        metadata: dict[str, Any],
    ) -> None:
        market_rows.append(
            {
                "protocol_id": protocol_ids[protocol_code],
                "chain_id": chain_ids[chain_code],
                "market_address": _normalize_token_address(market_address),
                "base_asset_token_id": base_asset_token_id,
                "collateral_token_id": collateral_token_id,
                "metadata_json": metadata,
            }
        )

    for chain_code, aave_chain in markets.aave_v3.items():
        for aave_market in aave_chain.markets:
            token_id = token_ids.get((chain_code, _normalize_token_address(aave_market.asset)))
            add_market(
                protocol_code="aave_v3",
                chain_code=chain_code,
                market_address=aave_market.asset,
                base_asset_token_id=token_id,
                collateral_token_id=None,
                metadata={
                    "symbol": aave_market.symbol,
                    "decimals": aave_market.decimals,
                    "kind": "reserve",
                },
            )

    for chain_code, spark_chain in markets.spark.items():
        for spark_market in spark_chain.markets:
            token_id = token_ids.get((chain_code, _normalize_token_address(spark_market.asset)))
            add_market(
                protocol_code="spark",
                chain_code=chain_code,
                market_address=spark_market.asset,
                base_asset_token_id=token_id,
                collateral_token_id=None,
                metadata={
                    "symbol": spark_market.symbol,
                    "decimals": spark_market.decimals,
                    "kind": "reserve",
                },
            )

    for chain_code, morpho_chain in markets.morpho.items():
        for morpho_market in morpho_chain.markets:
            loan_token_id = token_ids_by_symbol.get((chain_code, morpho_market.loan_token.upper()))
            collateral_token_id = token_ids_by_symbol.get(
                (chain_code, morpho_market.collateral_token.upper())
            )
            add_market(
                protocol_code="morpho",
                chain_code=chain_code,
                market_address=morpho_market.id,
                base_asset_token_id=loan_token_id,
                collateral_token_id=collateral_token_id,
                metadata={
                    "loan_token": morpho_market.loan_token,
                    "collateral_token": morpho_market.collateral_token,
                    "loan_decimals": morpho_market.loan_decimals,
                    "collateral_decimals": morpho_market.collateral_decimals,
                    "defillama_pool_id": morpho_market.defillama_pool_id,
                    "kind": "market",
                },
            )
        for morpho_vault in morpho_chain.vaults:
            base_asset_token_id: int | None = None
            if morpho_vault.asset_address is not None:
                base_asset_token_id = token_ids.get(
                    (chain_code, _normalize_token_address(morpho_vault.asset_address))
                )
            add_market(
                protocol_code="morpho",
                chain_code=chain_code,
                market_address=morpho_vault.address,
                base_asset_token_id=base_asset_token_id,
                collateral_token_id=None,
                metadata={
                    "kind": "vault",
                    "note": morpho_vault.note,
                    "asset_symbol": morpho_vault.asset_symbol,
                    "asset_address": (
                        _normalize_token_address(morpho_vault.asset_address)
                        if morpho_vault.asset_address is not None
                        else None
                    ),
                    "asset_decimals": morpho_vault.asset_decimals,
                },
            )

    for chain_code, euler_chain in markets.euler_v2.items():
        for euler_vault in euler_chain.vaults:
            token_id = token_ids.get(
                (chain_code, _normalize_token_address(euler_vault.asset_address))
            )
            add_market(
                protocol_code="euler_v2",
                chain_code=chain_code,
                market_address=euler_vault.address,
                base_asset_token_id=token_id,
                collateral_token_id=None,
                metadata={
                    "symbol": euler_vault.symbol,
                    "asset_symbol": euler_vault.asset_symbol,
                    "asset_address": _normalize_token_address(euler_vault.asset_address),
                    "asset_decimals": euler_vault.asset_decimals,
                    "kind": "vault",
                },
            )

    for chain_code, dolomite_chain in markets.dolomite.items():
        for dolomite_market in dolomite_chain.markets:
            token_id = token_ids_by_symbol.get((chain_code, dolomite_market.symbol.upper()))
            add_market(
                protocol_code="dolomite",
                chain_code=chain_code,
                market_address=str(dolomite_market.id),
                base_asset_token_id=token_id,
                collateral_token_id=None,
                metadata={
                    "symbol": dolomite_market.symbol,
                    "decimals": dolomite_market.decimals,
                    "kind": "market",
                },
            )

    for chain_code, kamino_chain in markets.kamino.items():
        for kamino_market in kamino_chain.markets:
            supply_token_id: int | None = None
            if kamino_market.supply_token is not None:
                supply_token_id = token_ids.get(
                    (chain_code, _normalize_token_address(kamino_market.supply_token.mint))
                )
            borrow_token_id: int | None = None
            if kamino_market.borrow_token is not None:
                borrow_token_id = token_ids.get(
                    (chain_code, _normalize_token_address(kamino_market.borrow_token.mint))
                )
            add_market(
                protocol_code="kamino",
                chain_code=chain_code,
                market_address=kamino_market.market_pubkey,
                # For dual-token markets we normalize borrow as base + supply as collateral.
                base_asset_token_id=borrow_token_id,
                collateral_token_id=supply_token_id,
                metadata={
                    "name": kamino_market.name,
                    "kind": "market",
                    "supply_token_symbol": (
                        kamino_market.supply_token.symbol
                        if kamino_market.supply_token is not None
                        else None
                    ),
                    "borrow_token_symbol": (
                        kamino_market.borrow_token.symbol
                        if kamino_market.borrow_token is not None
                        else None
                    ),
                },
            )

    for chain_code, zest_chain in markets.zest.items():
        for zest_market in zest_chain.markets:
            token_id = token_ids.get(
                (chain_code, _normalize_token_address(zest_market.asset_contract))
            )
            add_market(
                protocol_code="zest",
                chain_code=chain_code,
                market_address=zest_market.asset_contract,
                base_asset_token_id=token_id,
                collateral_token_id=None,
                metadata={
                    "symbol": zest_market.symbol,
                    "z_token": zest_market.z_token,
                    "borrow_fn": zest_market.borrow_fn,
                    "decimals": zest_market.decimals,
                    "kind": "market",
                },
            )

    for chain_code, wallet_balance_chain in markets.wallet_balances.items():
        for token in wallet_balance_chain.tokens:
            token_address = _normalize_token_address(token.address)
            token_id = token_ids.get((chain_code, token_address))
            add_market(
                protocol_code="wallet_balances",
                chain_code=chain_code,
                market_address=token_address,
                base_asset_token_id=token_id,
                collateral_token_id=None,
                metadata={
                    "symbol": token.symbol,
                    "decimals": token.decimals,
                    "kind": "wallet_balance_token",
                },
            )

    for consumer_market in consumer.markets:
        base_token_id = token_ids.get(
            (consumer_market.chain, _normalize_token_address(consumer_market.borrow_token.address))
        )
        collateral_token_id = token_ids.get(
            (
                consumer_market.chain,
                _normalize_token_address(consumer_market.collateral_token.address),
            )
        )
        add_market(
            protocol_code=consumer_market.protocol,
            chain_code=consumer_market.chain,
            market_address=consumer_market.market_address,
            base_asset_token_id=base_token_id,
            collateral_token_id=collateral_token_id,
            metadata={
                "name": consumer_market.name,
                "kind": "consumer_market",
                "borrow_token_symbol": consumer_market.borrow_token.symbol,
                "collateral_token_symbol": consumer_market.collateral_token.symbol,
            },
        )

    return _dedupe(market_rows, keys=("chain_id", "protocol_id", "market_address"))


def truncate_dimension_tables(session: Session, dry_run: bool = False) -> None:
    """Truncate dimension tables in dependency-safe order."""

    sql = """
    TRUNCATE TABLE
      wallet_product_map,
      markets,
      prices,
      market_snapshots,
      position_snapshots,
      tokens,
      wallets,
      products,
      chains,
      protocols
    RESTART IDENTITY CASCADE
    """
    if dry_run:
        return
    session.execute(text(sql))


def seed_database(
    session: Session,
    markets: MarketsConfig,
    wallet_products: WalletProductsConfig,
    consumer: ConsumerMarketsConfig,
) -> dict[str, TableSeedStats]:
    """Seed canonical dimensions deterministically and idempotently."""

    stats: dict[str, TableSeedStats] = {}

    chain_rows = [{"chain_code": code} for code in sorted(_collect_chain_codes(markets, consumer))]
    stats["chains"] = _upsert_do_nothing(session, Chain, chain_rows, ["chain_code"])
    chain_ids = _resolve_id_map(session, Chain, key_col="chain_code", id_col="chain_id")

    protocol_rows = [
        {"protocol_code": code} for code in sorted(_collect_protocol_codes(markets, consumer))
    ]
    stats["protocols"] = _upsert_do_nothing(session, Protocol, protocol_rows, ["protocol_code"])
    protocol_ids = _resolve_id_map(session, Protocol, key_col="protocol_code", id_col="protocol_id")

    product_rows = [
        {"product_code": product_code}
        for product_code in sorted(
            {assignment.product_code for assignment in wallet_products.assignments}
        )
    ]
    stats["products"] = _upsert_do_nothing(session, Product, product_rows, ["product_code"])
    product_ids = _resolve_id_map(session, Product, key_col="product_code", id_col="product_id")

    wallet_rows = _collect_wallet_rows(markets, wallet_products)
    stats["wallets"] = _upsert_do_nothing(session, Wallet, wallet_rows, ["address"])
    wallet_ids = _resolve_id_map(session, Wallet, key_col="address", id_col="wallet_id")

    token_candidates = _collect_token_rows(markets, consumer)
    token_rows = [
        {
            "chain_id": chain_ids[row["chain_code"]],
            "address_or_mint": row["address_or_mint"],
            "symbol": row["symbol"],
            "decimals": row["decimals"],
        }
        for row in token_candidates
    ]
    stats["tokens"] = _upsert_do_nothing(
        session,
        Token,
        token_rows,
        ["chain_id", "address_or_mint"],
    )

    token_result_rows = session.execute(
        select(Chain.chain_code, Token.address_or_mint, Token.token_id, Token.symbol)
        .join(Chain, Chain.chain_id == Token.chain_id)
        .order_by(Token.token_id)
    ).all()
    token_ids = {
        (chain_code, address_or_mint): token_id
        for chain_code, address_or_mint, token_id, _symbol in token_result_rows
    }
    token_ids_by_symbol = {
        (chain_code, symbol.upper()): token_id
        for chain_code, _address_or_mint, token_id, symbol in token_result_rows
    }

    market_rows = _collect_market_rows(
        markets,
        consumer,
        chain_ids=chain_ids,
        protocol_ids=protocol_ids,
        token_ids=token_ids,
        token_ids_by_symbol=token_ids_by_symbol,
    )
    stats["markets"] = _upsert_markets(session, market_rows)

    wallet_product_rows = [
        {
            "wallet_id": wallet_ids[canonical_address(assignment.wallet_address)],
            "product_id": product_ids[assignment.product_code],
        }
        for assignment in wallet_products.assignments
        if canonical_address(assignment.wallet_address) in wallet_ids
    ]
    wallet_product_rows = _dedupe(wallet_product_rows, keys=("wallet_id",))
    stats["wallet_product_map"] = _upsert_wallet_product_map(session, wallet_product_rows)

    return stats


def format_stats(stats: dict[str, TableSeedStats]) -> str:
    """Human-readable summary for CLI output."""

    lines = []
    for table_name in sorted(stats):
        item = stats[table_name]
        lines.append(
            f"{table_name}: before={item.before_count} after={item.after_count} "
            f"inserted={item.inserted} skipped={item.skipped}"
        )
    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the seeding script."""

    parser = argparse.ArgumentParser(
        description="Seed canonical dimensions from YAML config files."
    )
    parser.add_argument("--markets", type=Path, default=Path("config/markets.yaml"))
    parser.add_argument("--wallet-products", type=Path, default=Path("config/wallet_products.yaml"))
    parser.add_argument(
        "--consumer-markets", type=Path, default=Path("config/consumer_markets.yaml")
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--truncate-dimensions",
        action="store_true",
        help="truncate seeded tables before insertions",
    )
    return parser.parse_args()


def main() -> None:
    """CLI entrypoint."""

    args = parse_args()

    markets_cfg = load_markets_config(args.markets)
    wallet_products_cfg = load_wallet_products_config(args.wallet_products)
    consumer_cfg = load_consumer_markets_config(args.consumer_markets)

    engine = get_engine()
    with Session(engine) as session:
        if args.truncate_dimensions:
            truncate_dimension_tables(session=session, dry_run=args.dry_run)

        stats = seed_database(
            session=session,
            markets=markets_cfg,
            wallet_products=wallet_products_cfg,
            consumer=consumer_cfg,
        )

        if args.dry_run:
            session.rollback()
        else:
            session.commit()

    print(format_stats(stats))


if __name__ == "__main__":
    main()
