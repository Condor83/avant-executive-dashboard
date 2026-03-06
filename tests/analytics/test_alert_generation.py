"""Deterministic alert generation and lifecycle tests."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from analytics.alerts import AlertCandidate, AlertEngine
from analytics.risk_engine import RiskEngine
from analytics.yield_engine import denver_business_bounds_utc
from core.config import load_risk_thresholds_config
from core.db.models import (
    Alert,
    Chain,
    Market,
    MarketSnapshot,
    PositionSnapshot,
    Product,
    Protocol,
    Wallet,
    WalletProductMap,
)


def _migrate_to_head(database_url: str) -> None:
    config = Config("alembic.ini")
    config.set_main_option("sqlalchemy.url", database_url)
    command.upgrade(config, "head")


def _insert_market_snapshot(
    session: Session,
    *,
    as_of_ts_utc: datetime,
    market_id: int,
    utilization: Decimal,
    borrow_apy: Decimal,
    available_liquidity_usd: Decimal,
    total_supply_usd: Decimal = Decimal("100"),
) -> None:
    session.add(
        MarketSnapshot(
            as_of_ts_utc=as_of_ts_utc,
            block_number_or_slot="1",
            market_id=market_id,
            total_supply_usd=total_supply_usd,
            total_borrow_usd=total_supply_usd * utilization,
            utilization=utilization,
            supply_apy=Decimal("0.03"),
            borrow_apy=borrow_apy,
            available_liquidity_usd=available_liquidity_usd,
            caps_json=None,
            irm_params_json={"kink": "0.80"},
            source="rpc",
        )
    )


def _insert_position_snapshot(
    session: Session,
    *,
    as_of_ts_utc: datetime,
    wallet_id: int,
    market_id: int,
    position_key: str,
    supply_apy: Decimal,
    reward_apy: Decimal,
    borrow_apy: Decimal,
) -> None:
    session.add(
        PositionSnapshot(
            as_of_ts_utc=as_of_ts_utc,
            block_number_or_slot="1",
            wallet_id=wallet_id,
            market_id=market_id,
            position_key=position_key,
            supplied_amount=Decimal("100"),
            supplied_usd=Decimal("100"),
            borrowed_amount=Decimal("50"),
            borrowed_usd=Decimal("50"),
            supply_apy=supply_apy,
            borrow_apy=borrow_apy,
            reward_apy=reward_apy,
            equity_usd=Decimal("50"),
            health_factor=None,
            ltv=None,
            source="rpc",
        )
    )


def test_alert_rows_are_created_updated_and_resolved(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    thresholds = load_risk_thresholds_config(Path("config/risk_thresholds.yaml"))

    business_date = date(2026, 3, 3)
    sod_ts_utc, _ = denver_business_bounds_utc(business_date)
    previous_ts = sod_ts_utc - timedelta(hours=24)
    as_of_1 = sod_ts_utc + timedelta(hours=12)
    as_of_2 = sod_ts_utc + timedelta(hours=15)
    as_of_3 = sod_ts_utc + timedelta(hours=18)
    market_id = 0
    wallet_id = 0

    with Session(engine) as session:
        chain = Chain(chain_code="ethereum")
        protocol = Protocol(protocol_code="aave_v3")
        wallet = Wallet(
            address="0x1111111111111111111111111111111111111111",
            wallet_type="strategy",
        )
        product = Product(product_code="stablecoin_senior")
        session.add_all([chain, protocol, wallet, product])
        session.flush()

        market = Market(
            chain_id=chain.chain_id,
            protocol_id=protocol.protocol_id,
            market_address="0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            base_asset_token_id=None,
            collateral_token_id=None,
            metadata_json={"kind": "reserve"},
        )
        session.add(market)
        session.flush()
        market_id = market.market_id
        wallet_id = wallet.wallet_id

        session.add(WalletProductMap(wallet_id=wallet.wallet_id, product_id=product.product_id))

        _insert_market_snapshot(
            session,
            as_of_ts_utc=previous_ts,
            market_id=market_id,
            utilization=Decimal("0.50"),
            borrow_apy=Decimal("0.05"),
            available_liquidity_usd=Decimal("50"),
        )
        _insert_market_snapshot(
            session,
            as_of_ts_utc=as_of_1,
            market_id=market_id,
            utilization=Decimal("0.92"),
            borrow_apy=Decimal("0.12"),
            available_liquidity_usd=Decimal("5"),
        )
        _insert_position_snapshot(
            session,
            as_of_ts_utc=as_of_1,
            wallet_id=wallet_id,
            market_id=market_id,
            position_key="pos-risk",
            supply_apy=Decimal("0.03"),
            reward_apy=Decimal("0"),
            borrow_apy=Decimal("0.05"),
        )
        session.commit()

    with Session(engine) as session:
        risk_result_1 = RiskEngine(session, thresholds=thresholds).compute_for_date(
            business_date=business_date
        )
        summary_1 = AlertEngine(session, thresholds=thresholds).sync_candidates(
            as_of_ts_utc=risk_result_1.as_of_ts_utc,
            candidates=AlertEngine(session, thresholds=thresholds).build_candidates(risk_result_1),
        )
        session.commit()

        assert summary_1.opened == 4
        assert summary_1.updated == 0
        assert summary_1.resolved == 0
        assert summary_1.open_alerts == 4

        open_alerts = session.scalars(select(Alert).where(Alert.status == "open")).all()
        assert len(open_alerts) == 4
        assert {alert.alert_type for alert in open_alerts} == {
            "KINK_NEAR",
            "BORROW_RATE_SPIKE",
            "LIQUIDITY_SQUEEZE",
            "SPREAD_TOO_TIGHT",
        }

        market_alerts = [alert for alert in open_alerts if alert.entity_type == "market"]
        assert {alert.entity_id for alert in market_alerts} == {
            str(risk_result_1.market_rows[0].market_id)
        }

        position_alerts = [alert for alert in open_alerts if alert.entity_type == "position"]
        assert [alert.entity_id for alert in position_alerts] == ["pos-risk"]
        assert all(alert.severity == "high" for alert in open_alerts)

    with Session(engine) as session:
        _insert_market_snapshot(
            session,
            as_of_ts_utc=as_of_2,
            market_id=market_id,
            utilization=Decimal("0.91"),
            borrow_apy=Decimal("0.22"),
            available_liquidity_usd=Decimal("8"),
        )
        _insert_position_snapshot(
            session,
            as_of_ts_utc=as_of_2,
            wallet_id=wallet_id,
            market_id=market_id,
            position_key="pos-risk",
            supply_apy=Decimal("0.03"),
            reward_apy=Decimal("0"),
            borrow_apy=Decimal("0.045"),
        )
        session.commit()

    with Session(engine) as session:
        risk_result_2 = RiskEngine(session, thresholds=thresholds).compute_for_date(
            business_date=business_date
        )
        summary_2 = AlertEngine(session, thresholds=thresholds).sync_candidates(
            as_of_ts_utc=risk_result_2.as_of_ts_utc,
            candidates=AlertEngine(session, thresholds=thresholds).build_candidates(risk_result_2),
        )
        session.commit()

        assert summary_2.opened == 0
        assert summary_2.updated == 4
        assert summary_2.resolved == 0
        assert summary_2.open_alerts == 4

        alerts_after_update = session.scalars(select(Alert)).all()
        assert len(alerts_after_update) == 4
        assert all(alert.status == "open" for alert in alerts_after_update)

    with Session(engine) as session:
        _insert_market_snapshot(
            session,
            as_of_ts_utc=as_of_3,
            market_id=market_id,
            utilization=Decimal("0.70"),
            borrow_apy=Decimal("0.23"),
            available_liquidity_usd=Decimal("40"),
        )
        _insert_position_snapshot(
            session,
            as_of_ts_utc=as_of_3,
            wallet_id=wallet_id,
            market_id=market_id,
            position_key="pos-risk",
            supply_apy=Decimal("0.06"),
            reward_apy=Decimal("0.01"),
            borrow_apy=Decimal("0.03"),
        )
        session.commit()

    with Session(engine) as session:
        risk_result_3 = RiskEngine(session, thresholds=thresholds).compute_for_date(
            business_date=business_date
        )
        summary_3 = AlertEngine(session, thresholds=thresholds).sync_candidates(
            as_of_ts_utc=risk_result_3.as_of_ts_utc,
            candidates=AlertEngine(session, thresholds=thresholds).build_candidates(risk_result_3),
        )
        session.commit()

        assert summary_3.opened == 0
        assert summary_3.updated == 0
        assert summary_3.resolved == 4
        assert summary_3.open_alerts == 0

        all_alerts = session.scalars(select(Alert)).all()
        assert len(all_alerts) == 4
        assert all(alert.status == "resolved" for alert in all_alerts)


def test_sync_candidates_preserves_ack_status_on_upsert(postgres_database_url: str) -> None:
    _migrate_to_head(postgres_database_url)
    engine = create_engine(postgres_database_url)
    thresholds = load_risk_thresholds_config(Path("config/risk_thresholds.yaml"))
    first_ts = datetime(2026, 3, 3, 12, 0, tzinfo=UTC)
    second_ts = datetime(2026, 3, 3, 13, 0, tzinfo=UTC)

    with Session(engine) as session:
        session.add(
            Alert(
                ts_utc=first_ts,
                alert_type="KINK_NEAR",
                severity="med",
                entity_type="market",
                entity_id="market-1",
                payload_json={"version": 1},
                status="ack",
            )
        )
        session.commit()

    with Session(engine) as session:
        summary = AlertEngine(session, thresholds=thresholds).sync_candidates(
            as_of_ts_utc=second_ts,
            candidates=[
                AlertCandidate(
                    alert_type="KINK_NEAR",
                    severity="high",
                    entity_type="market",
                    entity_id="market-1",
                    payload_json={"version": 2},
                )
            ],
        )
        session.commit()

        alerts = session.scalars(
            select(Alert).where(
                Alert.alert_type == "KINK_NEAR",
                Alert.entity_type == "market",
                Alert.entity_id == "market-1",
            )
        ).all()

        assert summary.opened == 0
        assert summary.updated == 1
        assert summary.resolved == 0
        assert summary.open_alerts == 0
        assert len(alerts) == 1
        assert alerts[0].status == "ack"
        assert alerts[0].severity == "high"
        assert alerts[0].payload_json == {"version": 2}
        assert alerts[0].ts_utc == second_ts
