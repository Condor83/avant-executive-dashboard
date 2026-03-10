"""CLI regression tests for sync command exit behavior."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from typer.testing import CliRunner

from core.cli import app
from core.consumer_debank_visibility import ConsumerDebankVisibilitySyncSummary
from core.holder_supply import HolderSupplySyncSummary
from core.runner import RunnerSummary


class DummyRunner:
    def __init__(self, result: RunnerSummary) -> None:
        self._result = result

    def sync_snapshot(self, *, as_of_ts_utc: datetime) -> RunnerSummary:
        del as_of_ts_utc
        return self._result


class DummySession:
    def __init__(self) -> None:
        self.commit_calls = 0
        self.closed = False

    def commit(self) -> None:
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


class DummyCloseable:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True


class DummyScalarResult:
    def __init__(self, rows: list[str]) -> None:
        self._rows = rows

    def all(self) -> list[str]:
        return self._rows


class DummySeedSession:
    def __init__(self, wallet_addresses: list[str]) -> None:
        self._wallet_addresses = wallet_addresses
        self.commit_calls = 0
        self.closed = False

    def __enter__(self) -> DummySeedSession:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        del exc_type, exc, tb
        self.closed = True

    def scalars(self, _stmt) -> DummyScalarResult:
        return DummyScalarResult(self._wallet_addresses)

    def commit(self) -> None:
        self.commit_calls += 1


def test_sync_snapshot_exits_non_zero_after_committing_partial_results(monkeypatch) -> None:
    session = DummySession()
    closeable = DummyCloseable()
    result_summary = RunnerSummary(rows_written=2, issues_written=1, component_failures=1)

    monkeypatch.setattr(
        "core.cli._build_runner",
        lambda markets_path, consumer_markets_path, *, progress_callback=None: (
            DummyRunner(result_summary),
            session,
            [closeable],
        ),
    )

    result = CliRunner().invoke(app, ["sync", "snapshot", "--as-of", "2026-03-03T12:00:00Z"])

    assert result.exit_code == 1
    assert "component_failures=1" in result.output
    assert session.commit_calls == 1
    assert session.closed is True
    assert closeable.closed is True


def test_sync_consumer_holder_snapshots_reads_cohort_and_runs_snapshot(monkeypatch) -> None:
    seed_session = DummySeedSession(["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"])
    runner_session = DummySession()
    closeable = DummyCloseable()
    result_summary = RunnerSummary(rows_written=4, issues_written=0, component_failures=0)
    captured_wallet_addresses: list[str] = []

    monkeypatch.setattr("core.cli.load_markets_config", lambda path: object())
    monkeypatch.setattr("core.cli.load_consumer_markets_config", lambda path: object())
    monkeypatch.setattr("core.cli.load_wallet_products_config", lambda path: object())
    monkeypatch.setattr("core.cli.load_avant_tokens_config", lambda path: object())
    monkeypatch.setattr("core.cli.get_engine", lambda: object())
    monkeypatch.setattr("core.cli.Session", lambda engine: seed_session)
    monkeypatch.setattr("core.cli.seed_database", lambda **kwargs: {})
    monkeypatch.setattr(
        "core.cli._build_customer_snapshot_runner",
        lambda *, business_date, wallet_addresses, markets_path, consumer_markets_path, avant_tokens_path, progress_callback=None: (
            captured_wallet_addresses.extend(wallet_addresses) or DummyRunner(result_summary),
            runner_session,
            [closeable],
        ),
    )

    result = CliRunner().invoke(
        app,
        ["sync", "consumer-holder-snapshots", "--date", "2026-03-03"],
    )

    assert result.exit_code == 0
    assert "rows_written=4" in result.output
    assert captured_wallet_addresses == ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    assert seed_session.commit_calls == 1
    assert seed_session.closed is True
    assert runner_session.commit_calls == 1
    assert runner_session.closed is True
    assert closeable.closed is True


def test_sync_consumer_debank_visibility_runs_union_snapshot(monkeypatch) -> None:
    session = DummySession()
    closeable = DummyCloseable()
    captured: dict[str, object] = {}
    summary = ConsumerDebankVisibilitySyncSummary(
        business_date=date(2026, 3, 9),
        as_of_ts_utc=datetime(2026, 3, 10, 6, 0),
        union_wallet_count=154,
        seed_wallet_count=145,
        verified_cohort_wallet_count=57,
        signoff_cohort_wallet_count=44,
        new_discovered_not_in_seed_count=9,
        fetched_wallet_count=154,
        fetch_error_count=0,
        active_wallet_count=111,
        borrow_wallet_count=38,
        configured_surface_wallet_count=12,
        wallet_rows_written=154,
        protocol_rows_written=220,
        issues_written=0,
    )

    monkeypatch.setattr("core.cli.get_engine", lambda: object())
    monkeypatch.setattr("core.cli.Session", lambda engine: session)
    monkeypatch.setattr("core.cli.load_consumer_markets_config", lambda path: object())
    monkeypatch.setattr(
        "core.cli.get_settings",
        lambda: type(
            "Settings",
            (),
            {
                "debank_cloud_api_key": "test-key",
                "debank_cloud_base_url": "https://pro-openapi.debank.com",
                "request_timeout_seconds": 15.0,
            },
        )(),
    )
    monkeypatch.setattr("core.cli.DebankCloudClient", lambda **kwargs: closeable)
    monkeypatch.setattr(
        "core.cli.sync_consumer_debank_visibility",
        lambda **kwargs: captured.update(kwargs) or summary,
    )

    result = CliRunner().invoke(
        app,
        ["sync", "consumer-debank-visibility", "--date", "2026-03-09"],
    )

    assert result.exit_code == 0
    assert "union_wallets=154" in result.output
    assert captured["business_date"] == date(2026, 3, 9)
    assert captured["config_dir"] == Path("config")
    assert captured["max_concurrency"] == 6
    assert session.commit_calls == 1
    assert session.closed is True
    assert closeable.closed is True


def test_sync_holder_supply_inputs_runs_routescan_and_debank(monkeypatch) -> None:
    session = DummySession()
    routescan = DummyCloseable()
    debank = DummyCloseable()
    price_oracle = DummyCloseable()
    captured: dict[str, object] = {}
    markets_config = object()
    consumer_markets_config = object()
    wallet_products_config = object()
    avant_tokens = object()
    thresholds = object()
    holder_exclusions = object()
    summary = HolderSupplySyncSummary(
        business_date=date(2026, 3, 9),
        as_of_ts_utc=datetime(2026, 3, 10, 6, 0),
        chain_code="avalanche",
        token_symbol="savUSD",
        raw_holder_rows=763,
        monitoring_wallet_count=154,
        holder_rows_written=763,
        debank_token_rows_written=97,
        debank_wallets_scanned=154,
        issues_written=0,
    )

    monkeypatch.setattr("core.cli.get_engine", lambda: object())
    monkeypatch.setattr("core.cli.Session", lambda engine: session)
    monkeypatch.setattr("core.cli.load_markets_config", lambda path: markets_config)
    monkeypatch.setattr(
        "core.cli.load_consumer_markets_config", lambda path: consumer_markets_config
    )
    monkeypatch.setattr("core.cli.load_wallet_products_config", lambda path: wallet_products_config)
    monkeypatch.setattr("core.cli.load_avant_tokens_config", lambda path: avant_tokens)
    monkeypatch.setattr("core.cli.load_consumer_thresholds_config", lambda path: thresholds)
    monkeypatch.setattr("core.cli.load_holder_exclusions_config", lambda path: holder_exclusions)
    monkeypatch.setattr(
        "core.cli.get_settings",
        lambda: type(
            "Settings",
            (),
            {
                "debank_cloud_api_key": "test-key",
                "debank_cloud_base_url": "https://pro-openapi.debank.com",
                "defillama_base_url": "https://coins.llama.fi",
                "avant_api_base_url": "https://app.avantprotocol.com",
                "request_timeout_seconds": 15.0,
            },
        )(),
    )
    monkeypatch.setattr("core.cli.RouteScanClient", lambda **kwargs: routescan)
    monkeypatch.setattr("core.cli.DebankCloudClient", lambda **kwargs: debank)
    monkeypatch.setattr("core.cli.PriceOracle", lambda **kwargs: price_oracle)
    monkeypatch.setattr(
        "core.cli.sync_holder_supply_inputs",
        lambda **kwargs: captured.update(kwargs) or summary,
    )

    result = CliRunner().invoke(
        app,
        ["sync", "holder-supply-inputs", "--date", "2026-03-09"],
    )

    assert result.exit_code == 0
    assert "raw_holder_rows=763" in result.output
    assert "debank_wallets_scanned=154" in result.output
    assert captured["business_date"] == date(2026, 3, 9)
    assert captured["markets_path"] == Path("config/markets.yaml")
    assert captured["markets_config"] is markets_config
    assert captured["consumer_markets_config"] is consumer_markets_config
    assert captured["wallet_products_config"] is wallet_products_config
    assert captured["avant_tokens"] is avant_tokens
    assert captured["thresholds"] is thresholds
    assert captured["holder_exclusions"] is holder_exclusions
    assert session.commit_calls == 1
    assert session.closed is True
    assert routescan.closed is True
    assert debank.closed is True
    assert price_oracle.closed is True


def test_compute_holder_scorecard_runs_scorecard_and_executive(monkeypatch) -> None:
    session = DummySession()

    class _SupplyCoverageSummary:
        business_date = date(2026, 3, 9)
        rows_written = 1

    class _ScorecardSummary:
        business_date = date(2026, 3, 9)
        scorecard_rows_written = 1
        protocol_gap_rows_written = 4

    class _ExecutiveSummary:
        rows_written = 1

    class _DashboardSummary:
        segment_rows_written = 4
        protocol_rows_written = 2

    class _HolderScorecardEngine:
        def __init__(self, _session, *, thresholds) -> None:
            self.thresholds = thresholds

        def compute_daily(
            self,
            *,
            business_date: date,
            write_protocol_gaps: bool = True,
        ) -> _ScorecardSummary:
            assert business_date == date(2026, 3, 9)
            assert self.thresholds == "thresholds"
            assert write_protocol_gaps is False
            return _ScorecardSummary()

    class _ExecutiveSummaryEngine:
        def __init__(self, _session) -> None:
            pass

        def compute_daily(self, *, business_date: date) -> _ExecutiveSummary:
            assert business_date == date(2026, 3, 9)
            return _ExecutiveSummary()

    class _HolderSupplyCoverageEngine:
        def __init__(self, _session, *, avant_tokens, thresholds) -> None:
            assert avant_tokens == "avant_tokens"
            assert thresholds == "thresholds"

        def compute_daily(self, *, business_date: date) -> _SupplyCoverageSummary:
            assert business_date == date(2026, 3, 9)
            return _SupplyCoverageSummary()

    class _HolderDashboardEngine:
        def __init__(self, _session, *, avant_tokens, thresholds, holder_protocol_map) -> None:
            assert avant_tokens == "avant_tokens"
            assert thresholds == "thresholds"
            assert holder_protocol_map == "holder_protocol_map"

        def compute_daily(self, *, business_date: date) -> _DashboardSummary:
            assert business_date == date(2026, 3, 9)
            return _DashboardSummary()

    monkeypatch.setattr("core.cli.load_avant_tokens_config", lambda path: "avant_tokens")
    monkeypatch.setattr("core.cli.load_consumer_thresholds_config", lambda path: "thresholds")
    monkeypatch.setattr(
        "core.cli.load_holder_protocol_map_config",
        lambda path: "holder_protocol_map",
    )
    monkeypatch.setattr("core.cli.get_engine", lambda: object())
    monkeypatch.setattr("core.cli.Session", lambda engine: session)
    monkeypatch.setattr("core.cli.HolderSupplyCoverageEngine", _HolderSupplyCoverageEngine)
    monkeypatch.setattr("core.cli.HolderScorecardEngine", _HolderScorecardEngine)
    monkeypatch.setattr("core.cli.HolderDashboardEngine", _HolderDashboardEngine)
    monkeypatch.setattr("core.cli.ExecutiveSummaryEngine", _ExecutiveSummaryEngine)

    result = CliRunner().invoke(
        app,
        ["compute", "holder-scorecard", "--date", "2026-03-09"],
    )

    assert result.exit_code == 0
    assert "supply_rows_written=1" in result.output
    assert "scorecard_rows_written=1" in result.output
    assert "protocol_gap_rows_written=4" in result.output
    assert "dashboard_segment_rows=4" in result.output
    assert "dashboard_deploy_rows=2" in result.output
    assert session.commit_calls == 1
    assert session.closed is True
