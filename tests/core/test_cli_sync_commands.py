"""CLI regression tests for sync command exit behavior."""

from __future__ import annotations

from datetime import datetime

from typer.testing import CliRunner

from core.cli import app
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
