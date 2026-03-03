"""Sprint 00 smoke tests."""

from __future__ import annotations

import subprocess
import sys

import adapters
import analytics
import api
import core

# Keep imported modules referenced so static checks treat them as intentional imports.
_IMPORTED_MODULES = (core, adapters, analytics, api)


def test_package_modules_import() -> None:
    """Core package modules import successfully."""

    assert len(_IMPORTED_MODULES) == 4


def test_cli_help_runs() -> None:
    """CLI entrypoint returns help output without errors."""

    result = subprocess.run(
        [sys.executable, "-m", "core.cli", "--help"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0
    assert "Usage:" in result.stdout
    assert "--help" in result.stdout
