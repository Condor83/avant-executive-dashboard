"""Minimal CLI skeleton for local development and CI smoke checks."""

import typer

from core.settings import get_settings

app = typer.Typer(add_completion=False, help="Avant executive dashboard command line interface.")


@app.command("show-config")
def show_config() -> None:
    """Print a minimal runtime configuration summary."""

    settings = get_settings()
    typer.echo(f"app_env={settings.app_env}")
    typer.echo(f"database_url={settings.database_url}")


def main() -> None:
    """Run the CLI application."""

    app()


if __name__ == "__main__":
    main()
