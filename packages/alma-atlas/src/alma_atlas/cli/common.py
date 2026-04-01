"""Shared CLI helpers."""

from __future__ import annotations

from pathlib import Path

import typer
from rich import print as rprint

from alma_atlas.application.query.service import require_db_path
from alma_atlas.bootstrap import load_config as get_config


def require_db_path_or_exit() -> Path:
    """Return the configured Atlas DB path or exit with a standard message."""
    cfg = get_config()
    try:
        return require_db_path(cfg)
    except ValueError as exc:
        rprint(f"[yellow]{exc}[/yellow]")
        raise typer.Exit(1) from exc
