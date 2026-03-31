"""Shared CLI helpers."""

from __future__ import annotations

from pathlib import Path

import typer
from rich import print as rprint

from alma_atlas.config import get_config
from alma_atlas.graph_service import require_db_path


def require_db_path_or_exit() -> Path:
    """Return the configured Atlas DB path or exit with a standard message."""
    cfg = get_config()
    try:
        return require_db_path(cfg)
    except ValueError as exc:
        rprint(f"[yellow]{exc}[/yellow]")
        raise typer.Exit(1) from exc
