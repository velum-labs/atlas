"""CLI commands for managing post-scan hooks.

Usage:
    alma-atlas hooks list                    # Show configured hooks from atlas.yml
    alma-atlas hooks test [hook-name]        # Fire a test event to verify connectivity
"""

from __future__ import annotations

import asyncio
from typing import Annotated

import typer
from rich import print as rprint
from rich.console import Console
from rich.table import Table

from alma_atlas.config import get_config

app = typer.Typer(help="Manage post-scan hooks.")
console = Console()


def _load_hooks_cfg(config_file: str | None):
    """Return AtlasConfig with hooks loaded from atlas.yml or default config."""
    if config_file:
        from alma_atlas.config import load_atlas_yml

        return load_atlas_yml(config_file)
    return get_config()


@app.command("list")
def list_hooks(
    config_file: Annotated[
        str | None,
        typer.Option("--config-file", help="Path to atlas.yml."),
    ] = None,
) -> None:
    """Show all configured post-scan hooks."""
    cfg = _load_hooks_cfg(config_file)

    if not cfg.hooks:
        rprint("[yellow]No hooks configured. Add a [bold]hooks[/bold] section to atlas.yml.[/yellow]")
        raise typer.Exit(0)

    table = Table(title="Post-Scan Hooks", show_lines=False)
    table.add_column("Name", style="cyan")
    table.add_column("Type", style="magenta")
    table.add_column("Events")
    table.add_column("URL / Target", style="dim")

    for hook in cfg.hooks:
        events_str = ", ".join(hook.events)
        target = hook.url or "(stdout)" if hook.type == "log" else hook.url or ""
        table.add_row(hook.name, hook.type, events_str, target or "")

    console.print(table)


@app.command("test")
def test_hook(
    hook_name: Annotated[
        str | None,
        typer.Argument(help="Name of the hook to test. Defaults to all hooks."),
    ] = None,
    config_file: Annotated[
        str | None,
        typer.Option("--config-file", help="Path to atlas.yml."),
    ] = None,
) -> None:
    """Fire a test event to verify hook connectivity."""
    from alma_atlas.hooks import HookEvent, HookExecutor

    cfg = _load_hooks_cfg(config_file)

    if not cfg.hooks:
        rprint("[yellow]No hooks configured.[/yellow]")
        raise typer.Exit(1)

    hooks_to_test = cfg.hooks
    if hook_name:
        hooks_to_test = [h for h in cfg.hooks if h.name == hook_name]
        if not hooks_to_test:
            rprint(f"[red]Hook not found:[/red] {hook_name!r}")
            raise typer.Exit(1)

    from datetime import UTC, datetime

    event = HookEvent(
        event_type="scan_complete",
        source_id="test",
        timestamp=datetime.now(UTC).isoformat(),
        data={"test": True, "asset_count": 0, "edge_count": 0},
    )

    # Temporarily override events so all hooks fire on scan_complete
    from alma_atlas.config import PostScanHook
    test_hooks = [
        PostScanHook(
            name=h.name,
            type=h.type,
            events=["scan_complete"],
            url=h.url,
            headers=h.headers,
        )
        for h in hooks_to_test
    ]

    executor = HookExecutor(test_hooks)
    results = asyncio.run(executor.fire(event))

    all_ok = True
    for result in results:
        if result.success:
            rprint(f"  [green]✓[/green] {result.hook_name} — OK")
        else:
            rprint(f"  [red]✗[/red] {result.hook_name} — {result.error}")
            all_ok = False

    if not all_ok:
        raise typer.Exit(1)
