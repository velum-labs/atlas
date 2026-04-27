"""CLI: register Atlas as an MCP server in Cursor or Claude Desktop.

Writes the MCP server config so the user's agent client can discover Atlas
without manual JSON editing. Per eng review:

- Issue 6A: merges with existing MCP config; on parse failure, the existing
  file is renamed to `.bak` and a fresh config is written. Preserves the
  user's other MCP servers (dbt-mcp, snowflake-mcp, etc.) by default.
- Codex finding 2: Cursor extension entirely dropped from v1; this CLI is
  the only install path.
- Codex finding 3: `--scope project|global` flag for Cursor. Track 3a
  (concierge) defaults `--scope project`; Track 3b (public) defaults
  `--scope global`.
- Eng review failure mode: detect when Cursor / Claude Desktop isn't
  installed and print a helpful error rather than silently writing config to
  a directory the user doesn't actually use.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
from pathlib import Path
from typing import Annotated, Any

import typer
from rich import print as rprint

app = typer.Typer(help="Install Atlas as an MCP server in your AI agent client.")


_CURSOR_PROJECT_PATH = Path(".cursor/mcp.json")
_CURSOR_GLOBAL_PATH = Path.home() / ".cursor" / "mcp.json"
_MIN_TOKEN_LENGTH = 16


def _claude_desktop_config_path() -> Path:
    """Resolve the Claude Desktop MCP config path for the current OS."""
    system = platform.system()
    home = Path.home()
    if system == "Darwin":
        return home / "Library" / "Application Support" / "Claude" / "claude_desktop_config.json"
    if system == "Linux":
        return home / ".config" / "Claude" / "claude_desktop_config.json"
    if system == "Windows":
        appdata = Path(os.environ.get("APPDATA", str(home / "AppData" / "Roaming")))
        return appdata / "Claude" / "claude_desktop_config.json"
    raise typer.Exit(f"Unsupported OS for Claude Desktop install: {system}")


def _cursor_appears_installed() -> bool:
    """Heuristic: does Cursor appear installed on this machine?"""
    if _CURSOR_GLOBAL_PATH.parent.exists():
        return True
    if _CURSOR_PROJECT_PATH.parent.exists():
        return True
    return shutil.which("cursor") is not None


def _claude_desktop_appears_installed() -> bool:
    """Heuristic: does Claude Desktop appear installed?"""
    return _claude_desktop_config_path().parent.exists()


@app.command("cursor")
def install_cursor(
    token: Annotated[
        str | None,
        typer.Option("--token", help="Invite token for Atlas Companion mode (concierge audience)"),
    ] = None,
    scope: Annotated[
        str,
        typer.Option(
            "--scope",
            help="project (write to .cursor/mcp.json in cwd) or global (write to ~/.cursor/mcp.json)",
        ),
    ] = "global",
) -> None:
    """Register Atlas as an MCP server in Cursor."""
    if not _cursor_appears_installed():
        rprint(
            "[red]Cursor doesn't appear to be installed.[/red]\n"
            "Install Cursor from https://cursor.com first, then re-run this command."
        )
        raise typer.Exit(1)
    if scope not in ("project", "global"):
        rprint(f"[red]Invalid --scope:[/red] {scope!r}. Use 'project' or 'global'.")
        raise typer.Exit(1)
    _validate_token_format(token)

    target = _CURSOR_PROJECT_PATH if scope == "project" else _CURSOR_GLOBAL_PATH
    server_entry = _build_server_entry(token)

    _merge_mcp_server_into_config(
        config_path=target,
        server_name="atlas",
        server_entry=server_entry,
    )
    _emit_install_event(client="cursor", token=token, scope=scope)
    rprint(f"[green]Atlas registered with Cursor:[/green] {target}")


@app.command("claude")
def install_claude(
    token: Annotated[
        str | None,
        typer.Option("--token", help="Invite token for Atlas Companion mode (concierge audience)"),
    ] = None,
) -> None:
    """Register Atlas as an MCP server in Claude Desktop."""
    if not _claude_desktop_appears_installed():
        rprint(
            "[red]Claude Desktop doesn't appear to be installed.[/red]\n"
            "Install it from https://claude.ai/download first, then re-run this command."
        )
        raise typer.Exit(1)
    _validate_token_format(token)

    target = _claude_desktop_config_path()
    server_entry = _build_server_entry(token)

    _merge_mcp_server_into_config(
        config_path=target,
        server_name="atlas",
        server_entry=server_entry,
    )
    _emit_install_event(client="claude", token=token, scope="global")
    rprint(f"[green]Atlas registered with Claude Desktop:[/green] {target}")


def _emit_install_event(*, client: str, token: str | None, scope: str) -> None:
    """Fire telemetry events for an install action.

    Always fires a mandatory anonymous event with the install_source. If a
    token was supplied (concierge install), also fires an opt-in event with
    a hashed account correlator so the install can be attributed downstream
    in Velum's funnel analysis. Both event paths swallow any PostHog error.
    """
    from alma_atlas.telemetry import (
        mandatory_event,
        opt_in_event,
        telemetry_config_from_env,
    )

    cfg = telemetry_config_from_env()
    install_source = "concierge_invite" if token is not None else "direct_pip"

    mandatory_event(
        cfg,
        f"install_{client}",
        {"install_source": install_source},
    )

    if token is not None:
        import hashlib

        cfg.opt_in = True
        cfg.alma_account_token = hashlib.sha256(token.encode("utf-8")).hexdigest()[:16]
        opt_in_event(
            cfg,
            f"install_{client}",
            {"client": client, "scope": scope, "install_source": install_source},
        )


def _validate_token_format(token: str | None) -> None:
    """Reject malformed tokens at the CLI boundary before writing config."""
    if token is None:
        return
    if len(token) < _MIN_TOKEN_LENGTH:
        rprint(
            f"[red]Invalid --token:[/red] tokens must be at least {_MIN_TOKEN_LENGTH} characters."
        )
        raise typer.Exit(1)


def _build_server_entry(token: str | None) -> dict[str, Any]:
    """Build the MCP server entry that goes into the agent client's config."""
    args = ["serve"]
    if token is not None:
        args.extend(["--alma-token", token])
    return {"command": "alma-atlas", "args": args}


def _merge_mcp_server_into_config(
    *, config_path: Path, server_name: str, server_entry: dict[str, Any]
) -> None:
    """Merge an MCP server entry into the existing config JSON.

    On JSON parse failure or wrong root shape, the existing file is renamed
    to `<name>.bak` and a fresh config is written. The user's other MCP
    servers (dbt-mcp, snowflake-mcp, etc.) are preserved when the existing
    config parses cleanly.
    """
    config_path.parent.mkdir(parents=True, exist_ok=True)
    existing: dict[str, Any] = {}
    if config_path.exists():
        try:
            parsed = json.loads(config_path.read_text(encoding="utf-8"))
            if not isinstance(parsed, dict):
                raise ValueError("config root must be a JSON object")
            existing = parsed
        except (json.JSONDecodeError, ValueError) as exc:
            backup = _backup_path(config_path)
            config_path.rename(backup)
            rprint(
                f"[yellow]Existing {config_path.name} was malformed ({exc}); "
                f"backed up to {backup.name}.[/yellow]"
            )
            existing = {}

    servers = existing.get("mcpServers")
    if not isinstance(servers, dict):
        if servers is not None:
            backup = _backup_path(config_path)
            if config_path.exists():
                config_path.rename(backup)
                rprint(
                    f"[yellow]Existing mcpServers had wrong shape; "
                    f"backed up to {backup.name}.[/yellow]"
                )
        servers = {}
        existing["mcpServers"] = servers

    servers[server_name] = server_entry
    config_path.write_text(json.dumps(existing, indent=2) + "\n", encoding="utf-8")


def _backup_path(config_path: Path) -> Path:
    """Pick a non-clobbering .bak path next to the given config file."""
    base = config_path.with_suffix(config_path.suffix + ".bak")
    if not base.exists():
        return base
    # If .bak is taken (rerun), suffix .bak.1, .bak.2, etc.
    n = 1
    while True:
        candidate = config_path.with_suffix(config_path.suffix + f".bak.{n}")
        if not candidate.exists():
            return candidate
        n += 1
