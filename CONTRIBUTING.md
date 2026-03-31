# Contributing to Alma Atlas

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) — fast Python package manager

## Setup

```bash
# Clone the repo
git clone https://github.com/your-org/atlas.git
cd atlas

# Install all workspace packages in editable mode
uv sync --all-packages

# Verify the CLI works
uv run alma-atlas --help
```

## Project structure

```text
atlas/
├── packages/
│   ├── alma-atlas/          # CLI + MCP server + pipeline
│   ├── alma-atlas-store/    # SQLite store
│   ├── alma-ports/          # Protocol interfaces
│   ├── alma-connectors/     # Source adapters
│   ├── alma-analysis/       # Analysis functions
│   ├── alma-sqlkit/         # SQL utilities
│   └── alma-algebrakit/     # SQL algebra + fingerprinting
├── pyproject.toml           # Workspace root + ruff/pytest config
└── .python-version          # Pinned Python version
```

## Development workflow

```bash
# Run linter
uv run ruff check .

# Run formatter
uv run ruff format .

# Type check
uv run ty check $(python3 scripts/typecheck_targets.py --shell)

# Run tests
uv run pytest

# Run a specific package's tests
uv run pytest packages/alma-atlas/
```

## Documentation

```bash
# Install the docs toolchain alongside the workspace packages
uv sync --all-packages --group docs

# Preview the docs site locally
uv run mkdocs serve

# Build docs with warnings treated as errors
uv run mkdocs build --strict
```

## Code style

- **Formatter**: ruff format (line length 120)
- **Linter**: ruff with rules E, F, I, UP, B, SIM, TCH
- **Type checker**: ty (Astral)
- **Python**: 3.12+ only — use modern syntax freely

## Adding a new connector

1. Add your adapter class in `packages/alma-connectors/src/alma_connectors/<name>.py`
2. Implement the `SourceAdapterV2` protocol in `alma_connectors.source_adapter_v2`
3. Add any required dependencies directly to `packages/alma-connectors/pyproject.toml`
4. Export from `alma_connectors/__init__.py`

## Submitting changes

1. Fork and create a feature branch
2. Ensure `uv run ruff check .`, `uv run ruff format --check .`, and `uv run pytest` all pass
3. Open a pull request with a clear description

## Release

Workspace releases are lockstep and derive from the root `VERSION` file.
Run `python scripts/sync-versions.py --check` to verify version drift before releasing.
