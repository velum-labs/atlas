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
uv sync

# Verify the CLI works
uv run alma-atlas --help
```

## Project structure

```
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
uv run ty check packages/

# Run tests
uv run pytest

# Run a specific package's tests
uv run pytest packages/alma-atlas/
```

## Code style

- **Formatter**: ruff format (line length 120)
- **Linter**: ruff with rules E, F, I, UP, B, SIM, TCH
- **Type checker**: ty (Astral)
- **Python**: 3.12+ only — use modern syntax freely

## Adding a new connector

1. Add your adapter class in `packages/alma-connectors/src/alma_connectors/<name>.py`
2. Implement the `SourceAdapter` protocol from `alma_ports`
3. Add optional dependencies to `alma-connectors/pyproject.toml` under `[project.optional-dependencies]`
4. Export from `alma_connectors/__init__.py`

## Submitting changes

1. Fork and create a feature branch
2. Ensure `uv run ruff check .`, `uv run ruff format --check .`, and `uv run pytest` all pass
3. Open a pull request with a clear description

## Release

Packages are published independently to PyPI. Version bumps follow semantic versioning.
