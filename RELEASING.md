# Releasing Alma Atlas

## Current PyPI status

To check which packages are currently available on PyPI (without installing anything):

```bash
python3 scripts/check-pypi.py          # check current VERSION
python3 scripts/check-pypi.py --latest  # check any version exists
```

> **Note:** As of the initial ENG-422 infrastructure work, no packages have been published
> to PyPI yet. Tags `v0.1.0`–`v0.1.3` were development milestone tags that predated the
> publish workflow; they did not produce PyPI releases. The first real release should use
> `v0.1.4` or later (to avoid re-using those tag names). Configure the Trusted Publisher
> on PyPI first (see below), then run `./scripts/release.sh 0.1.4`.

## Public packages

All seven workspace packages are published to PyPI under the `almaos` Trusted Publisher:

| Package | PyPI URL | Purpose |
|---------|----------|---------|
| `alma-atlas` | https://pypi.org/project/alma-atlas/ | CLI, MCP server, scan orchestration |
| `alma-atlas-store` | https://pypi.org/project/alma-atlas-store/ | SQLite persistence |
| `alma-connectors` | https://pypi.org/project/alma-connectors/ | Source adapters (BigQuery, Snowflake, …) |
| `alma-analysis` | https://pypi.org/project/alma-analysis/ | Graph and lineage analysis |
| `alma-sqlkit` | https://pypi.org/project/alma-sqlkit/ | SQL parsing and normalization |
| `alma-algebrakit` | https://pypi.org/project/alma-algebrakit/ | Pure relational algebra engine |
| `alma-ports` | https://pypi.org/project/alma-ports/ | Shared protocols (zero dependencies) |

Most users only need `alma-atlas`. The other packages are published so they can be
used independently (e.g. `alma-sqlkit` in a custom query pipeline) and so that
pinned transitive dependencies resolve cleanly through PyPI rather than requiring
a monorepo clone.

## Version invariant

Every package always ships at the same version. The `VERSION` file at the repo
root is the single source of truth. `scripts/sync-versions.py --check` enforces
that all `pyproject.toml` files and all `__init__.__version__` attributes match
it. This check runs on every CI push and again at publish time.

## Release runbook

### Prerequisites

- You are on a clean `main` branch with no uncommitted changes.
- All CI checks pass on the commit you intend to release.
- You have push access to the repository.

### Steps

```bash
# 1. Decide the new version (semver: X.Y.Z)
VERSION=0.2.0

# 2. Run the release script — it bumps, commits, tags, and pushes
./scripts/release.sh "$VERSION"
```

That's it. The script:

1. Validates the version looks like semver.
2. Writes `VERSION`.
3. Runs `sync-versions.py` to update all `pyproject.toml` and `__init__.py` targets.
4. Commits `chore: bump version to X.Y.Z`.
5. Creates an annotated tag `vX.Y.Z`.
6. Pushes the commit and tag to `origin`.

Pushing the tag triggers `.github/workflows/publish.yml`, which:

1. Verifies the tag version matches `VERSION` and all packages are in sync.
2. Builds and publishes each package to PyPI via OIDC Trusted Publisher (no stored secrets).
3. Also publishes to `ghcr.io/velum-labs/` via PyOCI.
4. After all publishes succeed, installs `alma-atlas` from PyPI in a clean virtualenv and runs a CLI + import smoke test.
5. Queries the PyPI JSON API to confirm all 7 packages are visible at the released version.
6. Creates a GitHub Release with a table of all published package PyPI URLs.

### Monitoring a release

Watch the Actions run at:
```
https://github.com/almaos/atlas/actions/workflows/publish.yml
```

Jobs in order:
- `workspace-packages` — reads publish order from `pyproject.toml`
- `publish` — builds and uploads each package (matrix, sequential, fail-fast)
- `pypi-smoke` — installs from PyPI in a clean env, verifies CLI + imports + all packages visible
- `github-release` — creates the GitHub Release with package URLs

To verify PyPI availability manually at any time (no install required):

```bash
python3 scripts/check-pypi.py           # check current VERSION
python3 scripts/check-pypi.py --latest  # check any version exists
```

If `publish` fails mid-matrix, PyPI will have a partial release. The workflow
uses `skip-existing: true`, so re-triggering it with the same tag is safe —
already-uploaded packages are skipped and the remaining ones are retried.

### Diagnosing a publish failure

**Tag/version mismatch** — the error message prints the exact fix command.

**Wheel metadata invalid** — `twine check` prints the specific METADATA field
that failed. Usually means a missing `readme` file or invalid classifier.

**PyPI upload rejected** — most common causes:
  - Package name already claimed by another project (contact PyPI support)
  - Trusted Publisher not configured (go to PyPI project settings → Publishing)
  - Network error during upload (re-trigger the job)

**PyPI smoke fails** — the package uploaded but is not importable. Likely a
missing runtime dependency in `pyproject.toml` that was shadowed by the
workspace install during testing. Fix in `packages/<name>/pyproject.toml`,
bump patch version, and re-release.

## Trusted Publisher setup

The publish workflow uses [PyPI OIDC Trusted Publishing](https://docs.pypi.org/trusted-publishers/)
— no API token is stored in GitHub secrets.

For each PyPI project, the Trusted Publisher must be configured at:
```
https://pypi.org/manage/project/<package-name>/settings/publishing/
```

Settings:
- **Owner**: `almaos`
- **Repository**: `atlas`
- **Workflow**: `publish.yml`
- **Environment**: `release`
