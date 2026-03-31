#!/usr/bin/env python3
"""Sync version from root VERSION file to all workspace package pyproject.toml files.

Usage:
    python scripts/sync-versions.py           # Update all packages
    python scripts/sync-versions.py --check   # Check versions are in sync (exit 1 if not)
"""

import re
import sys
import tomllib
from pathlib import Path

REPO_ROOT = Path(__file__).parent.parent
VERSION_FILE = REPO_ROOT / "VERSION"
PACKAGES_DIR = REPO_ROOT / "packages"


def read_version() -> str:
    return VERSION_FILE.read_text().strip()


def workspace_packages() -> list[str]:
    pyproject = REPO_ROOT / "pyproject.toml"
    payload = tomllib.loads(pyproject.read_text())
    members = payload["tool"]["uv"]["workspace"]["members"]
    return [Path(member).name for member in members]


def get_package_version(pyproject_path: Path) -> str:
    content = pyproject_path.read_text()
    m = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not m:
        raise ValueError(f"Could not find version in {pyproject_path}")
    return m.group(1)


def set_package_version(pyproject_path: Path, version: str) -> None:
    content = pyproject_path.read_text()
    new_content = re.sub(
        r'^(version\s*=\s*)"[^"]+"',
        f'\\1"{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    pyproject_path.write_text(new_content)


def main() -> None:
    check_only = "--check" in sys.argv

    canonical = read_version()
    print(f"Canonical version (VERSION file): {canonical}")

    out_of_sync = []
    for pkg in workspace_packages():
        pyproject = PACKAGES_DIR / pkg / "pyproject.toml"
        if not pyproject.exists():
            print(f"  ERROR: {pyproject} not found", file=sys.stderr)
            sys.exit(1)
        current = get_package_version(pyproject)
        if current != canonical:
            out_of_sync.append((pkg, current))
            print(f"  {pkg}: {current} -> {canonical}  [OUT OF SYNC]")
        else:
            print(f"  {pkg}: {current}  [ok]")

    if check_only:
        if out_of_sync:
            print(
                f"\nERROR: {len(out_of_sync)} package(s) out of sync with VERSION file.",
                file=sys.stderr,
            )
            print(
                "Run `python scripts/sync-versions.py` to fix.",
                file=sys.stderr,
            )
            sys.exit(1)
        print("\nAll packages in sync.")
        return

    if out_of_sync:
        for pkg, _ in out_of_sync:
            pyproject = PACKAGES_DIR / pkg / "pyproject.toml"
            set_package_version(pyproject, canonical)
            print(f"  Updated {pkg} -> {canonical}")
        print(f"\nSynced {len(out_of_sync)} package(s) to version {canonical}.")
    else:
        print("\nAll packages already in sync.")


if __name__ == "__main__":
    main()
