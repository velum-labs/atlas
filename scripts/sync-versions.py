#!/usr/bin/env python3
"""Sync version from root VERSION file to all workspace package pyproject.toml files.

Usage:
    python scripts/sync-versions.py           # Update all packages
    python scripts/sync-versions.py --check   # Check versions are in sync (exit 1 if not)
"""

import re
import sys
from pathlib import Path

from workspace_packages import workspace_packages

REPO_ROOT = Path(__file__).parent.parent
VERSION_FILE = REPO_ROOT / "VERSION"
PACKAGES_DIR = REPO_ROOT / "packages"
ROOT_PYPROJECT = REPO_ROOT / "pyproject.toml"


def read_version() -> str:
    return VERSION_FILE.read_text().strip()


def get_toml_version(pyproject_path: Path) -> str:
    content = pyproject_path.read_text()
    m = re.search(r'^version\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not m:
        raise ValueError(f"Could not find version in {pyproject_path}")
    return m.group(1)


def set_toml_version(pyproject_path: Path, version: str) -> None:
    content = pyproject_path.read_text()
    new_content = re.sub(
        r'^(version\s*=\s*)"[^"]+"',
        f'\\1"{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    pyproject_path.write_text(new_content)


def get_python_version(module_path: Path) -> str:
    content = module_path.read_text()
    m = re.search(r'^__version__\s*=\s*"([^"]+)"', content, re.MULTILINE)
    if not m:
        raise ValueError(f"Could not find __version__ in {module_path}")
    return m.group(1)


def set_python_version(module_path: Path, version: str) -> None:
    content = module_path.read_text()
    new_content = re.sub(
        r'^(__version__\s*=\s*)"[^"]+"',
        f'\\1"{version}"',
        content,
        count=1,
        flags=re.MULTILINE,
    )
    module_path.write_text(new_content)


def iter_version_targets() -> list[tuple[str, Path, str]]:
    targets: list[tuple[str, Path, str]] = [("workspace-root", ROOT_PYPROJECT, "toml")]
    for pkg in workspace_packages():
        pyproject = PACKAGES_DIR / pkg / "pyproject.toml"
        targets.append((pkg, pyproject, "toml"))
        module_init = PACKAGES_DIR / pkg / "src" / pkg.replace("-", "_") / "__init__.py"
        if module_init.exists() and "__version__" in module_init.read_text():
            targets.append((f"{pkg}.__version__", module_init, "python"))
    return targets


def main() -> None:
    check_only = "--check" in sys.argv

    canonical = read_version()
    print(f"Canonical version (VERSION file): {canonical}")

    out_of_sync = []
    for label, path, target_type in iter_version_targets():
        if not path.exists():
            print(f"  ERROR: {path} not found", file=sys.stderr)
            sys.exit(1)
        current = get_toml_version(path) if target_type == "toml" else get_python_version(path)
        if current != canonical:
            out_of_sync.append((label, path, target_type, current))
            print(f"  {label}: {current} -> {canonical}  [OUT OF SYNC]")
        else:
            print(f"  {label}: {current}  [ok]")

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
        for label, path, target_type, _current in out_of_sync:
            if target_type == "toml":
                set_toml_version(path, canonical)
            else:
                set_python_version(path, canonical)
            print(f"  Updated {label} -> {canonical}")
        print(f"\nSynced {len(out_of_sync)} version target(s) to version {canonical}.")
    else:
        print("\nAll packages already in sync.")


if __name__ == "__main__":
    main()
