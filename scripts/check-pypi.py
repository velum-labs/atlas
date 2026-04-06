#!/usr/bin/env python3
"""Check whether all public Atlas packages are available on PyPI at the current version.

Queries the PyPI JSON API for each package. Exits 1 if any package is missing
or if the current repo version is not yet published. No packages are installed.

Usage:
    python3 scripts/check-pypi.py                    # check current VERSION
    python3 scripts/check-pypi.py --version 0.2.0   # check a specific version
    python3 scripts/check-pypi.py --latest           # check what's on PyPI (no version pin)
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

# All packages intended for public PyPI distribution, in publish order.
PUBLIC_PACKAGES = [
    "alma-ports",
    "alma-sqlkit",
    "alma-algebrakit",
    "alma-atlas-store",
    "alma-connectors",
    "alma-analysis",
    "alma-atlas",
]

PYPI_JSON_URL = "https://pypi.org/pypi/{package}/json"
PYPI_VERSION_URL = "https://pypi.org/pypi/{package}/{version}/json"


def fetch_json(url: str) -> dict | None:
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    except urllib.error.URLError as e:
        # Network-level failure (timeout, DNS, connection refused).
        # Raise SystemExit so CI sees a clean message instead of a traceback.
        raise SystemExit(f"ERROR: cannot reach PyPI ({e.reason}) — check network connectivity") from e


def check_package(package: str, version: str | None, latest_only: bool) -> tuple[bool, str]:
    """Return (ok, message) for a package/version check."""
    if latest_only or version is None:
        data = fetch_json(PYPI_JSON_URL.format(package=package))
        if data is None:
            return False, "not found on PyPI"
        latest = data["info"]["version"]
        return True, f"found (latest: {latest})"

    data = fetch_json(PYPI_VERSION_URL.format(package=package, version=version))
    if data is None:
        # Fall back to the package index to see what versions exist
        pkg_data = fetch_json(PYPI_JSON_URL.format(package=package))
        if pkg_data is None:
            return False, "package not found on PyPI"
        available = sorted(pkg_data["releases"].keys())
        latest = pkg_data["info"]["version"]
        return False, f"version {version} not found (latest: {latest}, available: {available[-5:]})"

    return True, "found"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", help="Version to check (default: read from VERSION file)")
    parser.add_argument("--latest", action="store_true", help="Only check package exists; ignore version")
    args = parser.parse_args()

    version: str | None
    if args.latest:
        version = None
        print("Checking PyPI availability (latest, no version pin):")
    else:
        version = args.version or (REPO_ROOT / "VERSION").read_text().strip()
        print(f"Checking PyPI availability for version {version}:")

    failures: list[str] = []
    for package in PUBLIC_PACKAGES:
        ok, msg = check_package(package, version, args.latest)
        status = "ok" if ok else "MISSING"
        print(f"  {package}: {msg}  [{status}]")
        if not ok:
            failures.append(package)

    print()
    if failures:
        print(f"ERROR: {len(failures)} package(s) not available on PyPI:", file=sys.stderr)
        for pkg in failures:
            print(f"  - {pkg}", file=sys.stderr)
        sys.exit(1)

    print("All packages available on PyPI.")


if __name__ == "__main__":
    main()
