#!/usr/bin/env python3
"""Read ordered workspace packages from the root pyproject.toml."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def workspace_packages() -> list[str]:
    content = (REPO_ROOT / "pyproject.toml").read_text()
    section_match = re.search(
        r"^\[tool\.uv\.workspace\]\s*(.*?)(?=^\[|\Z)",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if section_match is None:
        raise ValueError("Could not find [tool.uv.workspace] in pyproject.toml")

    members_match = re.search(r"members\s*=\s*\[(.*?)\]", section_match.group(1), re.DOTALL)
    if members_match is None:
        raise ValueError("Could not find workspace members in pyproject.toml")

    members = re.findall(r'"([^"]+)"', members_match.group(1))
    return [Path(member).name for member in members]


def workspace_package_paths() -> list[str]:
    return [f"packages/{name}" for name in workspace_packages()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="Print package names as a JSON array.")
    parser.add_argument(
        "--editable-args",
        action="store_true",
        help="Print shell-safe editable install arguments for pip.",
    )
    args = parser.parse_args()

    if args.json:
        print(json.dumps(workspace_packages()))
        return

    if args.editable_args:
        print(" ".join(f"-e {path}" for path in workspace_package_paths()))
        return

    for package in workspace_packages():
        print(package)


if __name__ == "__main__":
    main()
