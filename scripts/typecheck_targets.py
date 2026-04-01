#!/usr/bin/env python3
"""Print the configured source targets that must pass `ty check`."""

from __future__ import annotations

import argparse
import re
import shlex
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def typecheck_targets() -> list[str]:
    content = (REPO_ROOT / "pyproject.toml").read_text()
    section_match = re.search(
        r"^\[tool\.alma\]\s*(.*?)(?=^\[|\Z)",
        content,
        re.MULTILINE | re.DOTALL,
    )
    if section_match is None:
        raise ValueError("Could not find [tool.alma] in pyproject.toml")

    targets_match = re.search(r"typecheck-targets\s*=\s*\[(.*?)\]", section_match.group(1), re.DOTALL)
    if targets_match is None:
        raise ValueError("Could not find tool.alma.typecheck-targets in pyproject.toml")

    return re.findall(r'"([^"]+)"', targets_match.group(1))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell", action="store_true", help="Print shell-quoted targets on one line.")
    args = parser.parse_args()

    if args.shell:
        print(" ".join(shlex.quote(target) for target in typecheck_targets()))
        return

    for target in typecheck_targets():
        print(target)


if __name__ == "__main__":
    main()
