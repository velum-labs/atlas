"""Shared repository file scanning utilities.

Used by both :mod:`alma_atlas.agents.pipeline_analyzer` (fallback path) and
:mod:`alma_atlas.agents.codebase_explorer` (index pass).

This module is READ-ONLY with respect to the repository: it never modifies
any file in the scanned path.
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Maximum characters of file content included per file (avoids token overruns).
_MAX_FILE_CHARS = 4_000
# Maximum number of files to include in a single prompt.
_MAX_FILES = 40
# Glob patterns used to discover relevant pipeline code.
_SCAN_GLOBS: tuple[str, ...] = (
    "dags/**/*.py",
    "pipelines/**/*.py",
    "models/**/*.sql",
    "**/*.py",
    "**/*.sql",
)
# Directory names that are always skipped during scanning.
_SKIP_DIRS: frozenset[str] = frozenset(
    {".git", "__pycache__", ".venv", "node_modules", ".tox", "dist", "build", ".mypy_cache"}
)


def _is_skipped(p: Path) -> bool:
    return any(part in _SKIP_DIRS for part in p.parts)


def collect_repo_files(repo_path: Path) -> list[tuple[Path, str]]:
    """Return (path, content) pairs for relevant files found in *repo_path*.

    Files are collected in priority order defined by :data:`_SCAN_GLOBS` and
    capped at :data:`_MAX_FILES` total.  Each file's content is capped at
    :data:`_MAX_FILE_CHARS` characters.

    Args:
        repo_path: Filesystem path to the code repository to scan.

    Returns:
        List of ``(path, content)`` tuples for discovered files.
    """
    results: list[tuple[Path, str]] = []
    seen: set[Path] = set()

    for pattern in _SCAN_GLOBS:
        for file_path in sorted(repo_path.glob(pattern)):
            if file_path in seen or not file_path.is_file() or _is_skipped(file_path):
                continue
            seen.add(file_path)
            try:
                content = file_path.read_text(errors="replace")[:_MAX_FILE_CHARS]
            except OSError as exc:
                logger.debug("repo_scanner: skipping %s: %s", file_path, exc)
                continue
            results.append((file_path, content))
            if len(results) >= _MAX_FILES:
                return results

    return results


def build_file_index(repo_path: Path) -> list[tuple[str, int]]:
    """Return ``(rel_path, size_bytes)`` pairs for all scannable files in *repo_path*.

    Used by the codebase explorer for the LLM-free index pass.

    Args:
        repo_path: Filesystem path to the code repository to scan.

    Returns:
        List of ``(relative_path_string, file_size_in_bytes)`` tuples.
    """
    results: list[tuple[str, int]] = []
    seen: set[Path] = set()

    for pattern in _SCAN_GLOBS:
        for file_path in sorted(repo_path.glob(pattern)):
            if file_path in seen or not file_path.is_file() or _is_skipped(file_path):
                continue
            seen.add(file_path)
            try:
                size = file_path.stat().st_size
            except OSError:
                size = 0
            try:
                rel = str(file_path.relative_to(repo_path))
            except ValueError:
                rel = str(file_path)
            results.append((rel, size))

    return results
