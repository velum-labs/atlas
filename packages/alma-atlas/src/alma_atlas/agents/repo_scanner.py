"""Shared repository file scanning utilities.

Used by both :mod:`alma_atlas.agents.pipeline_analyzer` (fallback path) and
:mod:`alma_atlas.agents.codebase_explorer` (index pass).

This module is READ-ONLY with respect to the repository: it never modifies
any file in the scanned path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RepoScanConfig:
    """Tunable limits for repository file discovery."""

    max_file_chars: int = 4_000
    max_files: int = 40
    scan_globs: tuple[str, ...] = (
        "dags/**/*.py",
        "pipelines/**/*.py",
        "models/**/*.sql",
        "**/*.py",
        "**/*.sql",
    )
    skip_dirs: frozenset[str] = frozenset(
        {".git", "__pycache__", ".venv", "node_modules", ".tox", "dist", "build", ".mypy_cache"}
    )


DEFAULT_REPO_SCAN_CONFIG = RepoScanConfig()

# Backward-compatible constants for callers that still import them directly.
_MAX_FILE_CHARS = DEFAULT_REPO_SCAN_CONFIG.max_file_chars
_MAX_FILES = DEFAULT_REPO_SCAN_CONFIG.max_files


def _is_skipped(path: Path, config: RepoScanConfig) -> bool:
    return any(part in config.skip_dirs for part in path.parts)


def collect_repo_files(
    repo_path: Path,
    *,
    config: RepoScanConfig = DEFAULT_REPO_SCAN_CONFIG,
) -> list[tuple[Path, str]]:
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

    for pattern in config.scan_globs:
        for file_path in sorted(repo_path.glob(pattern)):
            if file_path in seen or not file_path.is_file() or _is_skipped(file_path, config):
                continue
            seen.add(file_path)
            try:
                content = file_path.read_text(errors="replace")[: config.max_file_chars]
            except OSError as exc:
                logger.debug("repo_scanner: skipping %s: %s", file_path, exc)
                continue
            results.append((file_path, content))
            if len(results) >= config.max_files:
                return results

    return results


def build_file_index(
    repo_path: Path,
    *,
    config: RepoScanConfig = DEFAULT_REPO_SCAN_CONFIG,
) -> list[tuple[str, int]]:
    """Return ``(rel_path, size_bytes)`` pairs for all scannable files in *repo_path*.

    Used by the codebase explorer for the LLM-free index pass.

    Args:
        repo_path: Filesystem path to the code repository to scan.

    Returns:
        List of ``(relative_path_string, file_size_in_bytes)`` tuples.
    """
    results: list[tuple[str, int]] = []
    seen: set[Path] = set()

    for pattern in config.scan_globs:
        for file_path in sorted(repo_path.glob(pattern)):
            if file_path in seen or not file_path.is_file() or _is_skipped(file_path, config):
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
