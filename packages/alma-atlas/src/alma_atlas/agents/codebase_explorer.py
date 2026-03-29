"""Codebase explorer — two-pass file selection for enrichment agents.

Performs an LLM-free *index pass* to build a file tree, then an LLM
*relevance pass* (using a cheap model) to rank files by their likely
usefulness for a given set of edges or assets.  Falls back to the
standard glob scan if the LLM call fails or returns no files.

This module is READ-ONLY with respect to the repository.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alma_atlas.agents.repo_scanner import (
    _MAX_FILE_CHARS,
    _is_skipped,
    build_file_index,
    collect_repo_files,
)
from alma_atlas.agents.schemas import ExplorerResult

if TYPE_CHECKING:
    from alma_atlas.agents.provider import LLMProvider

logger = logging.getLogger(__name__)

_EXPLORER_SYSTEM_PROMPT = """\
You are a code search expert. Given a repository file tree and a description
of data pipeline edges or data assets, identify which files are most likely to
contain useful information about how those entities are transformed or moved.

Return files ranked by relevance score:
- 1.0 = definitely contains relevant pipeline or transformation logic
- 0.5 = may contain relevant context (shared utilities, config)
- 0.0 = unrelated

Only include files with a relevance score > 0.1.  Return at most the number
of files requested.  For each file provide a short reason explaining the score.

Rules:
- Only include files that exist in the provided file tree.
- Do not invent file paths.
- Agents are READ-ONLY: never suggest modifying the repository.\
"""


def _format_file_tree(file_entries: list[tuple[str, int]]) -> str:
    """Format ``(rel_path, size)`` pairs into a compact tree-like listing."""
    lines: list[str] = []
    for rel, size in file_entries:
        kb = size / 1024
        lines.append(f"  {rel} ({kb:.1f}KB)")
    return "\n".join(lines) if lines else "  (empty)"


def _build_edge_descriptions(edges: list[Any]) -> list[str]:
    return [
        f"{e.upstream_id} → {e.downstream_id} (kind: {e.kind})"
        for e in edges
    ]


def _build_asset_descriptions(assets: list[dict[str, Any]]) -> list[str]:
    descs: list[str] = []
    for a in assets:
        asset_id = a.get("asset_id") or a.get("id", "?")
        name = a.get("name", "")
        kind = a.get("kind", "")
        desc = f"{asset_id}"
        if name and name != asset_id:
            desc += f" ({name})"
        if kind:
            desc += f" [{kind}]"
        descs.append(desc)
    return descs


def _build_explorer_prompt(descriptions: list[str], file_entries: list[tuple[str, int]], top_n: int) -> str:
    tree_text = _format_file_tree(file_entries)
    desc_text = "\n".join(f"- {d}" for d in descriptions)
    return (
        f"## Entities to find in the repository\n{desc_text}\n\n"
        f"## Repository files ({len(file_entries)} total)\n{tree_text}\n\n"
        f"Return at most {top_n} files ranked by relevance."
    )


async def _rank_files(
    descriptions: list[str],
    file_entries: list[tuple[str, int]],
    provider: LLMProvider,
    top_n: int,
) -> list[str]:
    """Ask the LLM to rank files; return a list of relative path strings."""
    user_prompt = _build_explorer_prompt(descriptions, file_entries, top_n)
    result: ExplorerResult = await provider.analyze(
        _EXPLORER_SYSTEM_PROMPT, user_prompt, ExplorerResult
    )
    valid_paths = {rel for rel, _ in file_entries}
    ranked = sorted(result.files, key=lambda f: f.relevance_score, reverse=True)
    return [f.path for f in ranked if f.path in valid_paths][:top_n]


def _load_ranked_files(repo_path: Path, rel_paths: list[str]) -> list[tuple[Path, str]]:
    """Read and return file content for each relative path."""
    results: list[tuple[Path, str]] = []
    for rel in rel_paths:
        abs_path = repo_path / rel
        if not abs_path.is_file() or _is_skipped(abs_path):
            continue
        try:
            content = abs_path.read_text(errors="replace")[:_MAX_FILE_CHARS]
        except OSError as exc:
            logger.debug("codebase_explorer: skipping %s: %s", abs_path, exc)
            continue
        results.append((abs_path, content))
    return results


async def explore_for_edges(
    edges: list[Any],
    repo_path: Path,
    provider: LLMProvider,
    *,
    top_n: int = 40,
) -> list[tuple[Path, str]]:
    """Select files relevant to a set of cross-system edges.

    Performs a two-pass file selection:
    1. **Index pass** (no LLM): build a file tree from *repo_path*.
    2. **Relevance pass** (LLM): ask the provider to rank files by relevance
       to the provided edges.

    Falls back to :func:`~alma_atlas.agents.repo_scanner.collect_repo_files`
    if the LLM call fails or returns no files.

    Args:
        edges:     List of edges (objects with ``upstream_id``, ``downstream_id``,
                   and ``kind`` attributes) to find pipeline code for.
        repo_path: Filesystem path to the code repository.
        provider:  Configured :class:`~alma_atlas.agents.provider.LLMProvider`
                   (should be a cheap/fast model).
        top_n:     Maximum number of files to return.

    Returns:
        ``(path, content)`` pairs suitable for the pipeline analysis agent.
    """
    if not edges:
        return []

    file_entries = build_file_index(repo_path)
    if not file_entries:
        logger.debug("codebase_explorer: no files found in %s, returning empty", repo_path)
        return []

    descriptions = _build_edge_descriptions(edges)
    try:
        ranked = await _rank_files(descriptions, file_entries, provider, top_n)
    except Exception as exc:
        logger.warning("codebase_explorer: LLM ranking failed, falling back to glob: %s", exc)
        return collect_repo_files(repo_path)

    if not ranked:
        logger.debug("codebase_explorer: LLM returned no ranked files, falling back to glob")
        return collect_repo_files(repo_path)

    logger.debug(
        "codebase_explorer: explorer selected %d/%d file(s) for %d edge(s)",
        len(ranked),
        len(file_entries),
        len(edges),
    )
    return _load_ranked_files(repo_path, ranked)


async def explore_for_assets(
    assets: list[dict[str, Any]],
    repo_path: Path,
    provider: LLMProvider,
    *,
    top_n: int = 40,
) -> list[tuple[Path, str]]:
    """Select files relevant to a set of data assets.

    Same two-pass approach as :func:`explore_for_edges` but keyed on asset
    identifiers and names rather than edge pairs.

    Args:
        assets:    List of asset context dicts (each must include ``asset_id``).
        repo_path: Filesystem path to the code repository.
        provider:  Configured LLM provider (should be a cheap/fast model).
        top_n:     Maximum number of files to return.

    Returns:
        ``(path, content)`` pairs suitable for the asset enrichment agent.
    """
    if not assets:
        return []

    file_entries = build_file_index(repo_path)
    if not file_entries:
        logger.debug("codebase_explorer: no files found in %s, returning empty", repo_path)
        return []

    descriptions = _build_asset_descriptions(assets)
    try:
        ranked = await _rank_files(descriptions, file_entries, provider, top_n)
    except Exception as exc:
        logger.warning("codebase_explorer: LLM ranking failed, falling back to glob: %s", exc)
        return collect_repo_files(repo_path)

    if not ranked:
        logger.debug("codebase_explorer: LLM returned no ranked files, falling back to glob")
        return collect_repo_files(repo_path)

    logger.debug(
        "codebase_explorer: explorer selected %d/%d file(s) for %d asset(s)",
        len(ranked),
        len(file_entries),
        len(assets),
    )
    return _load_ranked_files(repo_path, ranked)
