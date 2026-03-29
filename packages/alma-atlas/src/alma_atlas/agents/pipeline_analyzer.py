"""Pipeline analyzer — scans a code repository and enriches cross-system edges.

Collects relevant pipeline files (dbt models, Airflow DAGs, Python scripts,
SQL files) and asks the configured LLM provider to infer transport metadata
for a batch of unenriched edges.

This module is READ-ONLY with respect to the repository: it never modifies
any file in the scanned path.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from alma_atlas.agents.repo_scanner import (
    _MAX_FILE_CHARS,  # re-exported for callers that reference it directly  # noqa: F401
    _MAX_FILES,  # noqa: F401
    _SCAN_GLOBS,  # noqa: F401
    _SKIP_DIRS,  # noqa: F401
    collect_repo_files as _collect_repo_files,  # backward-compat alias
)
from alma_atlas.agents.schemas import EdgeEnrichment, PipelineAnalysisResult

if TYPE_CHECKING:
    from alma_atlas.agents.provider import LLMProvider
    from alma_atlas_store.edge_repository import Edge

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an expert data engineer analyzing a code repository to understand how data \
moves between systems.

Your task is to examine the provided code files and infer the transport metadata for \
each cross-system data edge listed below.

For each edge you can identify, report:
- source_table: the upstream asset name (schema.table format)
- dest_table: the downstream asset name (schema.table format)
- transport_kind: how data physically moves — one of CUSTOM_SCRIPT, AIRBYTE, \
FIVETRAN, CDC, CLOUD_TRANSFER, DBT_SEED, UNKNOWN
- schedule: cron expression or plain-English description if found, else null
- strategy: copy strategy — one of FULL, INCREMENTAL, CDC, APPEND_ONLY, UNKNOWN
- write_disposition: what happens at the destination — one of TRUNCATE, APPEND, \
MERGE, UNKNOWN
- watermark_column: column used for incremental loads if found, else null
- owner: DAG owner, script author, or team name if identifiable, else null
- confidence_note: one or two sentences explaining your reasoning

Rules:
- Use UNKNOWN for any value you cannot determine from the provided code.
- Only report edges for which you find concrete evidence in the files.
- Do not fabricate values or guess beyond what the code shows.
- Agents are READ-ONLY: never suggest modifying the repository.\
"""


def _build_user_prompt(
    edges: list[Edge],
    repo_files: list[tuple[Path, str]],
    repo_path: Path,
) -> str:
    """Compose the user-facing portion of the LLM prompt."""
    parts: list[str] = ["## Edge pairs to analyze\n"]
    for i, edge in enumerate(edges, 1):
        parts.append(
            f"{i}. upstream: {edge.upstream_id}"
            f"  →  downstream: {edge.downstream_id}"
            f"  (edge kind: {edge.kind})"
        )

    parts.append("\n## Repository files\n")
    for file_path, content in repo_files:
        try:
            rel = file_path.relative_to(repo_path)
        except ValueError:
            rel = file_path
        parts.append(f"--- {rel} ---")
        parts.append(content)
        parts.append("")

    if not repo_files:
        parts.append("(no relevant files found in repository)")

    return "\n".join(parts)


async def analyze_edges(
    edges: list[Edge],
    repo_path: Path,
    provider: LLMProvider,
    *,
    pre_filtered_files: list[tuple[Path, str]] | None = None,
) -> list[EdgeEnrichment]:
    """Analyze a batch of edges against a repository and return enrichment data.

    The function is READ-ONLY: it never writes to *repo_path* or the store.
    If the LLM call fails the exception is logged as a warning and an empty
    list is returned so the caller can decide how to proceed.

    Args:
        edges:               Unenriched :class:`~alma_atlas_store.edge_repository.Edge`
                             objects to analyse.
        repo_path:           Filesystem path to the code repository to scan.
        provider:            Configured :class:`~alma_atlas.agents.provider.LLMProvider`.
        pre_filtered_files:  Optional pre-selected ``(path, content)`` pairs from the
                             codebase explorer.  When provided, file scanning is skipped.
                             When ``None``, the standard glob scan is used.

    Returns:
        Zero or more :class:`EdgeEnrichment` instances as returned by the agent.
    """
    if not edges:
        return []

    if pre_filtered_files is not None:
        repo_files = pre_filtered_files
    else:
        repo_files = _collect_repo_files(repo_path)

    logger.debug(
        "pipeline_analyzer: %d edge(s), %d repo file(s) from %s",
        len(edges),
        len(repo_files),
        repo_path,
    )

    user_prompt = _build_user_prompt(edges, repo_files, repo_path)
    try:
        result: PipelineAnalysisResult = await provider.analyze(
            _SYSTEM_PROMPT, user_prompt, PipelineAnalysisResult
        )
    except Exception as exc:
        logger.warning("pipeline_analyzer: LLM call failed: %s", exc)
        return []

    return result.edges
