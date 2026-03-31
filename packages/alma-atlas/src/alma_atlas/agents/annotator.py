"""Annotator workflow for per-asset metadata enrichment.

This is the P2 "Codex learning" layer: given a set of asset IDs and their
current schema/lineage context, ask the configured LLM provider to infer
supplementary business metadata:

- ownership
- granularity
- join keys
- freshness guarantees
- business logic summary
- sensitivity classification

When Atlas is running against an ACP runtime with direct repository access, the
workflow can instruct the external agent to inspect the repository via ACP
file-system and terminal tools instead of relying on pre-filtered inline file
snippets.

The module is READ-ONLY with respect to the repository: it never modifies any
file in the scanned path.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alma_atlas.agents.repo_scanner import collect_repo_files as _collect_repo_files
from alma_atlas.agents.schemas import AnnotationResult, AssetAnnotation

if TYPE_CHECKING:
    from alma_atlas.agents.provider import LLMProvider

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """\
You are an expert analytics engineer. Your task is to read the provided code
repository and infer business metadata for the listed data assets.

For each asset, report:
- asset_id: the fully-qualified asset ID (exactly as provided)
- ownership: team/person responsible (if present), else null
- granularity: the row-level grain (e.g. 'one row per user per day'), else null
- join_keys: list of likely join keys (column names), else empty list
- freshness_guarantee: update cadence or SLA if present, else null
- business_logic_summary: 1-2 sentence plain-English description, else null
- sensitivity: one of 'PII', 'financial', 'public' if inferable, else null

Rules:
- Do NOT fabricate. If the repository doesn't contain evidence, leave fields
  null/empty.
- Keep business_logic_summary short and concrete.
- If the prompt says direct repository access is available, inspect the repo
  using ACP file-system and terminal tools before answering.
- Agents are READ-ONLY: never suggest modifying the repository.
"""


def _build_user_prompt(
    assets: list[dict[str, Any]],
    repo_files: list[tuple[Path, str]],
    repo_path: Path,
    *,
    allow_repo_exploration: bool,
) -> str:
    parts: list[str] = ["## Assets to annotate\n"]
    parts.append(json.dumps({"assets": assets}, indent=2))

    if allow_repo_exploration:
        parts.append("\n## Repository access\n")
        parts.append(
            "You are running inside an ACP session whose current working directory "
            f"is the repository root: {repo_path}"
        )
        parts.append(
            "Use ACP file-system and terminal tools to inspect the repository "
            "directly. Prefer targeted search/read commands over broad scans."
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
        if allow_repo_exploration:
            parts.append("(no inline file excerpts were pre-selected; inspect the repository directly)")
        else:
            parts.append("(no relevant files found in repository)")

    return "\n".join(parts)


async def analyze_assets(
    assets: list[dict[str, Any]],
    repo_path: Path,
    provider: LLMProvider,
    *,
    pre_filtered_files: list[tuple[Path, str]] | None = None,
    allow_repo_exploration: bool = False,
) -> list[AssetAnnotation]:
    """Analyze a batch of assets against a repository and return annotations.

    Args:
        assets:              List of JSON-serializable dicts (asset context) produced by
                             the orchestrator. Each entry must include at least asset_id.
        repo_path:           Filesystem path to the code repository to scan.
        provider:            Configured LLM provider.
        pre_filtered_files:  Optional pre-selected ``(path, content)`` pairs from the
                             codebase explorer.  When provided, file scanning is skipped.
                             When ``None``, the standard glob scan is used.
        allow_repo_exploration:
                            When True, do not pre-scan files locally. Instead, instruct
                            the ACP-backed runtime to inspect the repository directly.

    Returns:
        Zero or more AssetAnnotation objects.
    """
    if not assets:
        return []

    if pre_filtered_files is not None:
        repo_files = pre_filtered_files
    elif allow_repo_exploration:
        repo_files = []
    else:
        repo_files = _collect_repo_files(repo_path)
    logger.debug(
        "annotator: %d asset(s), %d repo file(s) from %s (direct_repo=%s)",
        len(assets),
        len(repo_files),
        repo_path,
        allow_repo_exploration,
    )

    user_prompt = _build_user_prompt(
        assets,
        repo_files,
        repo_path,
        allow_repo_exploration=allow_repo_exploration,
    )
    try:
        result: AnnotationResult = await provider.analyze(
            _SYSTEM_PROMPT,
            user_prompt,
            AnnotationResult,
        )
    except Exception as exc:
        logger.warning("annotator: LLM call failed: %s", exc)
        return []

    return result.annotations
