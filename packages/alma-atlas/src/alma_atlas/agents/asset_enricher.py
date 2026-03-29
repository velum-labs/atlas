"""Asset enricher — scans a code repository and produces per-asset annotations.

This is the P2 "Codex enrichment" layer: given a set of asset IDs and their
current schema/lineage context, ask the configured LLM provider to infer
supplementary business metadata:

- ownership
- granularity
- join keys
- freshness guarantees
- business logic summary
- sensitivity classification

The module is READ-ONLY with respect to the repository: it never modifies any
file in the scanned path.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alma_atlas.agents.schemas import AssetAnnotation, AssetEnrichmentResult

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
- Agents are READ-ONLY: never suggest modifying the repository.
"""


def _build_user_prompt(
    assets: list[dict[str, Any]],
    repo_files: list[tuple[Path, str]],
    repo_path: Path,
) -> str:
    parts: list[str] = ["## Assets to annotate\n"]
    parts.append(json.dumps({"assets": assets}, indent=2))

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


async def analyze_assets(
    assets: list[dict[str, Any]],
    repo_path: Path,
    provider: LLMProvider,
) -> list[AssetAnnotation]:
    """Analyze a batch of assets against a repository and return annotations.

    Args:
        assets:     List of JSON-serializable dicts (asset context) produced by
                   the orchestrator. Each entry must include at least asset_id.
        repo_path:  Filesystem path to the code repository to scan.
        provider:   Configured LLM provider.

    Returns:
        Zero or more AssetAnnotation objects.
    """
    if not assets:
        return []

    # Reuse the same repo file scanning strategy as pipeline_analyzer.
    from alma_atlas.agents.pipeline_analyzer import _collect_repo_files  # local import

    repo_files = _collect_repo_files(repo_path)
    logger.debug(
        "asset_enricher: %d asset(s), %d repo file(s) from %s",
        len(assets),
        len(repo_files),
        repo_path,
    )

    user_prompt = _build_user_prompt(assets, repo_files, repo_path)
    try:
        result: AssetEnrichmentResult = await provider.analyze(
            _SYSTEM_PROMPT,
            user_prompt,
            AssetEnrichmentResult,
        )
    except Exception as exc:
        logger.warning("asset_enricher: LLM call failed: %s", exc)
        return []

    return result.annotations
