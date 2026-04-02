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
- column_notes: a dict mapping column names to brief notes. Only annotate
  columns where the name or type alone is insufficient. Focus on:
  - Value meanings not obvious from the column name
  - Storage format quirks (dates stored as TEXT, times as strings, encoded categories)
  - Null semantics when NULL means something specific
  - Units when ambiguous (e.g. milliseconds vs seconds, USD vs local currency)
  - Surrogate keys that look like they might carry business meaning
  - Common pitfalls (e.g. "do not aggregate this column directly")
  Skip self-explanatory columns. Only include what is non-obvious.
  Leave empty if all columns are self-explanatory.
- notes: optional table-level catch-all for conventions, data quality issues,
  implicit business rules, or anything that does not fit the typed fields above.
  Leave null if there is nothing important to add.
- properties: if you notice something important that does not fit any field
  above (e.g. known data quality issues, partition strategy, common misuse),
  add it here using a descriptive key. Leave empty if nothing else stands out.

Rules:
- Do NOT fabricate. If the repository and profiling data do not contain
  evidence, leave fields null/empty.
- Keep business_logic_summary short and concrete.
- Use column profiling stats (top values, null counts, distinct counts) as
  evidence when inferring column semantics and writing column_notes.
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
    # Extract column_profiles from asset contexts before JSON dump to render them separately.
    profiles_by_asset: dict[str, list] = {}
    clean_assets: list[dict[str, Any]] = []
    for asset in assets:
        profiles = asset.get("column_profiles")
        if profiles:
            profiles_by_asset[asset["asset_id"]] = profiles
        clean_assets.append({k: v for k, v in asset.items() if k != "column_profiles"})

    parts: list[str] = ["## Assets to annotate\n"]
    parts.append(json.dumps({"assets": clean_assets}, indent=2))

    if profiles_by_asset:
        parts.append("\n## Column profiling stats\n")
        for asset_id, profiles in profiles_by_asset.items():
            parts.append(f"### {asset_id}")
            for col in profiles:
                tokens: list[str] = [f"- {col['column_name']}:"]
                if col.get("distinct_count") is not None:
                    tokens.append(f"distinct={col['distinct_count']}")
                if col.get("null_count") is not None:
                    null_info = f"nulls={col['null_count']}"
                    if col.get("null_fraction") is not None:
                        null_info += f" ({col['null_fraction']:.1%})"
                    tokens.append(null_info)
                if col.get("min_value") is not None or col.get("max_value") is not None:
                    tokens.append(f"range=[{col.get('min_value', '?')}..{col.get('max_value', '?')}]")
                if col.get("top_values"):
                    top = col["top_values"][:5]
                    vals = ", ".join(
                        f"{t['value']}({t['count']})"
                        for t in top
                        if "value" in t and "count" in t
                    )
                    if vals:
                        tokens.append(f"top=[{vals}]")
                if col.get("sample_values"):
                    tokens.append(f"samples={col['sample_values'][:3]}")
                parts.append(" ".join(tokens))
            parts.append("")

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
