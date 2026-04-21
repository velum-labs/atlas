"""Atlas inner agent -- prompt-based context gathering for high-level MCP tools.

Implements the prompt-based approach for Phase 4: gather schema + annotations +
profiles for relevant assets, then call the LLM provider once for a structured
context package.  Falls back to raw gathered context when no real provider is
configured.
"""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

from alma_atlas.agents.atlas_agent_schemas import (
    AskResult,
    ColumnContext,
    ContextPackage,
    JoinRecommendation,
    VerificationResult,
)

# Imported at module level so unittest.mock.patch can target this module's namespace.
from alma_atlas.application.learning.runtime import (
    effective_provider_name,
    provider_from_agent_config,
)

if TYPE_CHECKING:
    from alma_atlas.config import AtlasConfig

logger = logging.getLogger(__name__)

_CONTEXT_SYSTEM_PROMPT = """\
You are Atlas, a data expert. Given a question about a database, you analyze the \
provided schema, annotations, profiling stats, and relationship data to build a \
useful context package.

Your exploration playbook:
1. Identify tables likely needed to answer the question.
2. Read annotations for business context (column meanings, join guidance, quirks).
3. Note columns involved in WHERE / JOIN / GROUP BY and their distributions.
4. Check relationships between candidate tables.
5. Identify potential pitfalls (surrogate keys, format quirks, ambiguous joins).

Return a structured context package:
- relevant_tables: tables likely needed
- recommended_joins: how to join them (join_path + reasoning)
- column_context: per-column notes for key columns
- warnings: anything the analyst should watch out for
- evidence_interpretation: if evidence/hints are provided, explain what they mean
- summary: 1-2 sentence overview

Be precise and grounded in the provided context. Do not invent information.\
"""

_ASK_SYSTEM_PROMPT = """\
You are Atlas, a data expert. Given a question about a database, answer it clearly \
based on the provided schema, annotations, and profiling stats.

Return:
- answer: direct answer to the question, grounded in the context
- sources: the specific tables and columns you referenced
- caveats: important caveats or limitations (nulls, surrogate keys, coverage gaps)

Do not write SQL. Focus on understanding and explanation.\
"""

_VERIFY_SYSTEM_PROMPT = """\
You are Atlas, a data quality expert. Given a SQL query and database context \
(schema, annotations, profiling stats), analyse the query for correctness.

Check for:
- Joins on wrong columns (type mismatches, surrogate keys used as business keys)
- Missing filters (tenant isolation, date ranges, soft deletes)
- Column semantics issues (aggregating surrogate keys, wrong nullability assumptions)
- Any join_guidance warnings that apply

Return:
- valid: true if the query looks correct, false if there are issues
- warnings: specific issues found (reference the annotation or guidance that triggered each)
- suggestions: suggested fixes
- analysis: overall analysis of the query\
"""


def _schema_fingerprint(asset_ids: list[str]) -> str:
    """Compute a short stable fingerprint from a sorted list of asset IDs."""
    joined = ",".join(sorted(asset_ids))
    return hashlib.sha256(joined.encode()).hexdigest()[:16]


def _gather_context(db_path: Path, question: str, db_id: str, *, limit: int = 5) -> dict[str, Any]:
    """Gather schema, annotations, and profiles for assets relevant to *question*.

    Returns a dict with keys ``assets``, ``relationships``, and ``asset_ids``.
    """
    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.profiling_repository import ProfilingRepository
    from alma_atlas_store.schema_repository import SchemaRepository

    assets_data: list[dict[str, Any]] = []

    with Database(db_path) as db:
        asset_repo = AssetRepository(db)
        schema_repo = SchemaRepository(db)
        annotation_repo = AnnotationRepository(db)
        profiling_repo = ProfilingRepository(db)
        edge_repo = EdgeRepository(db)

        # Search for relevant assets by question text.
        results = asset_repo.search(question)[:limit]

        # Also search by db_id scope when provided.
        if db_id:
            seen_ids = {a.id for a in results}
            for a in asset_repo.search(db_id)[:limit]:
                if a.id not in seen_ids:
                    results.append(a)

        results = results[:limit]

        for asset in results:
            asset_data: dict[str, Any] = {
                "id": asset.id,
                "kind": asset.kind,
                "source": asset.source,
                "name": asset.name,
                "schema": [],
                "annotation": None,
                "profiles": {},
            }

            # Schema columns.
            snapshot = schema_repo.get_latest(asset.id)
            if snapshot is not None:
                if isinstance(snapshot.columns, str):
                    col_list = json.loads(snapshot.columns)
                else:
                    col_list = [vars(c) if hasattr(c, "__dict__") else c for c in snapshot.columns]
                asset_data["schema"] = col_list

            # Annotations.
            annotation = annotation_repo.get(asset.id)
            if annotation is not None:
                column_notes = annotation.properties.get("column_notes", {})
                asset_data["annotation"] = {
                    "ownership": annotation.ownership,
                    "granularity": annotation.granularity,
                    "join_keys": annotation.join_keys,
                    "business_logic_summary": annotation.business_logic_summary,
                    "sensitivity": annotation.sensitivity,
                    "notes": annotation.properties.get("notes"),
                    "column_notes": column_notes,
                }

            # Column profiles.
            for p in profiling_repo.list_for_asset(asset.id):
                asset_data["profiles"][p.column_name] = {
                    "distinct_count": p.distinct_count,
                    "null_fraction": p.null_fraction,
                    "min_value": p.min_value,
                    "max_value": p.max_value,
                    "top_values": (p.top_values or [])[:5],
                }

            assets_data.append(asset_data)

        # Pairwise relationships between found assets.
        asset_ids = [a["id"] for a in assets_data]
        relationships: list[dict[str, Any]] = []
        for i, aid_a in enumerate(asset_ids):
            for aid_b in asset_ids[i + 1:]:
                edges = [
                    e
                    for e in edge_repo.list_for_asset(aid_a)
                    if (e.upstream_id == aid_a and e.downstream_id == aid_b)
                    or (e.upstream_id == aid_b and e.downstream_id == aid_a)
                ]
                if edges:
                    relationships.append(
                        {
                            "asset_a": aid_a,
                            "asset_b": aid_b,
                            "edges": [
                                {
                                    "kind": e.kind,
                                    "join_guidance": e.metadata.get("join_guidance"),
                                }
                                for e in edges
                            ],
                        }
                    )

    return {"assets": assets_data, "relationships": relationships, "asset_ids": asset_ids}


def _format_context_for_prompt(context_data: dict[str, Any]) -> str:
    """Format gathered context into a prompt-friendly text block."""
    parts: list[str] = []

    for asset in context_data["assets"]:
        parts.append(f"## Table: {asset['id']}")

        if asset["schema"]:
            parts.append("Schema:")
            for col in asset["schema"]:
                name = col.get("name", "?") if isinstance(col, dict) else getattr(col, "name", "?")
                dtype = col.get("type", "?") if isinstance(col, dict) else getattr(col, "type", "?")
                nullable = col.get("nullable", True) if isinstance(col, dict) else getattr(col, "nullable", True)
                parts.append(f"  {name}  {dtype}  {'NULL' if nullable else 'NOT NULL'}")

        ann = asset.get("annotation") or {}
        if ann.get("business_logic_summary"):
            parts.append(f"Business context: {ann['business_logic_summary']}")
        if ann.get("notes"):
            parts.append(f"Notes: {ann['notes']}")
        if ann.get("join_keys"):
            parts.append(f"Join keys: {', '.join(ann['join_keys'])}")
        col_notes: dict[str, str] = ann.get("column_notes") or {}
        if col_notes:
            parts.append("Column notes:")
            for col_name, note in col_notes.items():
                parts.append(f"  {col_name}: {note}")

        if asset["profiles"]:
            parts.append("Column profiles:")
            for col_name, p in asset["profiles"].items():
                profile_parts: list[str] = []
                if p.get("distinct_count") is not None:
                    profile_parts.append(f"distinct={p['distinct_count']}")
                if p.get("null_fraction") is not None:
                    profile_parts.append(f"nulls={p['null_fraction']:.1%}")
                if p.get("top_values"):
                    tv = [
                        (v.get("value", str(v)) if isinstance(v, dict) else str(v))
                        for v in p["top_values"][:3]
                    ]
                    profile_parts.append(f"top=[{', '.join(tv)}]")
                if profile_parts:
                    parts.append(f"  {col_name}: {', '.join(profile_parts)}")

        parts.append("")

    if context_data["relationships"]:
        parts.append("## Relationships")
        for rel in context_data["relationships"]:
            parts.append(f"{rel['asset_a']} <-> {rel['asset_b']}")
            for edge in rel["edges"]:
                parts.append(f"  kind={edge['kind']}")
                if edge.get("join_guidance"):
                    parts.append(f"  join_guidance: {edge['join_guidance']}")
        parts.append("")

    return "\n".join(parts)


def _fallback_context_package(context_data: dict[str, Any], question: str) -> ContextPackage:
    """Build a ContextPackage from raw gathered context without LLM reasoning."""
    relevant_tables = context_data["asset_ids"]

    recommended_joins: list[JoinRecommendation] = []
    for rel in context_data["relationships"]:
        for edge in rel["edges"]:
            recommended_joins.append(
                JoinRecommendation(
                    tables=[rel["asset_a"], rel["asset_b"]],
                    join_path=f"{rel['asset_a']} -- {rel['asset_b']}",
                    guidance=edge.get("join_guidance"),
                )
            )

    column_context: list[ColumnContext] = []
    warnings: list[str] = []
    for asset in context_data["assets"]:
        ann = asset.get("annotation") or {}
        col_notes: dict[str, str] = ann.get("column_notes") or {}
        for col in asset.get("schema", []):
            col_name = col.get("name", "") if isinstance(col, dict) else getattr(col, "name", "")
            note = col_notes.get(col_name)
            profile = asset["profiles"].get(col_name, {})
            top_vals = [
                (v.get("value", str(v)) if isinstance(v, dict) else str(v))
                for v in (profile.get("top_values") or [])[:5]
            ]
            column_context.append(
                ColumnContext(
                    column_name=f"{asset['id']}.{col_name}",
                    type=col.get("type", "unknown") if isinstance(col, dict) else getattr(col, "type", "unknown"),
                    annotation=note,
                    null_fraction=profile.get("null_fraction"),
                    top_values=top_vals,
                )
            )
            if note and "surrogate key" in note.lower():
                warnings.append(
                    f"Column '{col_name}' in '{asset['id']}' is a surrogate key: {note}"
                )

    return ContextPackage(
        relevant_tables=relevant_tables,
        recommended_joins=recommended_joins,
        column_context=column_context,
        warnings=warnings,
        summary=f"Found {len(relevant_tables)} relevant table(s) for: {question}",
    )


def _db_path_for_cfg(cfg: AtlasConfig) -> Path:
    from alma_atlas.application.query.service import require_db_path

    return require_db_path(cfg)


async def run_atlas_context(
    cfg: AtlasConfig,
    question: str,
    db_id: str = "",
    evidence: str | None = None,
) -> ContextPackage:
    """Gather a curated context package for *question*, using LLM if available."""
    from alma_atlas.agents.agent_cache import AgentCache
    from alma_atlas_store.db import Database

    db_path = _db_path_for_cfg(cfg)
    context_data = _gather_context(db_path, question, db_id)
    asset_ids = context_data["asset_ids"]

    agent_cfg = cfg.learning.annotator
    if effective_provider_name(agent_cfg) == "mock" or not asset_ids:
        return _fallback_context_package(context_data, question)

    fp = _schema_fingerprint(asset_ids)

    # Check cache first.
    with Database(db_path) as db:
        cached = AgentCache(db).get(question, db_id, fp)
    if cached is not None:
        try:
            return ContextPackage.model_validate(cached)
        except Exception:
            pass

    # Build prompt and call LLM.
    context_text = _format_context_for_prompt(context_data)
    user_parts = [f"Question: {question}"]
    if evidence:
        user_parts.append(f"Evidence/hints: {evidence}")
    user_parts += ["", "Available context:", context_text]
    user_prompt = "\n".join(user_parts)

    provider = provider_from_agent_config(agent_cfg)
    try:
        result = await provider.analyze(_CONTEXT_SYSTEM_PROMPT, user_prompt, ContextPackage)
        with Database(db_path) as db:
            AgentCache(db).put(question, db_id, fp, result.model_dump())
        return result
    except Exception as exc:
        logger.warning("atlas_context LLM call failed, using raw context: %s", exc)
        return _fallback_context_package(context_data, question)
    finally:
        await provider.aclose()


async def run_atlas_ask(
    cfg: AtlasConfig,
    question: str,
    source_id: str | None = None,
) -> AskResult:
    """Answer a data question grounded in Atlas schema, annotations, and profiles."""
    db_path = _db_path_for_cfg(cfg)
    context_data = _gather_context(db_path, question, source_id or "")
    asset_ids = context_data["asset_ids"]

    agent_cfg = cfg.learning.annotator
    if effective_provider_name(agent_cfg) == "mock":
        table_list = ", ".join(asset_ids) if asset_ids else "none found"
        return AskResult(
            answer=f"Found {len(asset_ids)} relevant table(s): {table_list}. No LLM provider configured for deeper analysis.",
            sources=asset_ids,
        )

    context_text = _format_context_for_prompt(context_data)
    user_prompt = f"Question: {question}\n\nAvailable context:\n{context_text}"

    provider = provider_from_agent_config(agent_cfg)
    try:
        return await provider.analyze(_ASK_SYSTEM_PROMPT, user_prompt, AskResult)
    except Exception as exc:
        logger.warning("atlas_ask LLM call failed: %s", exc)
        return AskResult(
            answer=f"Context gathered but LLM call failed: {exc}",
            sources=asset_ids,
        )
    finally:
        await provider.aclose()


async def run_verify_deep(
    cfg: AtlasConfig,
    sql: str,
    source_id: str | None = None,
    static_result: dict[str, Any] | None = None,
) -> VerificationResult:
    """Perform deep SQL verification with LLM analysis."""
    db_path = _db_path_for_cfg(cfg)
    context_data = _gather_context(db_path, sql, source_id or "")

    agent_cfg = cfg.learning.annotator
    if effective_provider_name(agent_cfg) == "mock":
        warnings: list[str] = (static_result or {}).get("warnings", [])
        suggestions: list[str] = (static_result or {}).get("suggestions", [])
        return VerificationResult(
            valid=len(warnings) == 0,
            warnings=warnings,
            suggestions=suggestions,
            analysis="No LLM provider configured for deep analysis.",
        )

    context_text = _format_context_for_prompt(context_data)
    static_summary = ""
    if static_result:
        static_summary = f"\nStatic analysis:\n{json.dumps(static_result)}\n"

    user_prompt = f"SQL Query:\n{sql}\n{static_summary}\nDatabase context:\n{context_text}"

    provider = provider_from_agent_config(agent_cfg)
    try:
        return await provider.analyze(_VERIFY_SYSTEM_PROMPT, user_prompt, VerificationResult)
    except Exception as exc:
        logger.warning("atlas_verify deep LLM call failed: %s", exc)
        warnings = (static_result or {}).get("warnings", [])
        suggestions = (static_result or {}).get("suggestions", [])
        return VerificationResult(
            valid=len(warnings) == 0,
            warnings=warnings,
            suggestions=suggestions,
            analysis=f"LLM analysis failed: {exc}",
        )
    finally:
        await provider.aclose()
