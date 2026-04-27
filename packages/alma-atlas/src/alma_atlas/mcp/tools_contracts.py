"""Contract-oriented MCP tools: contract checks, violations, SQL verification, business term definitions."""

from __future__ import annotations

import contextlib
import json
from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec, _db_path


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the contracts category."""
    return (
        AtlasToolSpec(
            name="atlas_check_contract",
            description="Validate the current schema snapshot for an asset against its data contract spec, reporting any violations.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {"type": "string", "description": "Asset ID to check contracts for"},
                },
                "required": ["asset_id"],
            },
        ),
        AtlasToolSpec(
            name="atlas_list_violations",
            description="List recent enforcement violations stored in Atlas. Optionally filter by asset ID.",
            input_schema={
                "type": "object",
                "properties": {
                    "asset_id": {
                        "type": "string",
                        "description": "Filter violations to a specific asset ID (omit for all assets)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of violations to return",
                        "default": 50,
                    },
                },
            },
        ),
        AtlasToolSpec(
            name="atlas_verify",
            description="Check if something is correct -- a SQL query, a join path, a metric definition. Atlas will validate against its learned knowledge.",
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {"type": "string", "description": "SQL query to verify"},
                    "source_id": {"type": "string", "description": "Source/database context"},
                    "deep": {
                        "type": "boolean",
                        "description": "Use Atlas agent for deeper LLM-backed analysis (default: false)",
                        "default": False,
                    },
                },
                "required": ["sql"],
            },
        ),
        AtlasToolSpec(
            name="atlas_define_term",
            description="Define or update a business term (e.g., 'revenue', 'active user') with its definition, formula, and referenced columns.",
            input_schema={
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "definition": {"type": "string"},
                    "formula": {"type": "string"},
                    "referenced_columns": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["name"],
            },
        ),
    )


def handlers():
    """Return the dispatch dict for the contracts category.

    Note: atlas_verify dispatches between sync (`_handle_verify`) and async
    (`_handle_verify_deep`) variants based on the `deep` flag.
    """
    return {
        "atlas_check_contract": _handle_check_contract,
        "atlas_list_violations": _handle_list_violations,
        "atlas_verify": _dispatch_verify,
        "atlas_define_term": _handle_define_term,
    }


def _handle_check_contract(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.contracts.use_cases import check_asset_contracts

    asset_id = arguments["asset_id"]
    checks = check_asset_contracts(cfg, asset_id)
    if not checks:
        return [TextContent(type="text", text=f"No contracts found for asset: {asset_id}")]

    violations: list[str] = []
    for check in checks:
        issues = check.issues
        violations.extend(str(issue.get("message", "Unknown contract validation issue")) for issue in issues)

    if not violations:
        lines = [
            f"Contract check PASSED for {asset_id}",
            f"  {len(checks)} contract(s) validated, no violations found.",
        ]
    else:
        lines = [f"Contract check FAILED for {asset_id}: {len(violations)} violation(s)\n"]
        for v in violations:
            lines.append(f"  ✗ {v}")

    return [TextContent(type="text", text="\n".join(lines))]


def _handle_list_violations(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.application.query.service import list_violations

    asset_id = arguments.get("asset_id")
    limit = arguments.get("limit", 50)
    violations = list_violations(_db_path(cfg), asset_id=asset_id, limit=limit)

    if not violations:
        msg = f"No open violations found for {asset_id!r}." if asset_id else "No open violations found."
        return [TextContent(type="text", text=msg)]

    lines = [f"{len(violations)} open violation(s):\n"]
    for v in violations:
        resolved = " [resolved]" if v.resolved_at else ""
        lines.append(
            f"  [{v.severity}] {v.asset_id}  {v.violation_type}{resolved}"
            f"\n    {v.details.get('message', json.dumps(v.details))}"
            f"\n    detected: {v.detected_at}"
        )
    return [TextContent(type="text", text="\n".join(lines))]


def _handle_verify(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    import sqlglot
    from sqlglot import exp

    from alma_atlas_store.annotation_repository import AnnotationRepository
    from alma_atlas_store.asset_repository import AssetRepository
    from alma_atlas_store.db import Database
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_sqlkit.table_refs import extract_tables_from_sql

    sql = arguments.get("sql", "").strip()
    if not sql:
        result = {"valid": False, "warnings": ["No SQL provided."], "suggestions": []}
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    try:
        table_refs = extract_tables_from_sql(sql)
    except Exception as exc:
        result = {"valid": False, "warnings": [f"SQL parse error: {exc}"], "suggestions": []}
        return [TextContent(type="text", text=json.dumps(result, indent=2))]

    warnings: list[str] = []
    suggestions: list[str] = []

    agg_columns: set[str] = set()
    try:
        parsed = sqlglot.parse_one(sql)
        for agg in parsed.find_all(exp.Sum, exp.Avg):
            for col in agg.find_all(exp.Column):
                agg_columns.add(col.name.lower())
    except Exception:
        pass

    with Database(_db_path(cfg)) as db:
        asset_repo = AssetRepository(db)
        annotation_repo = AnnotationRepository(db)
        edge_repo = EdgeRepository(db)

        table_asset_map: dict[str, str] = {}
        for ref in table_refs:
            candidates = asset_repo.search(ref.canonical_name)
            if candidates:
                best = next(
                    (a for a in candidates if a.name == ref.canonical_name),
                    candidates[0],
                )
                table_asset_map[ref.canonical_name] = best.id

        for asset_id in table_asset_map.values():
            annotation = annotation_repo.get(asset_id)
            if annotation is None:
                continue
            column_notes = annotation.properties.get("column_notes", {})
            for col_name, note in column_notes.items():
                if note and "surrogate key" in note.lower() and col_name.lower() in agg_columns:
                    warnings.append(
                        f"Column '{col_name}' in '{asset_id}' is annotated as a surrogate key "
                        f"but appears in an aggregate function (SUM/AVG): {note}"
                    )

        table_names = list(table_asset_map.keys())
        for i, name_a in enumerate(table_names):
            for name_b in table_names[i + 1:]:
                asset_a = table_asset_map[name_a]
                asset_b = table_asset_map[name_b]
                for edge in edge_repo.list_for_asset(asset_a):
                    pair_match = (
                        (edge.upstream_id == asset_a and edge.downstream_id == asset_b)
                        or (edge.upstream_id == asset_b and edge.downstream_id == asset_a)
                    )
                    if pair_match:
                        join_guidance = edge.metadata.get("join_guidance")
                        if join_guidance:
                            warnings.append(
                                f"JOIN between '{name_a}' and '{name_b}': {join_guidance}"
                            )

    result = {"valid": len(warnings) == 0, "warnings": warnings, "suggestions": suggestions}
    return [TextContent(type="text", text=json.dumps(result, indent=2))]


async def _handle_verify_deep(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.agents.atlas_agent import run_verify_deep

    sql = arguments.get("sql", "").strip()
    source_id = arguments.get("source_id")

    static_texts = _handle_verify(cfg, {k: v for k, v in arguments.items() if k != "deep"})
    static_result: dict[str, Any] | None = None
    if static_texts:
        with contextlib.suppress(Exception):
            static_result = json.loads(static_texts[0].text)

    result = await run_verify_deep(cfg, sql, source_id=source_id, static_result=static_result)
    return [TextContent(type="text", text=result.model_dump_json(indent=2))]


def _dispatch_verify(cfg: AtlasConfig, arguments: dict[str, Any]):
    """Dispatch atlas_verify to static or deep handler based on the 'deep' flag."""
    if arguments.get("deep"):
        return _handle_verify_deep(cfg, arguments)
    return _handle_verify(cfg, arguments)


def _handle_define_term(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas_store.business_term_repository import BusinessTermRepository
    from alma_atlas_store.db import Database
    from alma_ports.business_term import BusinessTerm

    name = arguments["name"]
    definition = arguments.get("definition")
    formula = arguments.get("formula")
    referenced_columns = arguments.get("referenced_columns", [])

    term = BusinessTerm(
        name=name,
        definition=definition,
        formula=formula,
        referenced_columns=referenced_columns,
        source="manual",
    )

    with Database(_db_path(cfg)) as db:
        BusinessTermRepository(db).upsert(term)

    parts = [f"Business term '{name}' defined."]
    if definition:
        parts.append(f"Definition: {definition}")
    if formula:
        parts.append(f"Formula: {formula}")
    if referenced_columns:
        parts.append(f"Referenced columns: {', '.join(referenced_columns)}")

    return [TextContent(type="text", text="\n".join(parts))]
