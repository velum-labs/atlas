"""Agent-backed MCP tools: curated context bundles and natural-language Q&A via the inner agent."""

from __future__ import annotations

from typing import Any

from mcp.types import TextContent

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp._common import AtlasToolSpec


def specs() -> tuple[AtlasToolSpec, ...]:
    """Return tool specs for the agent category."""
    return (
        AtlasToolSpec(
            name="atlas_context",
            description=(
                "Get the context you need to work with this data. Describe what you're trying to do "
                "and Atlas will gather relevant tables, join paths, column semantics, value "
                "distributions, and known pitfalls."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "What you're trying to do or answer",
                    },
                    "db_id": {
                        "type": "string",
                        "description": "Database or source identifier (optional scope)",
                    },
                    "evidence": {
                        "type": "string",
                        "description": "Optional hints or domain context",
                    },
                },
                "required": ["question"],
            },
        ),
        AtlasToolSpec(
            name="atlas_ask",
            description=(
                "Ask Atlas a question about your data. Returns an explanation grounded in schema, "
                "annotations, profiling stats, and lineage. No SQL -- just understanding."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "source_id": {
                        "type": "string",
                        "description": "Optional: scope to a specific source",
                    },
                },
                "required": ["question"],
            },
        ),
    )


def handlers():
    """Return the dispatch dict for the agent category."""
    return {
        "atlas_context": _handle_context,
        "atlas_ask": _handle_ask,
    }


async def _handle_context(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.agents.atlas_agent import run_atlas_context

    question = arguments["question"]
    db_id = arguments.get("db_id", "")
    evidence = arguments.get("evidence")
    result = await run_atlas_context(cfg, question, db_id=db_id, evidence=evidence)
    return [TextContent(type="text", text=result.model_dump_json(indent=2))]


async def _handle_ask(cfg: AtlasConfig, arguments: dict[str, Any]) -> list[TextContent]:
    from alma_atlas.agents.atlas_agent import run_atlas_ask

    question = arguments["question"]
    source_id = arguments.get("source_id")
    result = await run_atlas_ask(cfg, question, source_id=source_id)
    return [TextContent(type="text", text=result.model_dump_json(indent=2))]
