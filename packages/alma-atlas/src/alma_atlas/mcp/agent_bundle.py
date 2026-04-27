"""Customer-facing agent context bundle: schema, adapter, renderer.

Defines `CompanionBundle` — the response shape returned by the Atlas Companion
MCP tools — plus a translation layer that adapts the inner agent's
`ContextPackage` and a renderer that turns a bundle into a prompt-ready text
block for an agent like Cursor or Claude Desktop.

Per eng review Tension 1 (versioned adapter, not direct ContextPackage reuse):
`ContextPackage` lives under `agents/` as the inner-agent prompt/cache contract.
`CompanionBundle` is the customer-facing surface. Each evolves independently;
the `adapt()` function is the explicit, versioned bridge between them.

The renderer caps column lists, related-asset lists, and relationship lists at
fixed limits with an "ask for more" affordance, so prompt budgets stay
predictable regardless of graph size.
"""

from __future__ import annotations

from pydantic import BaseModel, Field

from alma_atlas.agents.atlas_agent_schemas import ContextPackage

# Render budgets: keep agent prompts predictable.
MAX_COLUMNS_SHOWN = 12
MAX_RELATED_ASSETS_SHOWN = 8
MAX_RELATIONSHIPS_SHOWN = 8


class CompanionColumn(BaseModel):
    """One column of an asset's schema as surfaced to an agent."""

    name: str
    type: str
    nullable: bool = True
    annotation: str | None = None


class CompanionAsset(BaseModel):
    """A data asset described for an agent: identity, owner, schema."""

    asset_id: str
    kind: str = "unknown"  # bigquery, postgres, snowflake, dbt, ...
    source: str = "unknown"  # source identifier
    owner: str | None = None
    last_updated: str | None = None
    description: str | None = None
    columns: list[CompanionColumn] = Field(default_factory=list)
    columns_total: int = 0  # total count even if `columns` is truncated for prompt budget


class CompanionRelationship(BaseModel):
    """A relationship between two assets: lineage edge, join path, FK reference."""

    upstream: str
    downstream: str
    kind: str = "edge"  # join_path, foreign_key, lineage_edge, ...
    guidance: str | None = None  # human-friendly description of how to use the relationship


class CompanionBundle(BaseModel):
    """Versioned customer-facing agent context returned by Companion MCP tools.

    Decoupled from `ContextPackage` (the inner-agent's contract) so the customer
    surface can evolve independently. The version field signals breaking
    changes to MCP clients.
    """

    version: str = "v1"
    primary_asset: CompanionAsset | None = None
    related_assets: list[CompanionAsset] = Field(default_factory=list)
    relationships: list[CompanionRelationship] = Field(default_factory=list)
    summary: str | None = None  # 1-2 sentence overview for the agent
    warnings: list[str] = Field(default_factory=list)
    context_note: str | None = None  # business context (e.g., "amounts in CLP not USD")


def adapt(package: ContextPackage) -> CompanionBundle:
    """Translate the inner-agent's ContextPackage into a CompanionBundle.

    The mapping is intentionally lossy: ContextPackage carries fields shaped for
    the inner agent's evidence assembly; CompanionBundle carries fields shaped
    for prompt-window agent context. Adding new ContextPackage fields does not
    automatically expose them to the Companion surface.
    """
    related = [
        CompanionAsset(asset_id=table)
        for table in package.relevant_tables
    ]
    relationships: list[CompanionRelationship] = []
    for join in package.recommended_joins:
        upstream = join.tables[0] if len(join.tables) > 0 else ""
        downstream = join.tables[1] if len(join.tables) > 1 else ""
        relationships.append(
            CompanionRelationship(
                upstream=upstream,
                downstream=downstream,
                kind="join_path",
                guidance=join.guidance or join.join_path,
            )
        )
    warnings = list(package.warnings)
    return CompanionBundle(
        related_assets=related,
        relationships=relationships,
        summary=package.summary,
        warnings=warnings,
        context_note=package.evidence_interpretation,
    )


def render(bundle: CompanionBundle) -> str:
    """Render a CompanionBundle as a prompt-ready text block for an agent.

    Empty bundle renders as a helpful "no results" message so the agent can
    handle absent context gracefully without parsing an empty string.
    """
    if (
        bundle.primary_asset is None
        and not bundle.related_assets
        and not bundle.relationships
        and not bundle.summary
        and not bundle.warnings
        and not bundle.context_note
    ):
        return "No matching context found in your data stack."

    sections: list[str] = []

    if bundle.summary:
        sections.append(bundle.summary)

    if bundle.primary_asset is not None:
        sections.append(_render_primary_asset(bundle.primary_asset))

    if bundle.related_assets:
        sections.append(_render_related_assets(bundle.related_assets))

    if bundle.relationships:
        sections.append(_render_relationships(bundle.relationships))

    if bundle.context_note:
        sections.append(f"Context: {bundle.context_note}")

    if bundle.warnings:
        warn_lines = ["Warnings:"]
        for w in bundle.warnings:
            warn_lines.append(f"  - {w}")
        sections.append("\n".join(warn_lines))

    return "\n\n".join(sections)


def _render_primary_asset(asset: CompanionAsset) -> str:
    lines = [f"{asset.asset_id} [{asset.kind} -> {asset.source}]"]
    if asset.owner:
        lines.append(f"Owner: {asset.owner}")
    if asset.last_updated:
        lines.append(f"Updated: {asset.last_updated}")
    if asset.description:
        lines.append(f"Description: {asset.description}")
    if asset.columns:
        shown = asset.columns[:MAX_COLUMNS_SHOWN]
        total = max(asset.columns_total, len(asset.columns))
        more = total > len(shown)
        col_header = (
            f"Schema ({len(shown)} of {total} columns shown - ask for more):"
            if more
            else f"Schema ({len(shown)} columns):"
        )
        lines.append(col_header)
        for col in shown:
            null_marker = "" if col.nullable else ", NOT NULL"
            note = f" -- {col.annotation}" if col.annotation else ""
            lines.append(f"  {col.name} ({col.type}{null_marker}){note}")
    return "\n".join(lines)


def _render_related_assets(assets: list[CompanionAsset]) -> str:
    shown = assets[:MAX_RELATED_ASSETS_SHOWN]
    more = len(assets) > len(shown)
    header = (
        f"Related assets ({len(shown)} of {len(assets)} shown):"
        if more
        else f"Related assets ({len(shown)}):"
    )
    lines = [header]
    for asset in shown:
        owner = f" -- {asset.owner}" if asset.owner else ""
        lines.append(f"  {asset.asset_id}{owner}")
    return "\n".join(lines)


def _render_relationships(relationships: list[CompanionRelationship]) -> str:
    shown = relationships[:MAX_RELATIONSHIPS_SHOWN]
    more = len(relationships) > len(shown)
    header = (
        f"Relationships ({len(shown)} of {len(relationships)} shown):"
        if more
        else f"Relationships ({len(shown)}):"
    )
    lines = [header]
    for rel in shown:
        guidance = f" -- {rel.guidance}" if rel.guidance else ""
        lines.append(f"  {rel.upstream} -> {rel.downstream} ({rel.kind}){guidance}")
    return "\n".join(lines)
