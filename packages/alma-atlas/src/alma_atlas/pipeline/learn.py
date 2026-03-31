"""Learning orchestrator — identifies unlearned edges and calls the pipeline agent.

After cross-system edge discovery (schema_match / dbt_source_ref edges), this
module learns those edges with transport metadata inferred from a code
repository by the :mod:`alma_atlas.agents.pipeline_analyzer` agent.

Learning is idempotent: edges that already carry ``learning_status=learned``
in their metadata are skipped.

Lead/Specialist pattern
-----------------------
When a :class:`~alma_atlas.config.LearningConfig` is provided the orchestrator
uses a *lead/specialist* multi-agent pattern:

1. The **explorer** agent (cheap model) performs a two-pass file selection to
   find repository files relevant to the batch of edges or assets.
2. The **specialist** agent(s) receive only the pre-filtered files, reducing
   token usage and improving signal.

Both :func:`run_edge_learning` and :func:`run_asset_annotation` accept either a
single ``provider`` argument (legacy path) **or** a ``config`` keyword argument
(new path).  Both calling conventions are supported simultaneously.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.edge_repository import Edge, EdgeRepository

if TYPE_CHECKING:
    from alma_atlas.agents.provider import LLMProvider
    from alma_atlas.agents.schemas import EdgeEnrichment
    from alma_atlas.config import AgentConfig, LearningConfig
    from alma_atlas_store.db import Database

logger = logging.getLogger(__name__)

# Edge kinds that are candidates for pipeline learning.
#
# We include `depends_on` because dbt/SQL lineage edges are often the ones that have
# meaningful transport metadata (schedule, strategy, owner) in the surrounding pipeline code.
_LEARNABLE_KINDS: frozenset[str] = frozenset({"schema_match", "dbt_source_ref", "depends_on"})


def _is_real_provider(provider_name: str) -> bool:
    """Return True if the provider is a real (non-mock) LLM provider."""
    return provider_name != "mock"


def _effective_provider_name(agent_cfg: AgentConfig) -> str:
    """Return the runtime provider name for one agent config."""
    return "acp" if agent_cfg.agent is not None else agent_cfg.provider


def agent_config_is_enabled(agent_cfg: AgentConfig) -> bool:
    """Return True when one agent config is configured for non-mock execution."""
    return _is_real_provider(_effective_provider_name(agent_cfg))


def edge_learning_is_enabled(config: LearningConfig) -> bool:
    """Return True when edge learning has the required non-mock agents."""
    return agent_config_is_enabled(config.explorer) and agent_config_is_enabled(config.pipeline_analyzer)


def asset_annotation_is_enabled(config: LearningConfig) -> bool:
    """Return True when asset annotation has the required non-mock agents."""
    return agent_config_is_enabled(config.explorer) and agent_config_is_enabled(config.annotator)


def _provider_from_agent_config(agent_cfg: AgentConfig) -> LLMProvider:
    """Instantiate an LLMProvider from an :class:`~alma_atlas.config.AgentConfig`.

    When an :class:`~alma_atlas.config.AgentProcessConfig` is attached
    (``agent_cfg.agent is not None``), the ACP provider is used automatically
    regardless of the ``provider`` field value.
    """
    from alma_atlas.agents.provider import make_provider

    apc = agent_cfg.agent  # AgentProcessConfig or None

    # If an agent process config is present, default to ACP automatically.
    effective_provider = _effective_provider_name(agent_cfg)

    return make_provider(
        effective_provider,
        model=agent_cfg.model,
        agent_command=apc.command if apc else "claude-agent-acp",
        agent_args=list(apc.args) if apc else None,
        agent_env=dict(apc.env) if apc else None,
    )


def get_unlearned_edges(db: Database) -> list[Edge]:
    """Return edges that have not yet been learned by the pipeline analysis agent.

    An edge qualifies when its :attr:`~alma_atlas_store.edge_repository.Edge.kind`
    is one of the learnable kinds (``schema_match`` or ``dbt_source_ref``) and
    its metadata does not contain ``learning_status: learned``.

    Args:
        db: Open :class:`~alma_atlas_store.db.Database` connection.

    Returns:
        List of unlearned :class:`~alma_atlas_store.edge_repository.Edge` objects.
    """
    repo = EdgeRepository(db)
    return [
        e
        for e in repo.list_all()
        if e.kind in _LEARNABLE_KINDS
        # Cross-system edges (schema_match, dbt_source_ref) are ALWAYS learnable even when
        # schema.table matches — they represent real data flow between different systems
        # (e.g. pg::raw.users → dbt::raw.users = "how does raw data get loaded?").
        # Only skip true self-loops within the same source system.
        and not (
            e.upstream_id.split("::", 1)[0] == e.downstream_id.split("::", 1)[0]
            and e.upstream_id.split("::", 1)[-1] == e.downstream_id.split("::", 1)[-1]
        )
        and e.metadata.get("learning_status") != "learned"
    ]


def _object_part(asset_id: str) -> str:
    """Extract the ``schema.table`` portion from an asset ID like ``src::schema.table``."""
    return asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id


async def run_edge_learning(
    db: Database,
    repo_path: Path,
    provider: LLMProvider | None = None,
    *,
    config: LearningConfig | None = None,
) -> int:
    """Learn unlearned cross-system edges with pipeline transport metadata.

    Collects all unlearned edges, calls the pipeline analysis agent, and
    persists the returned :class:`~alma_atlas.agents.schemas.EdgeEnrichment`
    data back to the store.  Edges for which the agent returns no result are
    left unchanged.  Persistence failures are logged as warnings and do not
    abort the run.

    When *config* is provided the lead/specialist pattern is used:
    - The explorer agent (``config.explorer``) pre-filters repository files.
    - The pipeline analyzer (``config.pipeline_analyzer``) receives only
      the relevant files.

    Args:
        db:        Open :class:`~alma_atlas_store.db.Database` connection.
        repo_path: Filesystem path to the code repository to scan.
        provider:  Configured :class:`~alma_atlas.agents.provider.LLMProvider`
                   (legacy path, used when *config* is ``None``).
        config:    Per-agent :class:`~alma_atlas.config.LearningConfig`
                   (new path; takes precedence over *provider*).

    Returns:
        Number of edges successfully learned and persisted.
    """
    from alma_atlas.agents.pipeline_analyzer import analyze_edges

    unlearned = get_unlearned_edges(db)
    if not unlearned:
        logger.info("run_edge_learning: no unlearned edges found")
        return 0

    logger.info(
        "run_edge_learning: learning %d edge(s) from %s",
        len(unlearned),
        repo_path,
    )

    if config is not None:
        if not edge_learning_is_enabled(config):
            raise ValueError(
                "run_edge_learning requires non-mock explorer and pipeline_analyzer agent configs"
            )
        # Lead/specialist pattern.
        from alma_atlas.agents.codebase_explorer import explore_for_edges

        explorer_provider = _provider_from_agent_config(config.explorer)
        analyzer_provider = _provider_from_agent_config(config.pipeline_analyzer)

        # Deduplicate edges by (upstream_table, downstream_table) — schema_match and
        # dbt_source_ref for the same pair are redundant for learning purposes.
        seen_pairs: set[tuple[str, str]] = set()
        deduped: list[Edge] = []
        for e in unlearned:
            pair = (_object_part(e.upstream_id), _object_part(e.downstream_id))
            if pair not in seen_pairs:
                seen_pairs.add(pair)
                deduped.append(e)

        if len(deduped) < len(unlearned):
            logger.info(
                "run_edge_learning: deduplicated %d → %d edges for agent",
                len(unlearned),
                len(deduped),
            )

        # Batch edges to avoid overwhelming the LLM.  Large batches (>10 edges)
        # cause Opus to return empty results or malformed responses.
        _EDGE_BATCH_SIZE = 10
        enrichments: list = []
        for batch_start in range(0, len(deduped), _EDGE_BATCH_SIZE):
            batch = deduped[batch_start : batch_start + _EDGE_BATCH_SIZE]
            logger.info(
                "run_edge_learning: batch %d/%d (%d edges)",
                batch_start // _EDGE_BATCH_SIZE + 1,
                (len(deduped) + _EDGE_BATCH_SIZE - 1) // _EDGE_BATCH_SIZE,
                len(batch),
            )

            pre_filtered = await explore_for_edges(batch, repo_path, explorer_provider)
            logger.debug(
                "run_edge_learning: explorer pre-filtered %d file(s)",
                len(pre_filtered),
            )

            batch_enrichments = await analyze_edges(
                batch,
                repo_path,
                analyzer_provider,
                pre_filtered_files=pre_filtered,
            )
            enrichments.extend(batch_enrichments)
    elif provider is not None:
        # Legacy single-provider path.
        enrichments = await analyze_edges(unlearned, repo_path, provider)
    else:
        raise ValueError("run_edge_learning requires either 'provider' or 'config'")

    if not enrichments:
        logger.info("run_edge_learning: agent returned no results")
        return 0

    # Index enrichments by (schema.table, schema.table) for O(1) lookup.
    # Be forgiving: some models include system prefixes (e.g. "pg::raw.users").
    enrichment_index: dict[tuple[str, str], EdgeEnrichment] = {
        (_object_part(e.source_table), _object_part(e.dest_table)): e
        for e in enrichments
    }

    repo = EdgeRepository(db)
    learned_count = 0

    for edge in unlearned:
        src_obj = _object_part(edge.upstream_id)
        dst_obj = _object_part(edge.downstream_id)
        enrichment = enrichment_index.get((src_obj, dst_obj))
        if enrichment is None:
            logger.debug(
                "run_edge_learning: no match for %s → %s",
                edge.upstream_id,
                edge.downstream_id,
            )
            continue

        updated_metadata: dict = {
            **edge.metadata,
            "transport_kind": enrichment.transport_kind,
            "schedule": enrichment.schedule,
            "strategy": enrichment.strategy,
            "write_disposition": enrichment.write_disposition,
            "watermark_column": enrichment.watermark_column,
            "owner": enrichment.owner,
            "confidence_note": enrichment.confidence_note,
            "learning_status": "learned",
        }
        try:
            repo.upsert(
                Edge(
                    upstream_id=edge.upstream_id,
                    downstream_id=edge.downstream_id,
                    kind=edge.kind,
                    metadata=updated_metadata,
                )
            )
            learned_count += 1
            logger.debug(
                "run_edge_learning: persisted learning for %s → %s",
                edge.upstream_id,
                edge.downstream_id,
            )
        except Exception as exc:
            logger.warning(
                "run_edge_learning: failed to persist learning for %s → %s: %s",
                edge.upstream_id,
                edge.downstream_id,
                exc,
            )

    logger.info("run_edge_learning: %d edge(s) learned", learned_count)
    return learned_count


def get_unannotated_assets(db: Database, *, limit: int = 100) -> list[str]:
    """Return asset IDs that have no annotation record yet."""
    from alma_atlas_store.annotation_repository import AnnotationRepository

    return AnnotationRepository(db).list_unannotated(limit=limit)


async def run_asset_annotation(
    db: Database,
    repo_path: Path,
    provider: LLMProvider | None = None,
    *,
    config: LearningConfig | None = None,
    provider_name: str | None = None,
    model: str | None = None,
    limit: int = 100,
    batch_size: int = 20,
) -> int:
    """Annotate assets with supplementary business metadata.

    This is the P2 "Codex learning" path. It selects assets that have not yet
    been annotated, builds a context payload per asset (schema + basic lineage),
    calls the annotator agent, and persists results to the store.

    When *config* is provided the lead/specialist pattern is used:
    - The explorer agent (``config.explorer``) pre-filters repository files.
    - The annotator (``config.annotator``) receives only relevant files.

    Args:
        db:            Open Atlas database.
        repo_path:      Filesystem path to the code repository.
        provider:       Configured LLM provider (legacy path).
        config:         Per-agent LearningConfig (new path; takes precedence).
        provider_name:  Provider identifier used for provenance (legacy path).
        model:          Model identifier used for provenance (legacy path).
        limit:          Max assets to annotate in this run.
        batch_size:     Max assets per LLM call.

    Returns:
        Number of assets successfully annotated.
    """
    from alma_atlas.agents.annotator import analyze_assets
    from alma_atlas_store.annotation_repository import AnnotationRecord, AnnotationRepository
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_ids = get_unannotated_assets(db, limit=limit)
    if not asset_ids:
        logger.info("run_asset_annotation: no unannotated assets found")
        return 0

    # Resolve provider and provenance from config or legacy args.
    if config is not None:
        if not asset_annotation_is_enabled(config):
            raise ValueError(
                "run_asset_annotation requires non-mock explorer and annotator agent configs"
            )
        enricher_provider = _provider_from_agent_config(config.annotator)
        _provider_name = _effective_provider_name(config.annotator)
        _model = config.annotator.model
    elif provider is not None:
        enricher_provider = provider
        _provider_name = provider_name or "unknown"
        _model = model or "unknown"
    else:
        raise ValueError("run_asset_annotation requires either 'provider' or 'config'")

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    schema_repo = SchemaRepository(db)
    ann_repo = AnnotationRepository(db)

    # Preload edges once; we only need immediate neighbors.
    edges = edge_repo.list_all()
    upstream_by_asset: dict[str, list[str]] = {}
    downstream_by_asset: dict[str, list[str]] = {}
    for e in edges:
        downstream_by_asset.setdefault(e.upstream_id, []).append(e.downstream_id)
        upstream_by_asset.setdefault(e.downstream_id, []).append(e.upstream_id)

    annotated_by = f"agent:{_provider_name}:{_model}"
    annotated_count = 0

    # Batch asset contexts to respect context limits.
    for i in range(0, len(asset_ids), batch_size):
        batch_ids = asset_ids[i : i + batch_size]
        contexts: list[dict] = []
        for asset_id in batch_ids:
            asset = asset_repo.get(asset_id)
            if asset is None:
                continue
            schema = schema_repo.get_latest(asset_id)
            contexts.append(
                {
                    "asset_id": asset.id,
                    "source": asset.source,
                    "kind": asset.kind,
                    "name": asset.name,
                    "description": asset.description,
                    "tags": asset.tags,
                    "schema": (
                        {
                            "fingerprint": schema.fingerprint,
                            "columns": [
                                {"name": c.name, "type": c.type, "nullable": c.nullable}
                                for c in (schema.columns[:50] if schema else [])
                            ],
                        }
                        if schema
                        else None
                    ),
                    "lineage": {
                        "upstream": upstream_by_asset.get(asset_id, [])[:25],
                        "downstream": downstream_by_asset.get(asset_id, [])[:25],
                    },
                }
            )

        if not contexts:
            continue

        # Run explorer once per batch when using the lead/specialist pattern.
        if config is not None:
            from alma_atlas.agents.codebase_explorer import explore_for_assets

            explorer_provider = _provider_from_agent_config(config.explorer)
            pre_filtered = await explore_for_assets(contexts, repo_path, explorer_provider)
            logger.debug(
                "run_asset_annotation: explorer pre-filtered %d file(s) for batch %d",
                len(pre_filtered),
                i // batch_size,
            )
            annotations = await analyze_assets(
                contexts,
                repo_path,
                enricher_provider,
                pre_filtered_files=pre_filtered,
            )
        else:
            annotations = await analyze_assets(contexts, repo_path, enricher_provider)

        if not annotations:
            continue

        for ann in annotations:
            # Persist with provenance.
            try:
                ann_repo.upsert(
                    AnnotationRecord(
                        asset_id=ann.asset_id,
                        ownership=ann.ownership,
                        granularity=ann.granularity,
                        join_keys=ann.join_keys,
                        freshness_guarantee=ann.freshness_guarantee,
                        business_logic_summary=ann.business_logic_summary,
                        sensitivity=ann.sensitivity,
                        annotated_by=annotated_by,
                    )
                )
                annotated_count += 1
            except Exception as exc:
                logger.warning(
                    "run_asset_annotation: failed to persist annotation for %s: %s",
                    ann.asset_id,
                    exc,
                )

    logger.info("run_asset_annotation: %d asset(s) annotated", annotated_count)
    return annotated_count


# ---------------------------------------------------------------------------
# Backward compatibility aliases
# ---------------------------------------------------------------------------

#: Alias for :func:`get_unlearned_edges` (deprecated).
get_unenriched_edges = get_unlearned_edges

#: Alias for :func:`run_edge_learning` (deprecated).
run_enrichment = run_edge_learning

#: Alias for :func:`run_asset_annotation` (deprecated).
run_asset_enrichment = run_asset_annotation
