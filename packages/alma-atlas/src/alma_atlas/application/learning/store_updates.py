"""Persistence helpers for learning workflows."""

from __future__ import annotations

import logging

from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.edge_repository import EdgeRepository
from alma_ports.edge import Edge

logger = logging.getLogger(__name__)


def persist_edge_learning(db, unlearned_edges: list[Edge], enrichments: list) -> int:
    """Persist learned edge metadata and return the number of updates written."""
    enrichment_index = {
        (_object_part(enrichment.source_table), _object_part(enrichment.dest_table)): enrichment
        for enrichment in enrichments
    }

    repo = EdgeRepository(db)
    learned_count = 0

    for edge in unlearned_edges:
        src_obj = _object_part(edge.upstream_id)
        dst_obj = _object_part(edge.downstream_id)
        enrichment = enrichment_index.get((src_obj, dst_obj))
        if enrichment is None:
            logger.debug(
                "persist_edge_learning: no match for %s -> %s",
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
        except Exception as exc:
            logger.warning(
                "persist_edge_learning: failed to persist learning for %s -> %s: %s",
                edge.upstream_id,
                edge.downstream_id,
                exc,
            )

    return learned_count


def build_asset_annotation_contexts(
    db,
    asset_ids: list[str],
    column_profiles: dict[str, list] | None = None,
) -> list[dict]:
    """Build annotation contexts for one batch of assets."""
    from alma_atlas_store.edge_repository import EdgeRepository
    from alma_atlas_store.schema_repository import SchemaRepository

    asset_repo = AssetRepository(db)
    edge_repo = EdgeRepository(db)
    schema_repo = SchemaRepository(db)

    edges = edge_repo.list_all()
    upstream_by_asset: dict[str, list[str]] = {}
    downstream_by_asset: dict[str, list[str]] = {}
    for edge in edges:
        downstream_by_asset.setdefault(edge.upstream_id, []).append(edge.downstream_id)
        upstream_by_asset.setdefault(edge.downstream_id, []).append(edge.upstream_id)

    contexts: list[dict] = []
    for asset_id in asset_ids:
        asset = asset_repo.get(asset_id)
        if asset is None:
            continue
        schema = schema_repo.get_latest(asset_id)
        ctx: dict = {
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
                        {"name": column.name, "type": column.type, "nullable": column.nullable}
                        for column in (schema.columns[:50] if schema else [])
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
        if column_profiles and asset_id in column_profiles:
            ctx["column_profiles"] = [
                {
                    "column_name": p.column_name,
                    "distinct_count": p.distinct_count,
                    "null_count": p.null_count,
                    "null_fraction": p.null_fraction,
                    "min_value": p.min_value,
                    "max_value": p.max_value,
                    "top_values": p.top_values,
                    "sample_values": p.sample_values,
                }
                for p in column_profiles[asset_id]
            ]
        contexts.append(ctx)
    return contexts


def persist_annotations(
    db,
    *,
    annotations: list,
    annotated_by: str,
) -> int:
    """Persist annotation records and return the number written."""
    from alma_atlas_store.annotation_repository import AnnotationRecord, AnnotationRepository

    repo = AnnotationRepository(db)
    annotated_count = 0
    for annotation in annotations:
        try:
            props: dict = {**annotation.properties}
            if annotation.column_notes:
                props["column_notes"] = annotation.column_notes
            if annotation.notes is not None:
                props["notes"] = annotation.notes
            repo.upsert(
                AnnotationRecord(
                    asset_id=annotation.asset_id,
                    ownership=annotation.ownership,
                    granularity=annotation.granularity,
                    join_keys=annotation.join_keys,
                    freshness_guarantee=annotation.freshness_guarantee,
                    business_logic_summary=annotation.business_logic_summary,
                    sensitivity=annotation.sensitivity,
                    annotated_by=annotated_by,
                    properties=props,
                )
            )
            annotated_count += 1
        except Exception as exc:
            logger.warning(
                "persist_annotations: failed to persist annotation for %s: %s",
                annotation.asset_id,
                exc,
            )
    return annotated_count


def _object_part(asset_id: str) -> str:
    return asset_id.split("::", 1)[-1] if "::" in asset_id else asset_id
