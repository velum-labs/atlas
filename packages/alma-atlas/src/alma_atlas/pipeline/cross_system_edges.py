"""Cross-system edge discovery — compares schema snapshots across source pairs.

After scanning multiple sources, this module runs EdgeDiscoveryEngine over each
ordered pair of (source_A, source_B) to find schema-matched tables that likely
represent the same data flowing between systems.  Discovered edges are upserted
into the Atlas store as ``Edge`` objects with ``kind="schema_match"``.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from alma_analysis.edge_discovery import EdgeDiscoveryConfig, EdgeDiscoveryEngine
from alma_atlas_store.edge_repository import Edge, EdgeRepository

if TYPE_CHECKING:
    from alma_atlas_store.db import Database
    from alma_connectors.source_adapter import SchemaSnapshot

logger = logging.getLogger(__name__)


def discover_cross_system_edges(
    snapshots: dict[str, SchemaSnapshot],
    db: Database,
    *,
    configs: dict[tuple[str, str], EdgeDiscoveryConfig] | None = None,
    default_config: EdgeDiscoveryConfig | None = None,
) -> int:
    """Discover edges between all ordered pairs of scanned sources.

    For each ordered pair of source IDs (A → B) where A ≠ B, instantiates an
    ``EdgeDiscoveryEngine`` and calls ``discover_edges`` on the two snapshots.
    Only edges that meet the configured match threshold are persisted.

    Asset IDs in the store use the format ``{source_id}::{schema}.{table}``,
    matching the convention established in :func:`alma_atlas.pipeline.scan.run_scan`.

    Args:
        snapshots:      Mapping of ``source_id → SchemaSnapshot`` from completed
                        scan runs.  Sources with no snapshot (e.g. failed scans)
                        should be omitted by the caller.
        db:             Open :class:`~alma_atlas_store.db.Database` connection.
        configs:        Optional per-pair config overrides keyed by
                        ``(source_id_a, source_id_b)``.
        default_config: Default :class:`EdgeDiscoveryConfig` applied when no
                        per-pair override exists.  Defaults to the engine's own
                        default (threshold 0.60).

    Returns:
        Total number of store edges upserted.
    """
    source_ids = list(snapshots)
    if len(source_ids) < 2:
        return 0

    edge_repo = EdgeRepository(db)
    total = 0

    for i, source_id_a in enumerate(source_ids):
        for source_id_b in source_ids[i + 1 :]:

            # PersistedSourceAdapter.key must match ^[a-z0-9][a-z0-9_-]*$ —
            # sanitize colons the same way _build_adapter() does.
            adapter_key_a = source_id_a.replace(":", "-")
            adapter_key_b = source_id_b.replace(":", "-")

            pair_config = (configs or {}).get((source_id_a, source_id_b), default_config)
            engine = EdgeDiscoveryEngine(
                source_adapter_key=adapter_key_a,
                dest_adapter_key=adapter_key_b,
                config=pair_config,
            )

            try:
                data_edges = engine.discover_edges(snapshots[source_id_a], snapshots[source_id_b])
            except Exception as exc:
                logger.warning(
                    "Edge discovery failed for %s <> %s: %s",
                    source_id_a,
                    source_id_b,
                    exc,
                )
                continue

            for data_edge in data_edges:
                edge_discovery_meta = data_edge.transport.metadata.get("edge_discovery", {})
                if not edge_discovery_meta.get("meets_threshold", False):
                    continue

                upstream_id = f"{source_id_a}::{data_edge.source_object}"
                downstream_id = f"{source_id_b}::{data_edge.dest_object}"
                metadata: dict[str, object] = {
                    **edge_discovery_meta,
                    "confidence": data_edge.confidence,
                }

                edge_repo.upsert(
                    Edge(
                        upstream_id=upstream_id,
                        downstream_id=downstream_id,
                        kind="schema_match",
                        metadata=metadata,
                    )
                )
                total += 1

    return total


__all__ = ["discover_cross_system_edges"]
