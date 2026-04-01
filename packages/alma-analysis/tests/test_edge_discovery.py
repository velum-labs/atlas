"""Unit tests for schema-based edge discovery."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from alma_connectors.edge_model import EdgeTransport
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    ColumnSchema,
    ExtractionMeta,
    ExtractionScope,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
)

from alma_analysis.edge_discovery import (
    EdgeDiscoveryConfig,
    EdgeDiscoveryEngine,
)


def _table(
    schema_name: str,
    object_name: str,
    *,
    columns: tuple[tuple[str, str], ...],
    row_count: int | None = None,
) -> SchemaObject:
    return SchemaObject(
        schema_name=schema_name,
        object_name=object_name,
        kind=SchemaObjectKind.TABLE,
        columns=tuple(
            ColumnSchema(name=name, data_type=data_type, is_nullable=False)
            for name, data_type in columns
        ),
        row_count=row_count,
    )


def _snapshot(*objects: SchemaObject) -> SchemaSnapshotV2:
    return SchemaSnapshotV2(
        meta=ExtractionMeta(
            adapter_key="test-adapter",
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.SCHEMA,
            scope_context=ScopeContext(scope=ExtractionScope.DATABASE),
            captured_at=datetime(2026, 3, 18, tzinfo=UTC),
            duration_ms=10.0,
            row_count=len(objects),
        ),
        objects=objects,
    )


def _edge_discovery_metadata(transport: EdgeTransport) -> dict[str, object]:
    edge_discovery = transport.metadata["edge_discovery"]
    assert isinstance(edge_discovery, dict)
    return edge_discovery


def test_discover_edges_scores_exact_name_match_without_row_counts() -> None:
    engine = EdgeDiscoveryEngine(
        source_adapter_key="core-db",
        dest_adapter_key="warehouse",
    )
    source = _snapshot(
        _table(
            "public",
            "orders",
            columns=(
                ("id", "uuid"),
                ("amount", "numeric"),
                ("created_at", "timestamp with time zone"),
            ),
        )
    )
    dest = _snapshot(
        _table(
            "production",
            "orders",
            columns=(
                ("id", "STRING"),
                ("amount", "NUMERIC"),
                ("created_at", "TIMESTAMP"),
            ),
        )
    )

    edges = engine.discover_edges(source, dest)

    assert len(edges) == 1
    edge = edges[0]
    metadata = _edge_discovery_metadata(edge.transport)
    score_breakdown = metadata["score_breakdown"]

    assert edge.source_object == "public.orders"
    assert edge.dest_object == "production.orders"
    assert edge.confidence == pytest.approx(1.0)
    assert metadata["is_primary"] is True
    assert metadata["is_candidate"] is False
    assert metadata["meets_threshold"] is True
    assert score_breakdown == {
        "table_name_match": 1.0,
        "column_name_jaccard": 1.0,
        "type_compatibility_ratio": 1.0,
        "row_count_similarity": None,
        "total_score": pytest.approx(1.0),
    }


def test_discover_edges_applies_destination_dataset_scope() -> None:
    engine = EdgeDiscoveryEngine(
        source_adapter_key="core-db",
        dest_adapter_key="warehouse",
        config=EdgeDiscoveryConfig(dest_dataset_scope=("production",)),
    )
    source = _snapshot(_table("public", "orders", columns=(("id", "uuid"), ("amount", "numeric"))))
    dest = _snapshot(
        _table("dbt_artifacts", "orders", columns=(("id", "STRING"),)),
        _table(
            "production",
            "orders",
            columns=(("id", "STRING"), ("amount", "NUMERIC")),
        ),
    )

    edges = engine.discover_edges(source, dest)

    assert len(edges) == 1
    assert edges[0].dest_object == "production.orders"


def test_discover_edges_marks_highest_confidence_match_as_primary() -> None:
    engine = EdgeDiscoveryEngine(
        source_adapter_key="core-db",
        dest_adapter_key="warehouse",
        config=EdgeDiscoveryConfig(dest_dataset_scope=("warehouse", "staging")),
    )
    source = _snapshot(
        _table(
            "public",
            "customers",
            columns=(("id", "uuid"), ("email", "text"), ("status", "text")),
        )
    )
    dest = _snapshot(
        _table(
            "warehouse",
            "customers",
            columns=(("id", "STRING"), ("email", "STRING"), ("status", "STRING")),
        ),
        _table(
            "staging",
            "customers",
            columns=(("id", "STRING"), ("email", "STRING")),
        ),
    )

    edges = engine.discover_edges(source, dest)

    assert len(edges) == 2
    primary_metadata = _edge_discovery_metadata(edges[0].transport)
    secondary_metadata = _edge_discovery_metadata(edges[1].transport)

    assert edges[0].dest_object == "warehouse.customers"
    assert edges[1].dest_object == "staging.customers"
    assert edges[0].confidence > edges[1].confidence
    assert primary_metadata["is_primary"] is True
    assert primary_metadata["match_rank"] == 1
    assert secondary_metadata["is_primary"] is False
    assert secondary_metadata["is_candidate"] is False
    assert secondary_metadata["match_rank"] == 2


def test_discover_edges_preserves_below_threshold_candidates_with_threshold_matches() -> None:
    engine = EdgeDiscoveryEngine(
        source_adapter_key="core-db",
        dest_adapter_key="warehouse",
        config=EdgeDiscoveryConfig(match_threshold=0.60),
    )
    source = _snapshot(
        _table(
            "public",
            "payments",
            columns=(
                ("payment_id", "uuid"),
                ("amount", "numeric"),
                ("created_at", "timestamp"),
            ),
        )
    )
    dest = _snapshot(
        _table(
            "warehouse",
            "payments",
            columns=(
                ("payment_id", "STRING"),
                ("amount", "NUMERIC"),
                ("created_at", "TIMESTAMP"),
            ),
        ),
        _table(
            "candidate",
            "transfer_payments",
            columns=(
                ("payment_id", "STRING"),
                ("amount", "NUMERIC"),
                ("booked_at", "TIMESTAMP"),
            ),
        ),
    )

    edges = engine.discover_edges(source, dest)

    assert len(edges) == 2
    discovered = edges[0]
    candidate = edges[1]
    discovered_metadata = _edge_discovery_metadata(discovered.transport)
    candidate_metadata = _edge_discovery_metadata(candidate.transport)

    assert discovered.dest_object == "warehouse.payments"
    assert discovered_metadata["is_primary"] is True
    assert discovered_metadata["is_candidate"] is False
    assert discovered_metadata["match_rank"] == 1
    assert candidate.dest_object == "candidate.transfer_payments"
    assert candidate.confidence < 0.60
    assert candidate_metadata["is_primary"] is False
    assert candidate_metadata["is_candidate"] is True
    assert candidate_metadata["meets_threshold"] is False
    assert candidate_metadata["match_rank"] == 2


def test_discover_edges_returns_all_below_threshold_candidates() -> None:
    engine = EdgeDiscoveryEngine(
        source_adapter_key="core-db",
        dest_adapter_key="warehouse",
        config=EdgeDiscoveryConfig(match_threshold=0.60),
    )
    source = _snapshot(
        _table(
            "public",
            "payments",
            columns=(
                ("payment_id", "uuid"),
                ("amount", "numeric"),
                ("created_at", "timestamp"),
            ),
        )
    )
    dest = _snapshot(
        _table(
            "candidate",
            "transfer_payments",
            columns=(
                ("payment_id", "STRING"),
                ("amount", "NUMERIC"),
                ("booked_at", "TIMESTAMP"),
            ),
        ),
        _table(
            "candidate",
            "disbursements",
            columns=(("payment_id", "STRING"),),
        ),
    )

    edges = engine.discover_edges(source, dest)

    assert len(edges) == 2

    first_edge = edges[0]
    second_edge = edges[1]
    first_metadata = _edge_discovery_metadata(first_edge.transport)
    second_metadata = _edge_discovery_metadata(second_edge.transport)

    assert first_edge.dest_object == "candidate.transfer_payments"
    assert first_edge.confidence < 0.60
    assert first_metadata["is_primary"] is False
    assert first_metadata["is_candidate"] is True
    assert first_metadata["meets_threshold"] is False
    assert first_metadata["match_rank"] == 1

    assert second_edge.dest_object == "candidate.disbursements"
    assert second_edge.confidence < 0.60
    assert second_metadata["is_primary"] is False
    assert second_metadata["is_candidate"] is True
    assert second_metadata["meets_threshold"] is False
    assert second_metadata["match_rank"] == 2
