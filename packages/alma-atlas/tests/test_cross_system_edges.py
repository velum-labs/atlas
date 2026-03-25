"""Tests for alma_atlas.pipeline.cross_system_edges."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from alma_atlas.pipeline.cross_system_edges import discover_cross_system_edges
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository
from alma_connectors.source_adapter import (
    SchemaObjectKind,
    SchemaSnapshot,
    SourceColumnSchema,
    SourceTableSchema,
)

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _col(name: str, data_type: str = "text") -> SourceColumnSchema:
    return SourceColumnSchema(name=name, data_type=data_type, is_nullable=False)


def _table(
    schema_name: str,
    object_name: str,
    *,
    columns: tuple[tuple[str, str], ...] = (),
    row_count: int | None = None,
) -> SourceTableSchema:
    return SourceTableSchema(
        schema_name=schema_name,
        object_name=object_name,
        object_kind=SchemaObjectKind.TABLE,
        columns=tuple(_col(name, dtype) for name, dtype in columns),
        row_count=row_count,
    )


def _snapshot(*objects: SourceTableSchema) -> SchemaSnapshot:
    return SchemaSnapshot(
        captured_at=datetime(2026, 3, 23, tzinfo=UTC),
        objects=objects,
    )


@pytest.fixture
def db():
    """In-memory Atlas database."""
    with Database(":memory:") as database:
        yield database


def _seed_assets(db: Database, snapshots: dict[str, SchemaSnapshot]) -> None:
    """Pre-insert assets for all objects in ``snapshots`` so FK constraints pass.

    Mirrors the asset_id format used by ``run_scan``:
        ``{source_id}::{schema_name}.{object_name}``
    """
    repo = AssetRepository(db)
    for source_id, snapshot in snapshots.items():
        for obj in snapshot.objects:
            asset_id = f"{source_id}::{obj.schema_name}.{obj.object_name}"
            repo.upsert(
                Asset(
                    id=asset_id,
                    source=source_id,
                    kind=obj.object_kind.value,
                    name=f"{obj.schema_name}.{obj.object_name}",
                )
            )


# ---------------------------------------------------------------------------
# discover_cross_system_edges — basic behaviour
# ---------------------------------------------------------------------------


def test_matching_tables_produce_edges(db: Database) -> None:
    """Two sources sharing a table name and columns should yield discovered edges."""
    snapshots = {
        "postgres:prod": _snapshot(
            _table(
                "public",
                "orders",
                columns=(("id", "uuid"), ("amount", "numeric"), ("created_at", "timestamp")),
            )
        ),
        "bigquery:warehouse": _snapshot(
            _table(
                "production",
                "orders",
                columns=(("id", "STRING"), ("amount", "NUMERIC"), ("created_at", "TIMESTAMP")),
            )
        ),
    }
    _seed_assets(db, snapshots)

    count = discover_cross_system_edges(snapshots, db)

    # Undirected: one canonical edge per unordered pair (postgres→bigquery only)
    assert count == 1
    edges = EdgeRepository(db).list_all()
    assert len(edges) == 1
    assert edges[0].upstream_id == "postgres:prod::public.orders"
    assert edges[0].downstream_id == "bigquery:warehouse::production.orders"
    assert all(e.kind == "schema_match" for e in edges)


def test_no_overlap_produces_no_edges(db: Database) -> None:
    """Sources with completely different table names should produce no edges."""
    snapshots = {
        "postgres:prod": _snapshot(
            _table("public", "users", columns=(("id", "uuid"), ("email", "text")))
        ),
        "bigquery:warehouse": _snapshot(
            _table("production", "events", columns=(("event_id", "STRING"), ("ts", "TIMESTAMP")))
        ),
    }

    count = discover_cross_system_edges(snapshots, db)

    assert count == 0
    assert EdgeRepository(db).list_all() == []


def test_single_source_skips_discovery(db: Database) -> None:
    """With only one source there are no pairs to compare; should return 0."""
    snapshots = {
        "postgres:prod": _snapshot(
            _table("public", "orders", columns=(("id", "uuid"),))
        )
    }

    count = discover_cross_system_edges(snapshots, db)

    assert count == 0


def test_empty_snapshots_returns_zero(db: Database) -> None:
    count = discover_cross_system_edges({}, db)
    assert count == 0


# ---------------------------------------------------------------------------
# Threshold filtering
# ---------------------------------------------------------------------------


def test_high_threshold_excludes_weak_matches(db: Database) -> None:
    """A threshold of 1.0 should exclude matches that are not perfect."""
    from alma_analysis.edge_discovery import EdgeDiscoveryConfig

    # Same table name, but completely different column sets → score < 1.0
    snapshots = {
        "postgres:prod": _snapshot(
            _table("public", "orders", columns=(("id", "uuid"), ("amount", "numeric")))
        ),
        "bigquery:warehouse": _snapshot(
            _table("production", "orders", columns=(("pk", "STRING"), ("total", "FLOAT64")))
        ),
    }

    count = discover_cross_system_edges(
        snapshots,
        db,
        default_config=EdgeDiscoveryConfig(match_threshold=1.0),
    )

    assert count == 0


def test_low_threshold_includes_partial_matches(db: Database) -> None:
    """A very low threshold should include table-name-only matches."""
    from alma_analysis.edge_discovery import EdgeDiscoveryConfig

    snapshots = {
        "postgres:prod": _snapshot(
            _table("public", "orders", columns=(("id", "uuid"),))
        ),
        "bigquery:warehouse": _snapshot(
            _table("production", "orders", columns=(("pk", "STRING"),))
        ),
    }
    _seed_assets(db, snapshots)

    count = discover_cross_system_edges(
        snapshots,
        db,
        default_config=EdgeDiscoveryConfig(match_threshold=0.0),
    )

    assert count >= 1


# ---------------------------------------------------------------------------
# Multiple source pairs
# ---------------------------------------------------------------------------


def test_three_sources_compare_all_pairs(db: Database) -> None:
    """Three sources should produce edges for each directional pair that matches."""
    matching_columns = (("id", "uuid"), ("name", "text"), ("created_at", "timestamp"))
    snapshots = {
        "postgres:prod": _snapshot(_table("public", "customers", columns=matching_columns)),
        "bigquery:warehouse": _snapshot(_table("prod_ds", "customers", columns=matching_columns)),
        "bigquery:reporting": _snapshot(_table("reporting", "customers", columns=matching_columns)),
    }
    _seed_assets(db, snapshots)

    count = discover_cross_system_edges(snapshots, db)

    # 3 sources → 3 unordered pairs (C(3,2)), each with a matching table.
    assert count == 3
    edges = EdgeRepository(db).list_all()
    assert len(edges) == 3


def test_source_not_compared_against_itself(db: Database) -> None:
    """Each source should never be compared against itself."""
    snapshots = {
        "postgres:prod": _snapshot(_table("public", "orders", columns=(("id", "uuid"),))),
        "bigquery:warehouse": _snapshot(_table("prod", "orders", columns=(("id", "STRING"),))),
    }
    _seed_assets(db, snapshots)

    discover_cross_system_edges(snapshots, db)
    edges = EdgeRepository(db).list_all()

    # No self-referential edges
    for edge in edges:
        source_prefix = edge.upstream_id.split("::")[0]
        dest_prefix = edge.downstream_id.split("::")[0]
        assert source_prefix != dest_prefix


# ---------------------------------------------------------------------------
# Metadata correctness
# ---------------------------------------------------------------------------


def test_edge_metadata_contains_confidence(db: Database) -> None:
    """Upserted edges should carry a confidence value in metadata."""
    snapshots = {
        "postgres:prod": _snapshot(
            _table(
                "public",
                "orders",
                columns=(("id", "uuid"), ("amount", "numeric"), ("created_at", "timestamp")),
            )
        ),
        "bigquery:warehouse": _snapshot(
            _table(
                "production",
                "orders",
                columns=(("id", "STRING"), ("amount", "NUMERIC"), ("created_at", "TIMESTAMP")),
            )
        ),
    }
    _seed_assets(db, snapshots)

    discover_cross_system_edges(snapshots, db)

    edges = EdgeRepository(db).list_all()
    assert len(edges) >= 1
    for edge in edges:
        meta = edge.metadata
        assert "confidence" in meta
        assert isinstance(meta["confidence"], float)
        assert 0.0 <= meta["confidence"] <= 1.0


def test_edge_metadata_contains_score_breakdown(db: Database) -> None:
    """Upserted edges should include the full score breakdown from EdgeDiscoveryEngine."""
    snapshots = {
        "postgres:prod": _snapshot(
            _table(
                "public",
                "orders",
                columns=(("id", "uuid"), ("amount", "numeric")),
            )
        ),
        "bigquery:warehouse": _snapshot(
            _table(
                "production",
                "orders",
                columns=(("id", "STRING"), ("amount", "NUMERIC")),
            )
        ),
    }
    _seed_assets(db, snapshots)

    discover_cross_system_edges(snapshots, db)

    edges = EdgeRepository(db).list_all()
    assert len(edges) >= 1
    for edge in edges:
        meta = edge.metadata
        assert "score_breakdown" in meta
        breakdown = meta["score_breakdown"]
        assert isinstance(breakdown, dict)
        assert "table_name_match" in breakdown
        assert "column_name_jaccard" in breakdown
        assert "total_score" in breakdown


def test_edge_metadata_meets_threshold_is_true(db: Database) -> None:
    """Only edges where meets_threshold=True should be persisted."""
    snapshots = {
        "postgres:prod": _snapshot(
            _table(
                "public",
                "orders",
                columns=(("id", "uuid"), ("amount", "numeric"), ("created_at", "timestamp")),
            )
        ),
        "bigquery:warehouse": _snapshot(
            _table(
                "production",
                "orders",
                columns=(("id", "STRING"), ("amount", "NUMERIC"), ("created_at", "TIMESTAMP")),
            )
        ),
    }
    _seed_assets(db, snapshots)

    discover_cross_system_edges(snapshots, db)

    edges = EdgeRepository(db).list_all()
    for edge in edges:
        assert edge.metadata.get("meets_threshold") is True


# ---------------------------------------------------------------------------
# Asset ID format
# ---------------------------------------------------------------------------


def test_upstream_downstream_id_format(db: Database) -> None:
    """Edge IDs must use the {source_id}::{schema}.{table} format."""
    snapshots = {
        "postgres:mydb": _snapshot(
            _table("public", "users", columns=(("id", "uuid"), ("email", "text")))
        ),
        "bigquery:my-project": _snapshot(
            _table("dataset", "users", columns=(("id", "STRING"), ("email", "STRING")))
        ),
    }
    _seed_assets(db, snapshots)

    discover_cross_system_edges(snapshots, db)

    edges = EdgeRepository(db).list_all()
    assert len(edges) >= 1
    # Find the postgres→bigquery edge
    pg_to_bq = [e for e in edges if e.upstream_id.startswith("postgres:mydb::")]
    assert len(pg_to_bq) == 1
    assert pg_to_bq[0].upstream_id == "postgres:mydb::public.users"
    assert pg_to_bq[0].downstream_id == "bigquery:my-project::dataset.users"


# ---------------------------------------------------------------------------
# Per-pair config overrides
# ---------------------------------------------------------------------------


def test_per_pair_config_overrides_default(db: Database) -> None:
    """A per-pair config should take precedence over the default config."""
    from alma_analysis.edge_discovery import EdgeDiscoveryConfig

    snapshots = {
        "postgres:prod": _snapshot(
            _table("public", "orders", columns=(("id", "uuid"), ("amount", "numeric")))
        ),
        "bigquery:warehouse": _snapshot(
            _table("production", "orders", columns=(("id", "STRING"), ("total", "NUMERIC")))
        ),
    }
    _seed_assets(db, snapshots)

    # Default is strict (1.0), but the specific pair has a lenient threshold.
    count = discover_cross_system_edges(
        snapshots,
        db,
        configs={("postgres:prod", "bigquery:warehouse"): EdgeDiscoveryConfig(match_threshold=0.0)},
        default_config=EdgeDiscoveryConfig(match_threshold=1.0),
    )

    # postgres→bigquery should find an edge (lenient override), bigquery→postgres should not (strict default).
    assert count >= 1
    edges = EdgeRepository(db).list_all()
    upstream_ids = {e.upstream_id for e in edges}
    assert any(uid.startswith("postgres:prod::") for uid in upstream_ids)
