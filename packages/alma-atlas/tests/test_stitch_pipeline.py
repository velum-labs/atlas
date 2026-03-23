"""Tests for alma_atlas.pipeline.stitch — stitch()."""

from __future__ import annotations

from datetime import UTC, datetime

from alma_atlas.pipeline.stitch import stitch
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository
from alma_atlas_store.query_repository import QueryRepository
from alma_connectors.source_adapter import ObservedQueryEvent, TrafficObservationResult

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _event(sql: str, user: str = "analyst") -> ObservedQueryEvent:
    return ObservedQueryEvent(
        captured_at=datetime.now(UTC),
        sql=sql,
        source_name="test",
        query_type="SELECT",
        database_user=user,
    )


def _traffic(*sqls: str, user: str = "analyst") -> TrafficObservationResult:
    events = tuple(_event(sql, user) for sql in sqls)
    return TrafficObservationResult(scanned_records=len(events), events=events)


def _seed_assets(db: Database, *asset_ids: str) -> None:
    """Pre-seed asset IDs so FK constraints are satisfied."""
    repo = AssetRepository(db)
    for aid in asset_ids:
        repo.upsert(Asset(id=aid, source="test", kind="table", name=aid))


# ---------------------------------------------------------------------------
# Basic stitch behaviour
# ---------------------------------------------------------------------------


def test_stitch_returns_int(db: Database) -> None:
    _seed_assets(db, "public.orders", "pg:test::query::analyst")
    traffic = _traffic("SELECT id FROM public.orders")
    result = stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    assert isinstance(result, int)


def test_stitch_empty_traffic_returns_zero(db: Database) -> None:
    traffic = TrafficObservationResult(scanned_records=0, events=())
    assert stitch(traffic, db, source_id="pg:test") == 0


def test_stitch_no_tables_sql_produces_no_edges(db: Database) -> None:
    """A SQL event with no table references produces no edges."""
    # SELECT without FROM produces no table refs → no edges
    traffic = _traffic("SELECT 1")
    result = stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    assert result == 0


def test_stitch_writes_edges(db: Database) -> None:
    _seed_assets(db, "public.orders", "pg:test::query::analyst")
    traffic = _traffic("SELECT id FROM public.orders")
    stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    edges = EdgeRepository(db).list_all()
    assert len(edges) >= 1


def test_stitch_edge_upstream_is_table(db: Database) -> None:
    _seed_assets(db, "public.orders", "pg:test::query::analyst")
    traffic = _traffic("SELECT id FROM public.orders")
    stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    edges = EdgeRepository(db).list_all()
    upstreams = [e.upstream_id for e in edges]
    assert any("orders" in u for u in upstreams)


def test_stitch_edge_downstream_is_consumer(db: Database) -> None:
    _seed_assets(db, "public.orders", "pg:test::query::alice")
    traffic = _traffic("SELECT id FROM public.orders", user="alice")
    stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    edges = EdgeRepository(db).list_all()
    downstreams = [e.downstream_id for e in edges]
    assert any("alice" in d for d in downstreams)


def test_stitch_writes_query_observation(db: Database) -> None:
    _seed_assets(db, "public.orders", "pg:test::query::analyst")
    traffic = _traffic("SELECT id FROM public.orders")
    stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    observations = QueryRepository(db).list_all()
    assert len(observations) >= 1


def test_stitch_insert_into_uses_target_as_downstream(db: Database) -> None:
    """For INSERT INTO ... SELECT, downstream should be the target table, not the consumer."""
    sql = "INSERT INTO staging.results SELECT id FROM public.orders"
    _seed_assets(db, "public.orders", "staging.results", "pg:test::query::analyst")
    traffic = _traffic(sql)
    stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    edges = EdgeRepository(db).list_all()
    downstreams = {e.downstream_id for e in edges}
    # The downstream should be the insert target, not a consumer query ID
    assert any("results" in d or "staging" in d for d in downstreams)


def test_stitch_multiple_events(db: Database) -> None:
    _seed_assets(db, "public.orders", "public.customers", "pg:test::query::analyst")
    traffic = _traffic(
        "SELECT id FROM public.orders",
        "SELECT name FROM public.customers",
    )
    count = stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    assert count >= 2


def test_stitch_invalid_sql_does_not_raise(db: Database) -> None:
    traffic = _traffic("THIS IS NOT VALID SQL AT ALL !!!!")
    # Should not raise — invalid SQL is silently skipped
    result = stitch(traffic, db, source_id="pg:test", source_kind="postgres")
    assert isinstance(result, int)


def test_stitch_uses_postgres_dialect_by_default(db: Database) -> None:
    _seed_assets(db, "public.orders", "pg:test::query::analyst")
    traffic = _traffic("SELECT id FROM public.orders")
    # No source_kind provided — should default to postgres and not raise
    result = stitch(traffic, db, source_id="pg:test")
    assert isinstance(result, int)


def test_stitch_bigquery_dialect(db: Database) -> None:
    # BQ SQL parser returns "dataset.orders" as canonical name (drops project prefix)
    _seed_assets(db, "dataset.orders", "bq-proj::query::analyst")
    traffic = _traffic("SELECT id FROM `myproject.dataset.orders`")
    result = stitch(traffic, db, source_id="bq-proj", source_kind="bigquery")
    assert isinstance(result, int)
