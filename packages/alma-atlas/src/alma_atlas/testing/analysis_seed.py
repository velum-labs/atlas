"""Shared Atlas analysis seed data for tests."""

from __future__ import annotations

from pathlib import Path

from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.consumer_repository import Consumer, ConsumerRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.query_repository import QueryObservation, QueryRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot


def seed_analysis_data(db_path: Path) -> None:
    with Database(db_path) as db:
        assets = AssetRepository(db)
        edges = EdgeRepository(db)
        queries = QueryRepository(db)
        consumers = ConsumerRepository(db)
        schemas = SchemaRepository(db)

        assets.upsert(
            Asset(
                id="postgres:demo::public.orders",
                source="postgres:demo",
                kind="table",
                name="public.orders",
                metadata={"owner": "analytics"},
            )
        )
        assets.upsert(
            Asset(
                id="postgres:demo::public.customers",
                source="postgres:demo",
                kind="table",
                name="public.customers",
            )
        )
        assets.upsert(
            Asset(
                id="postgres:demo::query::analyst",
                source="postgres:demo",
                kind="query",
                name="analyst",
            )
        )
        assets.upsert(
            Asset(
                id="snowflake:warehouse::sales.orders",
                source="snowflake:warehouse",
                kind="table",
                name="sales.orders",
            )
        )

        edges.upsert(
            Edge(
                upstream_id="postgres:demo::public.orders",
                downstream_id="postgres:demo::query::analyst",
                kind="reads",
            )
        )
        edges.upsert(
            Edge(
                upstream_id="postgres:demo::public.customers",
                downstream_id="postgres:demo::query::analyst",
                kind="reads",
            )
        )

        join_sql = (
            "SELECT public.orders.id, public.customers.email "
            "FROM public.orders "
            "JOIN public.customers ON public.orders.customer_id = public.customers.id"
        )
        queries.upsert(
            QueryObservation(
                fingerprint="fp-orders-customers",
                sql_text=join_sql,
                tables=[
                    "postgres:demo::public.orders",
                    "postgres:demo::public.customers",
                ],
                source="postgres:demo",
            )
        )
        queries.upsert(
            QueryObservation(
                fingerprint="fp-orders-customers",
                sql_text=join_sql,
                tables=[
                    "postgres:demo::public.orders",
                    "postgres:demo::public.customers",
                ],
                source="postgres:demo",
            )
        )
        queries.upsert(
            QueryObservation(
                fingerprint="fp-orders-only",
                sql_text="SELECT public.orders.id FROM public.orders WHERE public.orders.id = 42",
                tables=["postgres:demo::public.orders"],
                source="postgres:demo",
            )
        )
        queries.upsert(
            QueryObservation(
                fingerprint="fp-snowflake",
                sql_text="SELECT sales.orders.id FROM sales.orders",
                tables=["snowflake:warehouse::sales.orders"],
                source="snowflake:warehouse",
            )
        )

        consumers.upsert(
            Consumer(
                id="consumer:analyst",
                kind="user",
                name="analyst",
                source="postgres:demo",
                asset_ids=["postgres:demo::public.orders"],
            )
        )

        schemas.upsert(
            SchemaSnapshot(
                asset_id="postgres:demo::public.orders",
                columns=[
                    ColumnInfo(name="id", type="INTEGER", nullable=False),
                    ColumnInfo(name="customer_id", type="INTEGER", nullable=False),
                ],
            )
        )
        schemas.upsert(
            SchemaSnapshot(
                asset_id="postgres:demo::public.customers",
                columns=[
                    ColumnInfo(name="id", type="INTEGER", nullable=False),
                    ColumnInfo(name="email", type="TEXT", nullable=False),
                ],
            )
        )
