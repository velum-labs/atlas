#!/usr/bin/env python
"""Generate the bundled Atlas sample-data SQLite snapshot.

Run from repo root:
    uv run python scripts/generate_sample_data.py

Writes to `packages/alma-atlas/src/alma_atlas/data/atlas-sample.db.gz`,
overwriting the existing file. Refresh on major releases per design doc spec.

The snapshot is a fictitious mid-stage analytics company: 5 raw + warehouse
assets in Snowflake, 5 dbt models, 5 Looker explores/dashboards. Cross-system
lineage chains demonstrate the cross-stack value Atlas surfaces. All asset
metadata, schemas, edges, and annotations land in the same shape Atlas
produces from a real scan.
"""

from __future__ import annotations

import gzip
import shutil
import tempfile
from pathlib import Path

from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import EdgeRepository
from alma_atlas_store.schema_repository import SchemaRepository
from alma_ports.annotation import AnnotationRecord
from alma_ports.asset import Asset
from alma_ports.edge import GraphEdge
from alma_ports.schema import ColumnInfo, SchemaSnapshot

# Output path inside the alma-atlas wheel
OUTPUT_PATH = (
    Path(__file__).resolve().parent.parent
    / "packages"
    / "alma-atlas"
    / "src"
    / "alma_atlas"
    / "data"
    / "atlas-sample.db.gz"
)

SNOWFLAKE_SOURCE = "snowflake:demo"
DBT_SOURCE = "dbt:demo"
LOOKER_SOURCE = "looker:demo"


# ---------------------------------------------------------------------------
# Asset definitions
# ---------------------------------------------------------------------------


def _snowflake_assets() -> list[Asset]:
    return [
        Asset(
            id=f"{SNOWFLAKE_SOURCE}::raw.events",
            source=SNOWFLAKE_SOURCE,
            kind="table",
            name="raw.events",
            description="Raw event stream from product clients (web + mobile).",
        ),
        Asset(
            id=f"{SNOWFLAKE_SOURCE}::analytics.users",
            source=SNOWFLAKE_SOURCE,
            kind="table",
            name="analytics.users",
            description="Dim table of all users (active + churned). One row per user.",
        ),
        Asset(
            id=f"{SNOWFLAKE_SOURCE}::analytics.orders",
            source=SNOWFLAKE_SOURCE,
            kind="table",
            name="analytics.orders",
            description="Fact table of placed orders. One row per order.",
        ),
        Asset(
            id=f"{SNOWFLAKE_SOURCE}::analytics.payments",
            source=SNOWFLAKE_SOURCE,
            kind="table",
            name="analytics.payments",
            description="Fact table of payment events (charge, refund, chargeback).",
        ),
        Asset(
            id=f"{SNOWFLAKE_SOURCE}::analytics.sessions",
            source=SNOWFLAKE_SOURCE,
            kind="table",
            name="analytics.sessions",
            description="Sessionized rollup of raw.events. Derived nightly.",
        ),
    ]


def _dbt_assets() -> list[Asset]:
    return [
        Asset(
            id=f"{DBT_SOURCE}::staging.stg_users",
            source=DBT_SOURCE,
            kind="model",
            name="staging.stg_users",
            description="Cleaned users staging model. Casts types, drops PII.",
        ),
        Asset(
            id=f"{DBT_SOURCE}::staging.stg_orders",
            source=DBT_SOURCE,
            kind="model",
            name="staging.stg_orders",
            description="Cleaned orders staging model. Filters out test orders.",
        ),
        Asset(
            id=f"{DBT_SOURCE}::marts.dim_users",
            source=DBT_SOURCE,
            kind="model",
            name="marts.dim_users",
            description="User dimension. Joins stg_users with cohort + segment data.",
        ),
        Asset(
            id=f"{DBT_SOURCE}::marts.fct_revenue",
            source=DBT_SOURCE,
            kind="model",
            name="marts.fct_revenue",
            description="Revenue fact. Aggregates payments by day x product x cohort.",
        ),
        Asset(
            id=f"{DBT_SOURCE}::marts.fct_user_ltv",
            source=DBT_SOURCE,
            kind="model",
            name="marts.fct_user_ltv",
            description="User lifetime value. Joins dim_users with fct_revenue.",
        ),
    ]


def _looker_assets() -> list[Asset]:
    return [
        Asset(
            id=f"{LOOKER_SOURCE}::explore.revenue",
            source=LOOKER_SOURCE,
            kind="explore",
            name="explore.revenue",
            description="Revenue explore. Built on marts.fct_revenue.",
        ),
        Asset(
            id=f"{LOOKER_SOURCE}::explore.user_funnel",
            source=LOOKER_SOURCE,
            kind="explore",
            name="explore.user_funnel",
            description="User funnel explore. Built on marts.fct_user_ltv + sessions.",
        ),
        Asset(
            id=f"{LOOKER_SOURCE}::dashboard.exec_summary",
            source=LOOKER_SOURCE,
            kind="dashboard",
            name="dashboard.exec_summary",
            description="Executive summary dashboard. Updated daily at 8am.",
        ),
        Asset(
            id=f"{LOOKER_SOURCE}::dashboard.product_kpis",
            source=LOOKER_SOURCE,
            kind="dashboard",
            name="dashboard.product_kpis",
            description="Product KPIs (DAU, retention, conversion).",
        ),
        Asset(
            id=f"{LOOKER_SOURCE}::dashboard.payment_health",
            source=LOOKER_SOURCE,
            kind="dashboard",
            name="dashboard.payment_health",
            description="Payment health dashboard. Used by finance ops.",
        ),
    ]


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------


SCHEMAS: dict[str, list[ColumnInfo]] = {
    f"{SNOWFLAKE_SOURCE}::raw.events": [
        ColumnInfo("event_id", "STRING", nullable=False, description="Unique event id"),
        ColumnInfo("user_id", "STRING", nullable=True, description="User who triggered the event"),
        ColumnInfo("session_id", "STRING", nullable=True),
        ColumnInfo("event_type", "STRING", nullable=False, description="page_view, click, purchase, ..."),
        ColumnInfo("event_at", "TIMESTAMP", nullable=False),
        ColumnInfo("properties", "VARIANT", nullable=True, description="Event properties JSON"),
    ],
    f"{SNOWFLAKE_SOURCE}::analytics.users": [
        ColumnInfo("user_id", "STRING", nullable=False, description="Surrogate primary key"),
        ColumnInfo("email", "STRING", nullable=True, description="Hashed email (PII)"),
        ColumnInfo("created_at", "TIMESTAMP", nullable=False),
        ColumnInfo("country", "STRING", nullable=True),
        ColumnInfo("plan", "STRING", nullable=False, description="free, pro, enterprise"),
        ColumnInfo("status", "STRING", nullable=False, description="active, churned, deleted"),
    ],
    f"{SNOWFLAKE_SOURCE}::analytics.orders": [
        ColumnInfo("order_id", "STRING", nullable=False, description="Primary key"),
        ColumnInfo("user_id", "STRING", nullable=False, description="FK to analytics.users"),
        ColumnInfo("order_total", "NUMERIC(12,2)", nullable=False),
        ColumnInfo("currency", "STRING", nullable=False, description="ISO 4217 (USD, EUR, ...)"),
        ColumnInfo("status", "STRING", nullable=False, description="pending, paid, cancelled, refunded"),
        ColumnInfo("placed_at", "TIMESTAMP", nullable=False),
        ColumnInfo("paid_at", "TIMESTAMP", nullable=True),
    ],
    f"{SNOWFLAKE_SOURCE}::analytics.payments": [
        ColumnInfo("payment_id", "STRING", nullable=False),
        ColumnInfo("order_id", "STRING", nullable=False, description="FK to analytics.orders"),
        ColumnInfo("amount", "NUMERIC(12,2)", nullable=False),
        ColumnInfo("currency", "STRING", nullable=False),
        ColumnInfo("kind", "STRING", nullable=False, description="charge, refund, chargeback"),
        ColumnInfo("processor", "STRING", nullable=False, description="stripe, paypal, ..."),
        ColumnInfo("processed_at", "TIMESTAMP", nullable=False),
    ],
    f"{SNOWFLAKE_SOURCE}::analytics.sessions": [
        ColumnInfo("session_id", "STRING", nullable=False),
        ColumnInfo("user_id", "STRING", nullable=True),
        ColumnInfo("started_at", "TIMESTAMP", nullable=False),
        ColumnInfo("ended_at", "TIMESTAMP", nullable=True),
        ColumnInfo("event_count", "INTEGER", nullable=False),
        ColumnInfo("source", "STRING", nullable=True, description="organic, paid, referral, direct"),
    ],
    f"{DBT_SOURCE}::staging.stg_users": [
        ColumnInfo("user_id", "STRING", nullable=False),
        ColumnInfo("created_at", "TIMESTAMP", nullable=False),
        ColumnInfo("country", "STRING", nullable=True),
        ColumnInfo("plan", "STRING", nullable=False),
        ColumnInfo("is_active", "BOOLEAN", nullable=False, description="status = 'active'"),
    ],
    f"{DBT_SOURCE}::staging.stg_orders": [
        ColumnInfo("order_id", "STRING", nullable=False),
        ColumnInfo("user_id", "STRING", nullable=False),
        ColumnInfo("order_total_usd", "NUMERIC(12,2)", nullable=False, description="Normalized to USD"),
        ColumnInfo("status", "STRING", nullable=False),
        ColumnInfo("placed_at", "TIMESTAMP", nullable=False),
    ],
    f"{DBT_SOURCE}::marts.dim_users": [
        ColumnInfo("user_id", "STRING", nullable=False),
        ColumnInfo("created_at", "TIMESTAMP", nullable=False),
        ColumnInfo("plan", "STRING", nullable=False),
        ColumnInfo("country", "STRING", nullable=True),
        ColumnInfo("cohort_month", "STRING", nullable=True),
        ColumnInfo("segment", "STRING", nullable=True, description="enterprise, smb, prosumer, churned"),
    ],
    f"{DBT_SOURCE}::marts.fct_revenue": [
        ColumnInfo("revenue_date", "DATE", nullable=False),
        ColumnInfo("product", "STRING", nullable=False),
        ColumnInfo("cohort_month", "STRING", nullable=True),
        ColumnInfo("gross_revenue_usd", "NUMERIC(14,2)", nullable=False),
        ColumnInfo("refunds_usd", "NUMERIC(14,2)", nullable=False),
        ColumnInfo("net_revenue_usd", "NUMERIC(14,2)", nullable=False, description="gross - refunds"),
    ],
    f"{DBT_SOURCE}::marts.fct_user_ltv": [
        ColumnInfo("user_id", "STRING", nullable=False),
        ColumnInfo("first_payment_at", "TIMESTAMP", nullable=True),
        ColumnInfo("lifetime_orders", "INTEGER", nullable=False),
        ColumnInfo("lifetime_revenue_usd", "NUMERIC(14,2)", nullable=False),
        ColumnInfo("predicted_ltv_usd", "NUMERIC(14,2)", nullable=True, description="ML output, refreshed weekly"),
    ],
}


# ---------------------------------------------------------------------------
# Edges (cross-system lineage)
# ---------------------------------------------------------------------------


def _edges() -> list[GraphEdge]:
    return [
        # Snowflake-internal: raw -> sessions
        GraphEdge(
            upstream_id=f"{SNOWFLAKE_SOURCE}::raw.events",
            downstream_id=f"{SNOWFLAKE_SOURCE}::analytics.sessions",
            kind="reads",
            metadata={"join_guidance": "events grouped by user_id + session_id, 30-min idle threshold"},
        ),
        # Snowflake -> dbt
        GraphEdge(
            upstream_id=f"{SNOWFLAKE_SOURCE}::analytics.users",
            downstream_id=f"{DBT_SOURCE}::staging.stg_users",
            kind="reads",
        ),
        GraphEdge(
            upstream_id=f"{SNOWFLAKE_SOURCE}::analytics.orders",
            downstream_id=f"{DBT_SOURCE}::staging.stg_orders",
            kind="reads",
        ),
        GraphEdge(
            upstream_id=f"{SNOWFLAKE_SOURCE}::analytics.payments",
            downstream_id=f"{DBT_SOURCE}::marts.fct_revenue",
            kind="reads",
            metadata={"join_guidance": "join on order_id; sum amount where kind='charge', subtract where kind='refund'"},
        ),
        # dbt staging -> dbt marts
        GraphEdge(
            upstream_id=f"{DBT_SOURCE}::staging.stg_users",
            downstream_id=f"{DBT_SOURCE}::marts.dim_users",
            kind="reads",
        ),
        GraphEdge(
            upstream_id=f"{DBT_SOURCE}::staging.stg_orders",
            downstream_id=f"{DBT_SOURCE}::marts.fct_revenue",
            kind="reads",
        ),
        GraphEdge(
            upstream_id=f"{DBT_SOURCE}::marts.dim_users",
            downstream_id=f"{DBT_SOURCE}::marts.fct_user_ltv",
            kind="reads",
        ),
        GraphEdge(
            upstream_id=f"{DBT_SOURCE}::marts.fct_revenue",
            downstream_id=f"{DBT_SOURCE}::marts.fct_user_ltv",
            kind="reads",
            metadata={"join_guidance": "join on user_id; aggregate net_revenue_usd over time"},
        ),
        # dbt marts -> Looker explores
        GraphEdge(
            upstream_id=f"{DBT_SOURCE}::marts.fct_revenue",
            downstream_id=f"{LOOKER_SOURCE}::explore.revenue",
            kind="exposes",
        ),
        GraphEdge(
            upstream_id=f"{DBT_SOURCE}::marts.fct_user_ltv",
            downstream_id=f"{LOOKER_SOURCE}::explore.user_funnel",
            kind="exposes",
        ),
        GraphEdge(
            upstream_id=f"{SNOWFLAKE_SOURCE}::analytics.sessions",
            downstream_id=f"{LOOKER_SOURCE}::explore.user_funnel",
            kind="exposes",
        ),
        # Looker explores -> dashboards
        GraphEdge(
            upstream_id=f"{LOOKER_SOURCE}::explore.revenue",
            downstream_id=f"{LOOKER_SOURCE}::dashboard.exec_summary",
            kind="renders",
        ),
        GraphEdge(
            upstream_id=f"{LOOKER_SOURCE}::explore.revenue",
            downstream_id=f"{LOOKER_SOURCE}::dashboard.payment_health",
            kind="renders",
        ),
        GraphEdge(
            upstream_id=f"{LOOKER_SOURCE}::explore.user_funnel",
            downstream_id=f"{LOOKER_SOURCE}::dashboard.product_kpis",
            kind="renders",
        ),
    ]


# ---------------------------------------------------------------------------
# Annotations (owners + business context)
# ---------------------------------------------------------------------------


_DATA_ENG = "data-eng@example.com"
_DATA_PLATFORM = "data-platform@example.com"
_PRODUCT = "product-analytics@example.com"
_FINANCE = "finance-ops@example.com"


ANNOTATIONS: dict[str, AnnotationRecord] = {
    f"{SNOWFLAKE_SOURCE}::raw.events": AnnotationRecord(
        asset_id=f"{SNOWFLAKE_SOURCE}::raw.events",
        ownership=_DATA_PLATFORM,
        granularity="one row per event",
        freshness_guarantee="streaming, < 5 minute lag",
        sensitivity="contains hashed user identifiers",
    ),
    f"{SNOWFLAKE_SOURCE}::analytics.users": AnnotationRecord(
        asset_id=f"{SNOWFLAKE_SOURCE}::analytics.users",
        ownership=_DATA_ENG,
        granularity="one row per user",
        join_keys=["user_id"],
        freshness_guarantee="hourly batch",
        sensitivity="PII (hashed email, country)",
        business_logic_summary="Master users dim. Soft-deletes are status='deleted', hard deletes are removed entirely.",
    ),
    f"{SNOWFLAKE_SOURCE}::analytics.orders": AnnotationRecord(
        asset_id=f"{SNOWFLAKE_SOURCE}::analytics.orders",
        ownership=_DATA_ENG,
        granularity="one row per order",
        join_keys=["order_id", "user_id"],
        freshness_guarantee="hourly batch",
        business_logic_summary="Includes test orders (status='cancelled' with metadata.test=true). Filter out for revenue queries.",
    ),
    f"{SNOWFLAKE_SOURCE}::analytics.payments": AnnotationRecord(
        asset_id=f"{SNOWFLAKE_SOURCE}::analytics.payments",
        ownership=_FINANCE,
        granularity="one row per payment event",
        join_keys=["order_id"],
        freshness_guarantee="real-time via webhooks",
        business_logic_summary="Refunds and chargebacks are negative amounts. Always filter or aggregate.",
    ),
    f"{DBT_SOURCE}::marts.fct_revenue": AnnotationRecord(
        asset_id=f"{DBT_SOURCE}::marts.fct_revenue",
        ownership=_FINANCE,
        granularity="one row per (date, product, cohort)",
        freshness_guarantee="daily at 6am UTC",
        business_logic_summary="Authoritative revenue source. Use net_revenue_usd for board reporting.",
    ),
    f"{DBT_SOURCE}::marts.fct_user_ltv": AnnotationRecord(
        asset_id=f"{DBT_SOURCE}::marts.fct_user_ltv",
        ownership=_PRODUCT,
        granularity="one row per user",
        freshness_guarantee="weekly (predicted_ltv_usd refreshed by ML pipeline)",
        business_logic_summary="LTV is a prediction; lifetime_revenue_usd is observed. Don't conflate them.",
    ),
    f"{LOOKER_SOURCE}::dashboard.exec_summary": AnnotationRecord(
        asset_id=f"{LOOKER_SOURCE}::dashboard.exec_summary",
        ownership="exec-staff@example.com",
        freshness_guarantee="auto-refreshes daily at 8am",
        business_logic_summary="Looked at by exec team in the Monday standup. Don't change without notice.",
    ),
}


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------


def generate(output_path: Path = OUTPUT_PATH) -> None:
    """Build the sample database and write it gzipped to output_path."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_db = Path(tmpdir) / "atlas-sample.db"
        with Database(tmp_db) as db:
            asset_repo = AssetRepository(db)
            schema_repo = SchemaRepository(db)
            edge_repo = EdgeRepository(db)
            annotation_repo = AnnotationRepository(db)

            assets = _snowflake_assets() + _dbt_assets() + _looker_assets()
            for asset in assets:
                asset_repo.upsert(asset)

            for asset_id, columns in SCHEMAS.items():
                schema_repo.upsert(SchemaSnapshot(asset_id=asset_id, columns=columns))

            for edge in _edges():
                edge_repo.upsert(edge)

            for annotation in ANNOTATIONS.values():
                annotation_repo.upsert(annotation)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(tmp_db, "rb") as src, gzip.open(output_path, "wb") as dst:
            shutil.copyfileobj(src, dst)

        size_kb = output_path.stat().st_size / 1024
        print(f"Wrote sample snapshot: {output_path}")
        print(f"  Assets: {len(assets)}")
        print(f"  Schemas: {len(SCHEMAS)}")
        print(f"  Edges: {len(_edges())}")
        print(f"  Annotations: {len(ANNOTATIONS)}")
        print(f"  Compressed size: {size_kb:.1f} KB")


if __name__ == "__main__":
    generate()
