"""Tests for source_adapter_v2 protocol types."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from alma_connectors.source_adapter import (
    ConnectionTestResult,
    ObservedQueryEvent,
    PersistedSourceAdapter,
    QueryResult,
    SetupInstructions,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    ColumnSchema,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionMeta,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    ObjectDefinition,
    ObjectDependency,
    OrchestrationSnapshot,
    OrchestrationTask,
    OrchestrationUnit,
    SchemaObject,
    SchemaObjectKind,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    SourceAdapterV2,
    TrafficExtractionResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 3, 23, 12, 0, 0, tzinfo=UTC)

_SCOPE_CTX = ScopeContext(
    scope=ExtractionScope.GLOBAL,
    identifiers={"account": "my-acct"},
)

_META = ExtractionMeta(
    adapter_key="sf-prod",
    adapter_kind=SourceAdapterKindV2.SNOWFLAKE,
    capability=AdapterCapability.SCHEMA,
    scope_context=_SCOPE_CTX,
    captured_at=_NOW,
    duration_ms=123.4,
    row_count=5,
)


# ---------------------------------------------------------------------------
# Smoke import — all __all__ names importable
# ---------------------------------------------------------------------------


def test_smoke_imports() -> None:
    import alma_connectors.source_adapter_v2 as mod

    for name in mod.__all__:
        assert hasattr(mod, name), f"__all__ member not found: {name}"


# ---------------------------------------------------------------------------
# ScopeContext
# ---------------------------------------------------------------------------


def test_scope_context_construction() -> None:
    ctx = ScopeContext(scope=ExtractionScope.DATABASE, identifiers={"db": "prod"})
    assert ctx.scope is ExtractionScope.DATABASE
    assert ctx.identifiers == {"db": "prod"}


def test_scope_context_empty_identifiers() -> None:
    ctx = ScopeContext(scope=ExtractionScope.GLOBAL)
    assert ctx.identifiers == {}


def test_scope_context_identifiers_normalized_to_dict() -> None:
    # Passing a plain dict is fine; frozen dataclass normalises to new dict
    src = {"k": "v"}
    ctx = ScopeContext(scope=ExtractionScope.REGION, identifiers=src)
    src["extra"] = "mutated"
    assert "extra" not in ctx.identifiers


# ---------------------------------------------------------------------------
# CapabilityProbeResult
# ---------------------------------------------------------------------------


def test_capability_probe_result_minimal() -> None:
    r = CapabilityProbeResult(
        capability=AdapterCapability.SCHEMA,
        available=True,
        scope=ExtractionScope.GLOBAL,
    )
    assert r.available is True
    assert r.permissions_missing == ()
    assert r.fallback_used is False
    assert r.message is None


def test_capability_probe_result_full() -> None:
    r = CapabilityProbeResult(
        capability=AdapterCapability.TRAFFIC,
        available=False,
        scope=ExtractionScope.GLOBAL,
        fallback_used=True,
        message="  missing jobs.query  ",
        permissions_missing=("bigquery.jobs.listAll",),
    )
    assert r.message == "missing jobs.query"
    assert r.permissions_missing == ("bigquery.jobs.listAll",)


def test_capability_probe_result_empty_message_raises() -> None:
    with pytest.raises(ValueError, match="message"):
        CapabilityProbeResult(
            capability=AdapterCapability.SCHEMA,
            available=True,
            scope=ExtractionScope.GLOBAL,
            message="   ",
        )


# ---------------------------------------------------------------------------
# ExtractionMeta
# ---------------------------------------------------------------------------


def test_extraction_meta_construction() -> None:
    assert _META.adapter_key == "sf-prod"
    assert _META.adapter_kind is SourceAdapterKindV2.SNOWFLAKE
    assert _META.row_count == 5


def test_extraction_meta_strips_adapter_key() -> None:
    m = ExtractionMeta(
        adapter_key="  my-key  ",
        adapter_kind=SourceAdapterKindV2.BIGQUERY,
        capability=AdapterCapability.DISCOVER,
        scope_context=_SCOPE_CTX,
        captured_at=_NOW,
        duration_ms=0.1,
        row_count=0,
    )
    assert m.adapter_key == "my-key"


def test_extraction_meta_negative_duration_raises() -> None:
    with pytest.raises(ValueError, match="duration_ms"):
        ExtractionMeta(
            adapter_key="x",
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.SCHEMA,
            scope_context=_SCOPE_CTX,
            captured_at=_NOW,
            duration_ms=-1.0,
            row_count=0,
        )


def test_extraction_meta_negative_row_count_raises() -> None:
    with pytest.raises(ValueError, match="row_count"):
        ExtractionMeta(
            adapter_key="x",
            adapter_kind=SourceAdapterKindV2.POSTGRES,
            capability=AdapterCapability.SCHEMA,
            scope_context=_SCOPE_CTX,
            captured_at=_NOW,
            duration_ms=1.0,
            row_count=-1,
        )


# ---------------------------------------------------------------------------
# DiscoveredContainer + DiscoverySnapshot
# ---------------------------------------------------------------------------


def test_discovered_container_construction() -> None:
    c = DiscoveredContainer(
        container_id="ds-1",
        container_type="dataset",
        display_name="analytics",
        location="us",
    )
    assert c.container_id == "ds-1"
    assert c.location == "us"
    assert c.metadata == {}


def test_discovered_container_strips_strings() -> None:
    c = DiscoveredContainer(
        container_id="  ds-1  ",
        container_type="  dataset  ",
        display_name="  analytics  ",
    )
    assert c.container_id == "ds-1"
    assert c.display_name == "analytics"


def test_discovered_container_metadata_isolated() -> None:
    src = {"k": "v"}
    c = DiscoveredContainer(
        container_id="ds-1",
        container_type="dataset",
        display_name="x",
        metadata=src,
    )
    src["mutated"] = True
    assert "mutated" not in c.metadata


def test_discovery_snapshot() -> None:
    snap = DiscoverySnapshot(meta=_META, containers=())
    assert snap.containers == ()


# ---------------------------------------------------------------------------
# SchemaObjectKind (v2 extension)
# ---------------------------------------------------------------------------


def test_schema_object_kind_v2_values() -> None:
    # Core v1 kinds present
    assert SchemaObjectKind.TABLE == "table"
    assert SchemaObjectKind.VIEW == "view"
    assert SchemaObjectKind.MATERIALIZED_VIEW == "materialized_view"
    # v2 additions
    assert SchemaObjectKind.UDF == "udf"
    assert SchemaObjectKind.PROCEDURE == "procedure"
    assert SchemaObjectKind.TABLE_FUNCTION == "table_function"
    assert SchemaObjectKind.MACRO == "macro"
    assert SchemaObjectKind.ML_MODEL == "ml_model"
    assert SchemaObjectKind.SEMANTIC_MODEL == "semantic_model"
    assert SchemaObjectKind.METRIC == "metric"


# ---------------------------------------------------------------------------
# ColumnSchema
# ---------------------------------------------------------------------------


def test_column_schema_construction() -> None:
    col = ColumnSchema(name="id", data_type="INT64")
    assert col.name == "id"
    assert col.is_nullable is True
    assert col.clustering_ordinal is None


def test_column_schema_strips_strings() -> None:
    col = ColumnSchema(name="  id  ", data_type="  INT64  ")
    assert col.name == "id"
    assert col.data_type == "INT64"


def test_column_schema_negative_clustering_ordinal_raises() -> None:
    with pytest.raises(ValueError, match="clustering_ordinal"):
        ColumnSchema(name="x", data_type="STRING", clustering_ordinal=-1)


# ---------------------------------------------------------------------------
# SchemaObject
# ---------------------------------------------------------------------------


def test_schema_object_table() -> None:
    obj = SchemaObject(
        schema_name="public",
        object_name="orders",
        kind=SchemaObjectKind.TABLE,
        row_count=1000,
        size_bytes=2048,
    )
    assert obj.schema_name == "public"
    assert obj.row_count == 1000


def test_schema_object_udf() -> None:
    obj = SchemaObject(
        schema_name="utils",
        object_name="format_date",
        kind=SchemaObjectKind.UDF,
        language="sql",
        return_type="STRING",
    )
    assert obj.kind is SchemaObjectKind.UDF
    assert obj.language == "sql"


def test_schema_object_negative_row_count_raises() -> None:
    with pytest.raises(ValueError, match="row_count"):
        SchemaObject(
            schema_name="s",
            object_name="t",
            kind=SchemaObjectKind.TABLE,
            row_count=-1,
        )


def test_schema_object_metadata_isolated() -> None:
    src: dict[str, object] = {"tag": "prod"}
    obj = SchemaObject(
        schema_name="s",
        object_name="t",
        kind=SchemaObjectKind.VIEW,
        metadata=src,
    )
    src["mutated"] = True
    assert "mutated" not in obj.metadata


# ---------------------------------------------------------------------------
# SchemaSnapshotV2
# ---------------------------------------------------------------------------


def test_schema_snapshot_v2() -> None:
    snap = SchemaSnapshotV2(meta=_META, objects=(), dependencies=())
    assert snap.objects == ()


# ---------------------------------------------------------------------------
# ObjectDefinition + DefinitionSnapshot
# ---------------------------------------------------------------------------


def test_object_definition_construction() -> None:
    d = ObjectDefinition(
        schema_name="public",
        object_name="orders",
        object_kind=SchemaObjectKind.VIEW,
        definition_text="SELECT 1",
        definition_language="sql",
    )
    assert d.definition_text == "SELECT 1"
    assert d.metadata == {}


def test_object_definition_strips_strings() -> None:
    d = ObjectDefinition(
        schema_name="  s  ",
        object_name="  t  ",
        object_kind=SchemaObjectKind.TABLE,
        definition_text="  CREATE TABLE t()  ",
        definition_language="  sql  ",
    )
    assert d.schema_name == "s"
    assert d.definition_language == "sql"


def test_definition_snapshot() -> None:
    snap = DefinitionSnapshot(meta=_META, definitions=())
    assert snap.definitions == ()


# ---------------------------------------------------------------------------
# TrafficExtractionResult
# ---------------------------------------------------------------------------


def test_traffic_extraction_result() -> None:
    event = ObservedQueryEvent(
        captured_at=_NOW,
        sql="SELECT 1",
        source_name="pg-prod",
        query_type="SELECT",
    )
    result = TrafficExtractionResult(meta=_META, events=(event,))
    assert len(result.events) == 1
    assert result.observation_cursor is None


def test_traffic_extraction_result_cursor_isolated() -> None:
    cursor: dict[str, object] = {"offset": 100}
    result = TrafficExtractionResult(
        meta=_META,
        events=(),
        observation_cursor=cursor,
    )
    cursor["mutated"] = True
    assert "mutated" not in result.observation_cursor  # type: ignore[operator]


# ---------------------------------------------------------------------------
# LineageEdge + LineageSnapshot
# ---------------------------------------------------------------------------


def test_lineage_edge_construction() -> None:
    edge = LineageEdge(
        source_object="public.orders",
        target_object="analytics.order_stats",
        edge_kind=LineageEdgeKind.DECLARED,
        confidence=0.95,
    )
    assert edge.confidence == 0.95
    assert edge.column_mappings == ()


def test_lineage_edge_confidence_out_of_range_raises() -> None:
    with pytest.raises(ValueError, match="confidence"):
        LineageEdge(
            source_object="a.b",
            target_object="c.d",
            edge_kind=LineageEdgeKind.HEURISTIC,
            confidence=1.1,
        )


def test_lineage_edge_kinds_present() -> None:
    assert LineageEdgeKind.DECLARED == "declared"
    assert LineageEdgeKind.INFERRED_SQL == "inferred_sql"
    assert LineageEdgeKind.INFERRED_TRAFFIC == "inferred_traffic"
    assert LineageEdgeKind.CONNECTOR_API == "connector_api"
    assert LineageEdgeKind.HEURISTIC == "heuristic"


def test_lineage_snapshot() -> None:
    snap = LineageSnapshot(meta=_META, edges=())
    assert snap.edges == ()


# ---------------------------------------------------------------------------
# OrchestrationTask + OrchestrationUnit + OrchestrationSnapshot
# ---------------------------------------------------------------------------


def test_orchestration_task_construction() -> None:
    task = OrchestrationTask(
        task_id="load_orders",
        task_type="sql",
        upstream_task_ids=("extract_raw",),
    )
    assert task.task_id == "load_orders"
    assert task.upstream_task_ids == ("extract_raw",)


def test_orchestration_unit_construction() -> None:
    task = OrchestrationTask(task_id="t1", task_type="python")
    unit = OrchestrationUnit(
        unit_id="dag-etl",
        unit_type="dag",
        display_name="ETL Pipeline",
        schedule="0 0 * * *",
        tasks=(task,),
    )
    assert unit.schedule == "0 0 * * *"
    assert len(unit.tasks) == 1


def test_orchestration_unit_strips_strings() -> None:
    unit = OrchestrationUnit(
        unit_id="  dag-1  ",
        unit_type="  dag  ",
        display_name="  My DAG  ",
    )
    assert unit.unit_id == "dag-1"
    assert unit.display_name == "My DAG"


def test_orchestration_snapshot() -> None:
    snap = OrchestrationSnapshot(meta=_META, units=())
    assert snap.units == ()


# ---------------------------------------------------------------------------
# SourceAdapterKindV2 — includes all v1 kinds and v2 additions
# ---------------------------------------------------------------------------


def test_source_adapter_kind_v2_values() -> None:
    assert SourceAdapterKindV2.POSTGRES == "postgres"
    assert SourceAdapterKindV2.BIGQUERY == "bigquery"
    assert SourceAdapterKindV2.DBT == "dbt"
    assert SourceAdapterKindV2.SNOWFLAKE == "snowflake"
    assert SourceAdapterKindV2.AIRFLOW == "airflow"
    assert SourceAdapterKindV2.LOOKER == "looker"
    assert SourceAdapterKindV2.FIVETRAN == "fivetran"
    assert SourceAdapterKindV2.METABASE == "metabase"


# ---------------------------------------------------------------------------
# Protocol conformance (mock adapter)
# ---------------------------------------------------------------------------


class _MockAdapter:
    """Minimal SourceAdapterV2 conformant implementation for tests."""

    @property
    def declared_capabilities(self) -> frozenset[AdapterCapability]:
        return frozenset({AdapterCapability.DISCOVER, AdapterCapability.SCHEMA})

    async def test_connection(
        self, adapter: PersistedSourceAdapter
    ) -> ConnectionTestResult:
        return ConnectionTestResult(success=True, message="ok")

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        caps = capabilities or self.declared_capabilities
        return tuple(
            CapabilityProbeResult(
                capability=c,
                available=True,
                scope=ExtractionScope.GLOBAL,
            )
            for c in caps
        )

    async def discover(self, adapter: PersistedSourceAdapter) -> DiscoverySnapshot:
        return DiscoverySnapshot(meta=_META, containers=())

    async def extract_schema(self, adapter: PersistedSourceAdapter) -> SchemaSnapshotV2:
        return SchemaSnapshotV2(meta=_META, objects=())

    async def extract_definitions(
        self, adapter: PersistedSourceAdapter
    ) -> DefinitionSnapshot:
        return DefinitionSnapshot(meta=_META, definitions=())

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        return TrafficExtractionResult(meta=_META, events=())

    async def extract_lineage(
        self, adapter: PersistedSourceAdapter
    ) -> LineageSnapshot:
        return LineageSnapshot(meta=_META, edges=())

    async def extract_orchestration(
        self, adapter: PersistedSourceAdapter
    ) -> OrchestrationSnapshot:
        return OrchestrationSnapshot(meta=_META, units=())

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
        dry_run: bool = False,
    ) -> QueryResult:
        return QueryResult(success=True, row_count=0, duration_ms=0.0)

    def get_setup_instructions(self) -> SetupInstructions:
        return SetupInstructions(title="Mock", summary="Mock adapter", steps=("Nothing to do",))


def test_protocol_isinstance() -> None:
    adapter = _MockAdapter()
    assert isinstance(adapter, SourceAdapterV2)


def test_protocol_declared_capabilities() -> None:
    adapter = _MockAdapter()
    caps = adapter.declared_capabilities
    assert AdapterCapability.DISCOVER in caps
    assert AdapterCapability.SCHEMA in caps
    assert AdapterCapability.TRAFFIC not in caps


# ---------------------------------------------------------------------------
# Normalisation edge-cases
# ---------------------------------------------------------------------------


def test_object_dependency_all_fields_stripped() -> None:
    dep = ObjectDependency(
        source_schema="  s1  ",
        source_object="  t1  ",
        target_schema="  s2  ",
        target_object="  t2  ",
    )
    assert dep.source_schema == "s1"
    assert dep.target_object == "t2"


def test_schema_object_name_empty_raises() -> None:
    with pytest.raises(ValueError, match="object_name"):
        SchemaObject(schema_name="s", object_name="  ", kind=SchemaObjectKind.TABLE)


def test_lineage_edge_confidence_zero_ok() -> None:
    edge = LineageEdge(
        source_object="a.b",
        target_object="c.d",
        edge_kind=LineageEdgeKind.HEURISTIC,
        confidence=0.0,
    )
    assert edge.confidence == 0.0


def test_lineage_edge_confidence_one_ok() -> None:
    edge = LineageEdge(
        source_object="a.b",
        target_object="c.d",
        edge_kind=LineageEdgeKind.DECLARED,
        confidence=1.0,
    )
    assert edge.confidence == 1.0
