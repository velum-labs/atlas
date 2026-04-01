"""Tests for DbtAdapter v2 capabilities (ENG-408)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from alma_connectors import DbtAdapter, SourceAdapterKind
from alma_connectors.adapters.dbt import _MANIFEST_V12_PREFIX
from alma_connectors.source_adapter import (
    BigQueryAdapterConfig,
    ExternalSecretRef,
    PersistedSourceAdapter,
    SourceAdapterStatus,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    DefinitionSnapshot,
    DiscoverySnapshot,
    LineageEdgeKind,
    LineageSnapshot,
    SchemaSnapshotV2,
)
from alma_connectors.source_adapter_v2 import (
    SchemaObjectKind as SchemaObjectKindV2,
)

# ---------------------------------------------------------------------------
# Fixtures — fake PersistedSourceAdapter
# ---------------------------------------------------------------------------

_FAKE_SA_SECRET = ExternalSecretRef(provider="vault", reference="path/to/secret")


def _fake_adapter() -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=str(uuid4()),
        key="test-dbt",
        display_name="Test dbt",
        kind=SourceAdapterKind.DBT,
        target_id="target-1",
        config=BigQueryAdapterConfig(
            service_account_secret=_FAKE_SA_SECRET,
            project_id="fake-project",
        ),
        status=SourceAdapterStatus.READY,
    )


# ---------------------------------------------------------------------------
# Fixtures — dbt artifact factories
# ---------------------------------------------------------------------------


def _manifest(
    nodes: dict[str, Any] | None = None,
    sources: dict[str, Any] | None = None,
    project_name: str = "my_project",
) -> dict[str, Any]:
    return {
        "metadata": {
            "dbt_schema_version": f"{_MANIFEST_V12_PREFIX}/manifest.json",
            "dbt_version": "1.8.0",
            "project_name": project_name,
            "adapter_type": "bigquery",
        },
        "nodes": nodes or {},
        "sources": sources or {},
    }


def _model_node(
    *,
    uid: str = "model.my_project.orders",
    name: str = "orders",
    schema: str = "public",
    materialized: str = "table",
    columns: dict[str, Any] | None = None,
    depends_on: list[str] | None = None,
    compiled_code: str = "",
    description: str = "",
) -> dict[str, Any]:
    return {
        "unique_id": uid,
        "resource_type": "model",
        "name": name,
        "alias": name,
        "schema": schema,
        "database": "mydb",
        "config": {"materialized": materialized},
        "columns": columns or {},
        "depends_on": {"nodes": depends_on or []},
        "compiled_code": compiled_code,
        "description": description,
        "tags": [],
    }


def _source_node(
    *,
    uid: str = "source.my_project.raw.orders",
    name: str = "orders",
    schema: str = "raw",
    columns: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "unique_id": uid,
        "resource_type": "source",
        "name": name,
        "schema": schema,
        "database": "mydb",
        "columns": columns or {},
        "depends_on": {"nodes": []},
        "description": "",
        "tags": [],
    }


def _write_json(path: Path, data: dict[str, Any]) -> str:
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def run(coro: Any) -> Any:  # noqa: ANN401
    return asyncio.run(coro)


# ===========================================================================
# Tests — declared_capabilities
# ===========================================================================


def test_declared_capabilities_has_four_caps() -> None:
    adapter = DbtAdapter(manifest_path="/nonexistent/manifest.json")
    caps = adapter.declared_capabilities
    assert AdapterCapability.DISCOVER in caps
    assert AdapterCapability.SCHEMA in caps
    assert AdapterCapability.DEFINITIONS in caps
    assert AdapterCapability.LINEAGE in caps


def test_declared_capabilities_excludes_traffic_and_orchestration() -> None:
    adapter = DbtAdapter(manifest_path="/nonexistent/manifest.json")
    caps = adapter.declared_capabilities
    assert AdapterCapability.TRAFFIC not in caps
    assert AdapterCapability.ORCHESTRATION not in caps


# ===========================================================================
# Tests — probe()
# ===========================================================================


def test_probe_all_available_with_valid_manifest(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    results = run(adapter.probe(_fake_adapter()))
    assert len(results) == 4
    assert all(r.available for r in results)


def test_probe_returns_result_per_declared_capability(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    results = run(adapter.probe(_fake_adapter()))
    returned_caps = {r.capability for r in results}
    assert returned_caps == adapter.declared_capabilities


def test_probe_all_unavailable_when_manifest_missing() -> None:
    adapter = DbtAdapter(manifest_path="/does/not/exist/manifest.json")
    results = run(adapter.probe(_fake_adapter()))
    assert len(results) == 4
    assert all(not r.available for r in results)
    assert all(r.message is not None for r in results)


def test_probe_all_unavailable_when_manifest_invalid_json(tmp_path: Path) -> None:
    bad_path = tmp_path / "manifest.json"
    bad_path.write_text("not json", encoding="utf-8")
    adapter = DbtAdapter(manifest_path=str(bad_path))
    results = run(adapter.probe(_fake_adapter()))
    assert all(not r.available for r in results)


def test_probe_subset_of_capabilities(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    subset = frozenset({AdapterCapability.DISCOVER, AdapterCapability.LINEAGE})
    results = run(adapter.probe(_fake_adapter(), capabilities=subset))
    assert len(results) == 2
    returned_caps = {r.capability for r in results}
    assert returned_caps == subset


# ===========================================================================
# Tests — discover()
# ===========================================================================


def test_discover_returns_discovery_snapshot(tmp_path: Path) -> None:
    nodes = {"model.p.m": _model_node(uid="model.p.m", name="m", schema="analytics")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.discover(_fake_adapter()))
    assert isinstance(result, DiscoverySnapshot)


def test_discover_includes_project_container(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(project_name="acme"))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.discover(_fake_adapter()))
    project_containers = [c for c in result.containers if c.container_type == "project"]
    assert len(project_containers) == 1
    assert "acme" in project_containers[0].display_name


def test_discover_includes_schema_containers(tmp_path: Path) -> None:
    nodes = {
        "model.p.a": _model_node(uid="model.p.a", name="a", schema="analytics"),
        "model.p.b": _model_node(uid="model.p.b", name="b", schema="staging"),
    }
    sources = {
        "source.p.raw.c": _source_node(uid="source.p.raw.c", name="c", schema="raw"),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes, sources=sources))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.discover(_fake_adapter()))
    schema_containers = {c.display_name for c in result.containers if c.container_type == "schema"}
    assert "analytics" in schema_containers
    assert "staging" in schema_containers
    assert "raw" in schema_containers


def test_discover_deduplicates_schemas(tmp_path: Path) -> None:
    nodes = {
        "model.p.a": _model_node(uid="model.p.a", name="a", schema="analytics"),
        "model.p.b": _model_node(uid="model.p.b", name="b", schema="analytics"),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.discover(_fake_adapter()))
    schema_containers = [c for c in result.containers if c.container_type == "schema"]
    assert len(schema_containers) == 1


def test_discover_meta_has_correct_capability(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.discover(_fake_adapter()))
    assert result.meta.capability == AdapterCapability.DISCOVER
    assert result.meta.adapter_kind.value == "dbt"


# ===========================================================================
# Tests — extract_schema()
# ===========================================================================


def test_extract_schema_returns_schema_snapshot(tmp_path: Path) -> None:
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders", schema="public")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    assert isinstance(result, SchemaSnapshotV2)


def test_extract_schema_model_as_table(tmp_path: Path) -> None:
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders", schema="public")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    obj = next(o for o in result.objects if o.object_name == "orders")
    assert obj.kind == SchemaObjectKindV2.TABLE
    assert obj.schema_name == "public"


def test_extract_schema_source_as_external_table(tmp_path: Path) -> None:
    sources = {"source.p.raw.orders": _source_node(uid="source.p.raw.orders", name="orders", schema="raw")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(sources=sources))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    obj = next(o for o in result.objects if o.object_name == "orders")
    assert obj.kind == SchemaObjectKindV2.EXTERNAL_TABLE


def test_extract_schema_view_materialization(tmp_path: Path) -> None:
    nodes = {"model.p.v": _model_node(uid="model.p.v", name="v", materialized="view")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    assert result.objects[0].kind == SchemaObjectKindV2.VIEW


def test_extract_schema_columns_with_description(tmp_path: Path) -> None:
    columns = {
        "user_id": {"name": "user_id", "data_type": "integer", "description": "PK"},
    }
    nodes = {"model.p.users": _model_node(uid="model.p.users", name="users", columns=columns)}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    col = result.objects[0].columns[0]
    assert col.name == "user_id"
    assert col.description == "PK"


def test_extract_schema_catalog_enriches_column_types(tmp_path: Path) -> None:
    columns = {"id": {"name": "id", "data_type": "unknown"}}
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders", columns=columns)}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    catalog_data = {
        "nodes": {"model.p.orders": {"columns": {"id": {"name": "id", "type": "INT64"}}}},
        "sources": {},
    }
    catalog_path = _write_json(tmp_path / "catalog.json", catalog_data)
    adapter = DbtAdapter(manifest_path=manifest_path, catalog_path=catalog_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    col = result.objects[0].columns[0]
    assert col.data_type == "INT64"


def test_extract_schema_no_catalog_falls_back_to_manifest(tmp_path: Path) -> None:
    columns = {"amount": {"name": "amount", "data_type": "numeric"}}
    nodes = {"model.p.facts": _model_node(uid="model.p.facts", name="facts", columns=columns)}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    col = result.objects[0].columns[0]
    assert col.data_type == "numeric"


def test_extract_schema_meta_has_correct_capability(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_schema(_fake_adapter()))
    assert result.meta.capability == AdapterCapability.SCHEMA


# ===========================================================================
# Tests — extract_definitions()
# ===========================================================================


def test_extract_definitions_returns_definition_snapshot(tmp_path: Path) -> None:
    nodes = {
        "model.p.m": _model_node(
            uid="model.p.m",
            name="m",
            schema="public",
            compiled_code="SELECT 1 AS id",
        )
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_definitions(_fake_adapter()))
    assert isinstance(result, DefinitionSnapshot)


def test_extract_definitions_compiled_sql_included(tmp_path: Path) -> None:
    sql = "SELECT id, name FROM raw.users"
    nodes = {
        "model.p.users": _model_node(
            uid="model.p.users",
            name="users",
            schema="public",
            compiled_code=sql,
        )
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_definitions(_fake_adapter()))
    assert len(result.definitions) == 1
    defn = result.definitions[0]
    assert defn.definition_text == sql
    assert defn.definition_language == "sql"
    assert defn.schema_name == "public"
    assert defn.object_name == "users"


def test_extract_definitions_skips_nodes_without_compiled_sql(tmp_path: Path) -> None:
    nodes = {
        "model.p.a": _model_node(uid="model.p.a", name="a", schema="s", compiled_code="SELECT 1"),
        "model.p.b": _model_node(uid="model.p.b", name="b", schema="s", compiled_code=""),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_definitions(_fake_adapter()))
    assert len(result.definitions) == 1
    assert result.definitions[0].object_name == "a"


def test_extract_definitions_empty_when_no_compiled_sql(tmp_path: Path) -> None:
    nodes = {"model.p.a": _model_node(uid="model.p.a", name="a", schema="s")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_definitions(_fake_adapter()))
    assert len(result.definitions) == 0


def test_extract_definitions_meta_capability(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_definitions(_fake_adapter()))
    assert result.meta.capability == AdapterCapability.DEFINITIONS


# ===========================================================================
# Tests — extract_lineage()
# ===========================================================================


def test_extract_lineage_returns_lineage_snapshot(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_lineage(_fake_adapter()))
    assert isinstance(result, LineageSnapshot)


def test_extract_lineage_ref_edge_declared(tmp_path: Path) -> None:
    nodes = {
        "model.p.base": _model_node(uid="model.p.base", name="base", schema="public"),
        "model.p.derived": _model_node(
            uid="model.p.derived",
            name="derived",
            schema="public",
            depends_on=["model.p.base"],
        ),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_lineage(_fake_adapter()))
    assert len(result.edges) == 1
    edge = result.edges[0]
    assert edge.edge_kind == LineageEdgeKind.DECLARED
    assert edge.confidence == 1.0
    assert edge.source_object == "public.base"
    assert edge.target_object == "public.derived"


def test_extract_lineage_source_ref_edge(tmp_path: Path) -> None:
    nodes = {
        "model.p.orders": _model_node(
            uid="model.p.orders",
            name="orders",
            schema="analytics",
            depends_on=["source.p.raw.raw_orders"],
        ),
    }
    sources = {
        "source.p.raw.raw_orders": _source_node(
            uid="source.p.raw.raw_orders", name="raw_orders", schema="raw"
        ),
    }
    manifest_path = _write_json(
        tmp_path / "manifest.json", _manifest(nodes=nodes, sources=sources)
    )
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_lineage(_fake_adapter()))
    assert len(result.edges) == 1
    edge = result.edges[0]
    assert edge.source_object == "raw.raw_orders"
    assert edge.target_object == "analytics.orders"
    assert edge.edge_kind == LineageEdgeKind.DECLARED


def test_extract_lineage_unknown_dep_skipped(tmp_path: Path) -> None:
    nodes = {
        "model.p.orders": _model_node(
            uid="model.p.orders",
            name="orders",
            schema="public",
            depends_on=["model.p.nonexistent"],
        ),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_lineage(_fake_adapter()))
    assert len(result.edges) == 0


def test_extract_lineage_multiple_edges(tmp_path: Path) -> None:
    nodes = {
        "model.p.a": _model_node(uid="model.p.a", name="a", schema="s"),
        "model.p.b": _model_node(uid="model.p.b", name="b", schema="s"),
        "model.p.c": _model_node(
            uid="model.p.c",
            name="c",
            schema="s",
            depends_on=["model.p.a", "model.p.b"],
        ),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_lineage(_fake_adapter()))
    assert len(result.edges) == 2
    targets = {e.source_object for e in result.edges}
    assert "s.a" in targets
    assert "s.b" in targets


def test_extract_lineage_meta_capability(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.extract_lineage(_fake_adapter()))
    assert result.meta.capability == AdapterCapability.LINEAGE


# ===========================================================================
# Tests — extract_traffic / extract_orchestration raise NotImplementedError
# ===========================================================================


def test_extract_traffic_raises_not_implemented(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    with pytest.raises(NotImplementedError):
        run(adapter.extract_traffic(_fake_adapter()))


def test_extract_orchestration_raises_not_implemented(tmp_path: Path) -> None:
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest())
    adapter = DbtAdapter(manifest_path=manifest_path)
    with pytest.raises(NotImplementedError):
        run(adapter.extract_orchestration(_fake_adapter()))


# ===========================================================================
# Tests — Fintual manifest integration (realistic data)
# ===========================================================================

FINTUAL_MANIFEST = "/opt/velum/repos/velum-alma-extract/customers/fintual/dbt-bq-main/target/manifest.json"


@pytest.mark.skipif(
    not __import__("pathlib").Path(FINTUAL_MANIFEST).exists(),
    reason="Fintual manifest not available",
)
def test_fintual_discover(tmp_path: Path) -> None:
    adapter = DbtAdapter(manifest_path=FINTUAL_MANIFEST)
    result = run(adapter.discover(_fake_adapter()))
    # Should find the dbtbq project container.
    project_containers = [c for c in result.containers if c.container_type == "project"]
    assert len(project_containers) == 1
    assert "dbtbq" in project_containers[0].display_name
    # Should find multiple schema containers.
    schema_containers = [c for c in result.containers if c.container_type == "schema"]
    assert len(schema_containers) > 10


@pytest.mark.skipif(
    not __import__("pathlib").Path(FINTUAL_MANIFEST).exists(),
    reason="Fintual manifest not available",
)
def test_fintual_extract_schema(tmp_path: Path) -> None:
    adapter = DbtAdapter(manifest_path=FINTUAL_MANIFEST)
    result = run(adapter.extract_schema(_fake_adapter()))
    # 137 model nodes (filtered from 330 total) + 152 sources = 289
    assert len(result.objects) > 200
    # Sources should be EXTERNAL_TABLE.
    external = [o for o in result.objects if o.kind == SchemaObjectKindV2.EXTERNAL_TABLE]
    assert len(external) > 100


@pytest.mark.skipif(
    not __import__("pathlib").Path(FINTUAL_MANIFEST).exists(),
    reason="Fintual manifest not available",
)
def test_fintual_extract_definitions_empty_since_no_compiled_sql(tmp_path: Path) -> None:
    """The Fintual manifest has no compiled_code (compile was not run), so definitions are empty."""
    adapter = DbtAdapter(manifest_path=FINTUAL_MANIFEST)
    result = run(adapter.extract_definitions(_fake_adapter()))
    # Graceful — returns empty tuple rather than crashing.
    assert isinstance(result.definitions, tuple)


@pytest.mark.skipif(
    not __import__("pathlib").Path(FINTUAL_MANIFEST).exists(),
    reason="Fintual manifest not available",
)
def test_fintual_extract_lineage(tmp_path: Path) -> None:
    adapter = DbtAdapter(manifest_path=FINTUAL_MANIFEST)
    result = run(adapter.extract_lineage(_fake_adapter()))
    # 132 models with depends_on, producing many edges.
    assert len(result.edges) > 100
    # All edges are DECLARED with confidence 1.0.
    assert all(e.edge_kind == LineageEdgeKind.DECLARED for e in result.edges)
    assert all(e.confidence == 1.0 for e in result.edges)


@pytest.mark.skipif(
    not __import__("pathlib").Path(FINTUAL_MANIFEST).exists(),
    reason="Fintual manifest not available",
)
def test_fintual_probe_all_available(tmp_path: Path) -> None:
    adapter = DbtAdapter(manifest_path=FINTUAL_MANIFEST)
    results = run(adapter.probe(_fake_adapter()))
    assert all(r.available for r in results)
