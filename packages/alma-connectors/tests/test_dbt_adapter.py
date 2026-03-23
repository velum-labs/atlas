"""Tests for DbtAdapter."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest

from alma_connectors import DbtAdapter, SourceAdapterKind
from alma_connectors.adapters.dbt import _MANIFEST_V12_PREFIX, _MANIFEST_V20_PREFIX
from alma_connectors.source_adapter import (
    BigQueryAdapterConfig,
    ExternalSecretRef,
    PersistedSourceAdapter,
    SchemaObjectKind,
    SourceAdapterStatus,
)

# ---------------------------------------------------------------------------
# Fixtures — fake PersistedSourceAdapter
# ---------------------------------------------------------------------------

_FAKE_SA_SECRET = ExternalSecretRef(provider="vault", reference="path/to/secret")


def _fake_adapter(kind: str = "dbt") -> PersistedSourceAdapter:
    """Return a minimal PersistedSourceAdapter for passing to protocol methods."""
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


def _manifest_v12(
    nodes: dict[str, Any] | None = None,
    sources: dict[str, Any] | None = None,
    project_name: str = "my_project",
) -> dict[str, Any]:
    """Build a minimal manifest.json v12 payload."""
    return {
        "metadata": {
            "dbt_schema_version": f"{_MANIFEST_V12_PREFIX}/manifest.json",
            "dbt_version": "1.8.0",
            "project_name": project_name,
        },
        "nodes": nodes or {},
        "sources": sources or {},
    }


def _manifest_v20(
    nodes: dict[str, Any] | None = None,
    sources: dict[str, Any] | None = None,
    project_name: str = "my_project",
) -> dict[str, Any]:
    """Build a minimal manifest.json v20 (Fusion) payload."""
    return {
        "metadata": {
            "dbt_schema_version": f"{_MANIFEST_V20_PREFIX}/manifest.json",
            "dbt_version": "2.0.0",
            "project_name": project_name,
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
    }


def _catalog(nodes: dict[str, Any] | None = None, sources: dict[str, Any] | None = None) -> dict[str, Any]:
    return {"nodes": nodes or {}, "sources": sources or {}}


def _run_results(results: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v4/run-results.json"},
        "results": results or [],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_json(path: Path, data: dict[str, Any]) -> str:
    path.write_text(json.dumps(data), encoding="utf-8")
    return str(path)


def run(coro: Any) -> Any:  # noqa: ANN401
    return asyncio.run(coro)


# ===========================================================================
# Tests — adapter metadata
# ===========================================================================


def test_kind_is_dbt() -> None:
    """DbtAdapter.kind must be SourceAdapterKind.DBT."""
    adapter = DbtAdapter(manifest_path="/nonexistent/manifest.json")
    assert adapter.kind == SourceAdapterKind.DBT


def test_capabilities() -> None:
    """Capability flags must match file-based, read-only semantics."""
    adapter = DbtAdapter(manifest_path="/nonexistent/manifest.json")
    caps = adapter.capabilities
    assert caps.can_test_connection is True
    assert caps.can_introspect_schema is True
    assert caps.can_observe_traffic is False
    assert caps.can_execute_query is False


def test_get_setup_instructions_returns_value() -> None:
    """get_setup_instructions should return a non-empty SetupInstructions."""
    adapter = DbtAdapter(manifest_path="/nonexistent/manifest.json")
    instructions = adapter.get_setup_instructions()
    assert instructions.title
    assert instructions.summary
    assert len(instructions.steps) >= 2


# ===========================================================================
# Tests — test_connection
# ===========================================================================


def test_test_connection_success(tmp_path: Path) -> None:
    """test_connection succeeds for a valid manifest with a schema version."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is True
    assert "my_project" in result.message


def test_test_connection_missing_file() -> None:
    """test_connection fails gracefully when manifest.json does not exist."""
    adapter = DbtAdapter(manifest_path="/does/not/exist/manifest.json")
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is False
    assert "not found" in result.message.lower()


def test_test_connection_invalid_json(tmp_path: Path) -> None:
    """test_connection fails gracefully when manifest.json is not valid JSON."""
    bad_path = tmp_path / "manifest.json"
    bad_path.write_text("{ this is not valid json }", encoding="utf-8")
    adapter = DbtAdapter(manifest_path=str(bad_path))
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is False
    assert "invalid json" in result.message.lower()


def test_test_connection_missing_schema_version(tmp_path: Path) -> None:
    """test_connection fails when dbt_schema_version is absent."""
    manifest = {"metadata": {}, "nodes": {}, "sources": {}}
    manifest_path = _write_json(tmp_path / "manifest.json", manifest)
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is False
    assert "dbt_schema_version" in result.message


def test_test_connection_resource_count(tmp_path: Path) -> None:
    """test_connection resource_count reflects nodes + sources."""
    nodes = {
        "model.p.a": _model_node(uid="model.p.a", name="a"),
        "model.p.b": _model_node(uid="model.p.b", name="b"),
    }
    sources = {"source.p.raw.c": _source_node(uid="source.p.raw.c", name="c")}
    manifest = _manifest_v12(nodes=nodes, sources=sources)
    manifest_path = _write_json(tmp_path / "manifest.json", manifest)
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is True
    assert result.resource_count == 3


def test_test_connection_project_name_override(tmp_path: Path) -> None:
    """project_name kwarg overrides manifest-embedded project name."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(project_name="original"))
    adapter = DbtAdapter(manifest_path=manifest_path, project_name="overridden")
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is True
    assert "overridden" in result.message


# ===========================================================================
# Tests — introspect_schema: objects
# ===========================================================================


def test_introspect_schema_model(tmp_path: Path) -> None:
    """A model node appears as a SourceTableSchema."""
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders", schema="public")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.schema_name == "public"
    assert obj.object_name == "orders"
    assert obj.object_kind == SchemaObjectKind.TABLE


def test_introspect_schema_source(tmp_path: Path) -> None:
    """A source node appears as a SourceTableSchema with TABLE kind."""
    sources = {"source.p.raw.orders": _source_node(uid="source.p.raw.orders", name="orders", schema="raw")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(sources=sources))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.schema_name == "raw"
    assert obj.object_name == "orders"
    assert obj.object_kind == SchemaObjectKind.TABLE


def test_introspect_schema_ephemeral_model_is_view(tmp_path: Path) -> None:
    """Ephemeral models are represented as VIEW."""
    nodes = {
        "model.p.eph": _model_node(uid="model.p.eph", name="eph", materialized="ephemeral"),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert snapshot.objects[0].object_kind == SchemaObjectKind.VIEW


def test_introspect_schema_view_model(tmp_path: Path) -> None:
    """View-materialized models are represented as VIEW."""
    nodes = {
        "model.p.v": _model_node(uid="model.p.v", name="v", materialized="view"),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert snapshot.objects[0].object_kind == SchemaObjectKind.VIEW


def test_introspect_schema_columns_from_manifest(tmp_path: Path) -> None:
    """Columns declared in manifest appear in the schema object."""
    columns = {
        "id": {"name": "id", "data_type": "integer"},
        "name": {"name": "name", "data_type": "varchar"},
    }
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders", columns=columns)}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    col_map = {c.name: c for c in snapshot.objects[0].columns}
    assert "id" in col_map
    assert col_map["id"].data_type == "integer"
    assert "name" in col_map
    assert col_map["name"].data_type == "varchar"


# ===========================================================================
# Tests — dependency extraction
# ===========================================================================


def test_introspect_schema_ref_dependency(tmp_path: Path) -> None:
    """ref() dependencies produce SourceObjectDependency edges."""
    nodes = {
        "model.p.base": _model_node(uid="model.p.base", name="base", schema="public"),
        "model.p.derived": _model_node(
            uid="model.p.derived",
            name="derived",
            schema="public",
            depends_on=["model.p.base"],
        ),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert len(snapshot.dependencies) == 1
    dep = snapshot.dependencies[0]
    assert dep.source_schema == "public"
    assert dep.source_object == "derived"
    assert dep.target_schema == "public"
    assert dep.target_object == "base"


def test_introspect_schema_source_dependency(tmp_path: Path) -> None:
    """source() references create edges from model to source node."""
    nodes = {
        "model.p.orders": _model_node(
            uid="model.p.orders",
            name="orders",
            schema="public",
            depends_on=["source.p.raw.raw_orders"],
        ),
    }
    sources = {
        "source.p.raw.raw_orders": _source_node(
            uid="source.p.raw.raw_orders", name="raw_orders", schema="raw"
        ),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes, sources=sources))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert len(snapshot.dependencies) == 1
    dep = snapshot.dependencies[0]
    assert dep.source_object == "orders"
    assert dep.target_object == "raw_orders"
    assert dep.target_schema == "raw"


def test_introspect_schema_missing_dep_node_skipped(tmp_path: Path) -> None:
    """Dependencies referencing unknown node IDs are silently skipped."""
    nodes = {
        "model.p.orders": _model_node(
            uid="model.p.orders",
            name="orders",
            schema="public",
            depends_on=["model.p.nonexistent"],
        ),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    # Unknown dependency is skipped; no crash, no edges.
    assert len(snapshot.dependencies) == 0


# ===========================================================================
# Tests — manifest version detection (v12 and v20)
# ===========================================================================


def test_manifest_v12_accepted(tmp_path: Path) -> None:
    """Manifest v12 schema version is accepted by test_connection."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is True
    assert "v12" in result.message


def test_manifest_v20_accepted(tmp_path: Path) -> None:
    """Manifest v20 (Fusion) schema version is accepted by test_connection."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v20())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.test_connection(_fake_adapter()))
    assert result.success is True
    assert "v20" in result.message


def test_manifest_v20_introspect(tmp_path: Path) -> None:
    """introspect_schema works identically for v20 manifests."""
    nodes = {"model.p.m": _model_node(uid="model.p.m", name="m", schema="dbt")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v20(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].schema_name == "dbt"


# ===========================================================================
# Tests — catalog enrichment
# ===========================================================================


def test_catalog_enriches_column_types(tmp_path: Path) -> None:
    """Catalog column types override manifest data_type values."""
    columns = {"id": {"name": "id", "data_type": "unknown_from_manifest"}}
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders", columns=columns)}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))

    catalog_nodes = {
        "model.p.orders": {
            "columns": {"id": {"name": "id", "type": "integer", "comment": None}},
        }
    }
    catalog_path = _write_json(tmp_path / "catalog.json", _catalog(nodes=catalog_nodes))

    adapter = DbtAdapter(manifest_path=manifest_path, catalog_path=catalog_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    col = snapshot.objects[0].columns[0]
    assert col.name == "id"
    assert col.data_type == "integer"


def test_catalog_adds_catalog_only_columns(tmp_path: Path) -> None:
    """Columns present only in catalog (not in manifest) are added."""
    # Manifest has no columns declared.
    nodes = {"model.p.orders": _model_node(uid="model.p.orders", name="orders")}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))

    catalog_nodes = {
        "model.p.orders": {
            "columns": {
                "id": {"name": "id", "type": "bigint", "comment": None},
                "status": {"name": "status", "type": "text", "comment": None},
            }
        }
    }
    catalog_path = _write_json(tmp_path / "catalog.json", _catalog(nodes=catalog_nodes))

    adapter = DbtAdapter(manifest_path=manifest_path, catalog_path=catalog_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    col_names = {c.name for c in snapshot.objects[0].columns}
    assert col_names == {"id", "status"}


def test_introspect_without_catalog_uses_manifest_types(tmp_path: Path) -> None:
    """When no catalog is provided, manifest data_type values are used."""
    columns = {"amount": {"name": "amount", "data_type": "numeric"}}
    nodes = {"model.p.facts": _model_node(uid="model.p.facts", name="facts", columns=columns)}
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))

    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    col = snapshot.objects[0].columns[0]
    assert col.data_type == "numeric"


# ===========================================================================
# Tests — observe_traffic and execute_query stubs
# ===========================================================================


def test_observe_traffic_returns_empty(tmp_path: Path) -> None:
    """observe_traffic always returns zero records and no events."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12())
    adapter = DbtAdapter(manifest_path=manifest_path)
    result = run(adapter.observe_traffic(_fake_adapter()))
    assert result.scanned_records == 0
    assert result.events == ()


def test_execute_query_raises(tmp_path: Path) -> None:
    """execute_query raises NotImplementedError."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12())
    adapter = DbtAdapter(manifest_path=manifest_path)
    with pytest.raises(NotImplementedError):
        run(adapter.execute_query(_fake_adapter(), "SELECT 1"))


# ===========================================================================
# Tests — snapshot timestamp
# ===========================================================================


def test_introspect_schema_captured_at_is_utc(tmp_path: Path) -> None:
    """SchemaSnapshot.captured_at has UTC timezone info."""
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12())
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    from datetime import UTC

    assert snapshot.captured_at.tzinfo == UTC


# ===========================================================================
# Tests — non-model node types are excluded
# ===========================================================================


def test_non_model_nodes_excluded(tmp_path: Path) -> None:
    """Nodes with resource_type != model/seed/snapshot are excluded from objects."""
    nodes = {
        "test.p.not_null_orders_id": {
            "unique_id": "test.p.not_null_orders_id",
            "resource_type": "test",
            "name": "not_null_orders_id",
            "schema": "public",
            "config": {"materialized": "table"},
            "columns": {},
            "depends_on": {"nodes": []},
        },
        "model.p.orders": _model_node(uid="model.p.orders", name="orders"),
    }
    manifest_path = _write_json(tmp_path / "manifest.json", _manifest_v12(nodes=nodes))
    adapter = DbtAdapter(manifest_path=manifest_path)
    snapshot = run(adapter.introspect_schema(_fake_adapter()))
    assert len(snapshot.objects) == 1
    assert snapshot.objects[0].object_name == "orders"
