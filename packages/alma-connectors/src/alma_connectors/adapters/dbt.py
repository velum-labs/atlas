"""dbt manifest source adapter implementation."""

from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from alma_connectors.source_adapter import (
    ConnectionTestResult,
    PersistedSourceAdapter,
    QueryResult,
    SchemaObjectKind,
    SchemaSnapshot,
    SetupInstructions,
    SourceAdapterCapabilities,
    SourceAdapterKind,
    SourceColumnSchema,
    SourceObjectDependency,
    SourceTableSchema,
    TrafficObservationResult,
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
    OrchestrationSnapshot,
    SchemaObject,
    SchemaObjectKind as SchemaObjectKindV2,
    SchemaSnapshotV2,
    ScopeContext,
    SourceAdapterKindV2,
    TrafficExtractionResult,
)

logger = logging.getLogger(__name__)

# Manifest schema version URL prefix used to detect dbt Core version family.
_MANIFEST_V12_PREFIX = "https://schemas.getdbt.com/dbt/manifest/v12"
_MANIFEST_V20_PREFIX = "https://schemas.getdbt.com/dbt/manifest/v20"

# resource_type values treated as first-class schema objects (models).
_MODEL_RESOURCE_TYPES = frozenset(("model", "seed", "snapshot"))

# Materialization → SchemaObjectKind mapping.
_MATERIALIZATION_KIND: dict[str, SchemaObjectKind] = {
    "table": SchemaObjectKind.TABLE,
    "incremental": SchemaObjectKind.TABLE,
    "snapshot": SchemaObjectKind.TABLE,
    "seed": SchemaObjectKind.TABLE,
    "view": SchemaObjectKind.VIEW,
    "ephemeral": SchemaObjectKind.VIEW,
    "materialized_view": SchemaObjectKind.MATERIALIZED_VIEW,
}

# v2 materialization → SchemaObjectKindV2 mapping.
_MATERIALIZATION_KIND_V2: dict[str, SchemaObjectKindV2] = {
    "table": SchemaObjectKindV2.TABLE,
    "incremental": SchemaObjectKindV2.TABLE,
    "snapshot": SchemaObjectKindV2.TABLE,
    "seed": SchemaObjectKindV2.TABLE,
    "view": SchemaObjectKindV2.VIEW,
    "ephemeral": SchemaObjectKindV2.VIEW,
    "materialized_view": SchemaObjectKindV2.MATERIALIZED_VIEW,
}


class DbtAdapter:
    """File-based dbt source adapter.

    Parses dbt artifact files (manifest.json, catalog.json, run_results.json)
    and exposes schema objects and lineage through the SourceAdapter protocol.
    No live database connection is required.

    Supported manifest versions:
        - v12 (dbt Core 1.8+)
        - v20 (dbt Fusion)

    Data flow:
        manifest.json (required) → nodes + sources + dependencies
        catalog.json  (optional) → column type / description enrichment
        run_results.json (optional) → execution timing metadata
    """

    kind = SourceAdapterKind.DBT
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=False,
        can_execute_query=False,
    )

    # v2 — dbt has no live DB so TRAFFIC and ORCHESTRATION are not supported.
    declared_capabilities: frozenset[AdapterCapability] = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
        AdapterCapability.DEFINITIONS,
        AdapterCapability.LINEAGE,
    })

    def __init__(
        self,
        manifest_path: str,
        catalog_path: str | None = None,
        run_results_path: str | None = None,
        project_name: str | None = None,
    ) -> None:
        """Initialise the adapter with paths to dbt artifact files.

        Args:
            manifest_path: Path to manifest.json (required).
            catalog_path: Path to catalog.json produced by ``dbt docs generate``
                (optional; enriches column types and descriptions).
            run_results_path: Path to run_results.json (optional; enriches
                objects with execution timing metadata).
            project_name: Override project name used in log messages and
                connection test output.  Defaults to the value stored in
                manifest metadata.
        """
        self._manifest_path = manifest_path
        self._catalog_path = catalog_path
        self._run_results_path = run_results_path
        self._project_name = project_name

    # ------------------------------------------------------------------
    # Internal helpers — file I/O
    # ------------------------------------------------------------------

    def _load_json(self, path: str, *, label: str) -> dict[str, Any]:
        """Load and parse a JSON file, raising descriptive errors."""
        file_path = Path(path)
        if not file_path.exists():
            raise FileNotFoundError(f"{label} not found: {path}")
        try:
            with file_path.open(encoding="utf-8") as fh:
                data = json.load(fh)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON in {label} ({path}): {exc}") from exc
        if not isinstance(data, dict):
            raise ValueError(f"{label} must be a JSON object, got {type(data).__name__}")
        return data  # type: ignore[return-value]

    def _load_manifest(self) -> dict[str, Any]:
        """Load manifest.json."""
        return self._load_json(self._manifest_path, label="manifest.json")

    def _load_catalog(self) -> dict[str, Any] | None:
        """Load catalog.json if a path was provided."""
        if self._catalog_path is None:
            return None
        return self._load_json(self._catalog_path, label="catalog.json")

    def _load_run_results(self) -> dict[str, Any] | None:
        """Load run_results.json if a path was provided."""
        if self._run_results_path is None:
            return None
        return self._load_json(self._run_results_path, label="run_results.json")

    # ------------------------------------------------------------------
    # Internal helpers — manifest parsing
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_manifest_version(manifest: dict[str, Any]) -> str:
        """Return the raw dbt_schema_version string from manifest metadata."""
        return manifest.get("metadata", {}).get("dbt_schema_version", "")

    @staticmethod
    def _object_kind_for_node(node: dict[str, Any]) -> SchemaObjectKind:
        """Derive SchemaObjectKind from node config.materialized / resource_type."""
        resource_type = node.get("resource_type", "")
        if resource_type == "source":
            return SchemaObjectKind.TABLE
        materialized = node.get("config", {}).get("materialized", "table")
        return _MATERIALIZATION_KIND.get(materialized, SchemaObjectKind.TABLE)

    @staticmethod
    def _extract_columns(
        node: dict[str, Any],
        catalog_node: dict[str, Any] | None,
    ) -> tuple[SourceColumnSchema, ...]:
        """Build column list, preferring catalog data for type information.

        Catalog columns carry ``type`` from the warehouse; manifest columns may
        carry ``data_type`` from schema.yml annotations.  The merge strategy is:
            1. Iterate manifest columns (preserving declaration order).
            2. For each, look up the matching catalog column for its type.
            3. Append any catalog-only columns not present in the manifest.
        """
        # Build catalog column lookup keyed by lower-case name.
        catalog_cols: dict[str, dict[str, Any]] = {}
        if catalog_node:
            for col_name, col_data in catalog_node.get("columns", {}).items():
                catalog_cols[col_name.lower()] = col_data

        columns: list[SourceColumnSchema] = []
        seen: set[str] = set()

        for col_name, col_data in node.get("columns", {}).items():
            col_lower = col_name.lower()
            seen.add(col_lower)
            catalog_col = catalog_cols.get(col_lower, {})
            data_type = catalog_col.get("type") or col_data.get("data_type") or "unknown"
            columns.append(
                SourceColumnSchema(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=True,
                )
            )

        # Append catalog-only columns (not declared in schema.yml).
        for col_lower, col_data in catalog_cols.items():
            if col_lower not in seen:
                data_type = col_data.get("type") or "unknown"
                display_name = col_data.get("name") or col_lower
                columns.append(
                    SourceColumnSchema(
                        name=display_name,
                        data_type=data_type,
                        is_nullable=True,
                    )
                )

        return tuple(columns)

    def _node_to_table_schema(
        self,
        node: dict[str, Any],
        catalog_node: dict[str, Any] | None,
    ) -> SourceTableSchema:
        """Convert a manifest node / source to a SourceTableSchema."""
        schema_name = node.get("schema", "")
        object_name = node.get("alias") or node.get("name", "")
        object_kind = self._object_kind_for_node(node)
        columns = self._extract_columns(node, catalog_node)
        return SourceTableSchema(
            schema_name=schema_name,
            object_name=object_name,
            object_kind=object_kind,
            columns=columns,
        )

    # ------------------------------------------------------------------
    # Protocol methods
    # ------------------------------------------------------------------

    async def test_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Verify that manifest.json exists, is valid JSON, and has a schema version.

        Args:
            adapter: Persisted adapter record (used for context only).

        Returns:
            ConnectionTestResult indicating success or failure with a message.
        """
        try:
            manifest = self._load_manifest()
        except FileNotFoundError as exc:
            return ConnectionTestResult(success=False, message=str(exc))
        except ValueError as exc:
            return ConnectionTestResult(success=False, message=str(exc))

        schema_version = self._detect_manifest_version(manifest)
        if not schema_version:
            return ConnectionTestResult(
                success=False,
                message="manifest.json is missing metadata.dbt_schema_version",
            )

        project_name = self._project_name or manifest.get("metadata", {}).get("project_name", "") or "unknown"
        node_count = len(manifest.get("nodes", {})) + len(manifest.get("sources", {}))

        return ConnectionTestResult(
            success=True,
            message=f"dbt project '{project_name}' loaded (schema: {schema_version})",
            resource_count=node_count,
            resource_label="dbt objects",
        )

    async def introspect_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
        """Parse manifest and catalog artifacts to produce a SchemaSnapshot.

        Processing steps:
            1. Load manifest.json (required) and catalog.json (optional).
            2. Collect models, seeds, and snapshots from ``manifest["nodes"]``.
            3. Collect external sources from ``manifest["sources"]``.
            4. Merge column types from catalog when available.
            5. Build SourceObjectDependency edges from ``depends_on.nodes``.

        Args:
            adapter: Persisted adapter record (used for context only).

        Returns:
            SchemaSnapshot with all discovered objects and lineage edges.
        """
        manifest = self._load_manifest()
        catalog = self._load_catalog()

        # Build catalog lookup: unique_id → catalog node data.
        catalog_lookup: dict[str, dict[str, Any]] = {}
        if catalog:
            for uid, node_data in catalog.get("nodes", {}).items():
                catalog_lookup[uid] = node_data
            for uid, node_data in catalog.get("sources", {}).items():
                catalog_lookup[uid] = node_data

        # Build a combined node lookup for dependency resolution.
        node_lookup: dict[str, dict[str, Any]] = {}
        objects: list[SourceTableSchema] = []

        # --- Models, seeds, snapshots ---
        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type", "") not in _MODEL_RESOURCE_TYPES:
                continue
            node_lookup[uid] = node
            objects.append(self._node_to_table_schema(node, catalog_lookup.get(uid)))

        # --- External sources ---
        for uid, source in manifest.get("sources", {}).items():
            node_lookup[uid] = source
            objects.append(self._node_to_table_schema(source, catalog_lookup.get(uid)))

        # --- Dependency edges ---
        dependencies: list[SourceObjectDependency] = []
        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type", "") not in _MODEL_RESOURCE_TYPES:
                continue

            node_schema = node.get("schema", "")
            node_name = node.get("alias") or node.get("name", "")
            if not node_schema or not node_name:
                continue

            for dep_uid in node.get("depends_on", {}).get("nodes", []):
                dep_node = node_lookup.get(dep_uid)
                if dep_node is None:
                    logger.debug("Skipping unknown dependency %s for node %s", dep_uid, uid)
                    continue
                dep_schema = dep_node.get("schema", "")
                dep_name = dep_node.get("alias") or dep_node.get("name", "")
                if not dep_schema or not dep_name:
                    continue
                dependencies.append(
                    SourceObjectDependency(
                        source_schema=node_schema,
                        source_object=node_name,
                        target_schema=dep_schema,
                        target_object=dep_name,
                    )
                )

        return SchemaSnapshot(
            captured_at=datetime.now(UTC),
            objects=tuple(objects),
            dependencies=tuple(dependencies),
        )

    async def observe_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        """Return an empty result — dbt artifacts do not carry query traffic.

        Args:
            adapter: Persisted adapter record (unused).
            since: Lower bound timestamp (unused).

        Returns:
            TrafficObservationResult with zero records and no events.
        """
        return TrafficObservationResult(scanned_records=0, events=())

    async def execute_query(
        self,
        adapter: PersistedSourceAdapter,
        sql: str,
        *,
        max_rows: int | None = None,
        probe_target: str | None = None,
    ) -> QueryResult:
        """Not supported — dbt adapter is read-only and file-based.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("dbt adapter does not support query execution (can_execute_query=False)")

    def get_setup_instructions(self) -> SetupInstructions:
        """Return operator guidance for enabling the dbt adapter.

        Returns:
            SetupInstructions with steps for generating and locating dbt artifacts.
        """
        return SetupInstructions(
            title="dbt Manifest Adapter",
            summary=(
                "Parse dbt artifact files to extract schema objects and lineage. "
                "No live database connection is required."
            ),
            steps=(
                "Run `dbt compile` or `dbt run` to generate manifest.json in target/",
                "Optionally run `dbt docs generate` to produce catalog.json with column types",
                "Optionally locate run_results.json for execution timing metadata",
                "Provide the file paths when constructing the DbtAdapter",
            ),
        )

    # ------------------------------------------------------------------
    # v2 — internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _object_kind_v2_for_node(node: dict[str, Any]) -> SchemaObjectKindV2:
        """Derive v2 SchemaObjectKind from node config."""
        resource_type = node.get("resource_type", "")
        if resource_type == "source":
            return SchemaObjectKindV2.EXTERNAL_TABLE
        materialized = node.get("config", {}).get("materialized", "table")
        return _MATERIALIZATION_KIND_V2.get(materialized, SchemaObjectKindV2.TABLE)

    @staticmethod
    def _extract_columns_v2(
        node: dict[str, Any],
        catalog_node: dict[str, Any] | None,
    ) -> tuple[ColumnSchema, ...]:
        """Build v2 ColumnSchema list, merging manifest and catalog data."""
        catalog_cols: dict[str, dict[str, Any]] = {}
        if catalog_node:
            for col_name, col_data in catalog_node.get("columns", {}).items():
                catalog_cols[col_name.lower()] = col_data

        columns: list[ColumnSchema] = []
        seen: set[str] = set()

        for col_name, col_data in node.get("columns", {}).items():
            col_lower = col_name.lower()
            seen.add(col_lower)
            catalog_col = catalog_cols.get(col_lower, {})
            data_type = catalog_col.get("type") or col_data.get("data_type") or "unknown"
            description = col_data.get("description") or None
            columns.append(
                ColumnSchema(
                    name=col_name,
                    data_type=data_type,
                    is_nullable=True,
                    description=description if description else None,
                )
            )

        for col_lower, col_data in catalog_cols.items():
            if col_lower not in seen:
                data_type = col_data.get("type") or "unknown"
                display_name = col_data.get("name") or col_lower
                columns.append(
                    ColumnSchema(
                        name=display_name,
                        data_type=data_type,
                        is_nullable=True,
                    )
                )

        return tuple(columns)

    def _node_to_schema_object_v2(
        self,
        node: dict[str, Any],
        catalog_node: dict[str, Any] | None,
    ) -> SchemaObject:
        """Convert a manifest node / source to a v2 SchemaObject."""
        schema_name = node.get("schema", "")
        object_name = node.get("alias") or node.get("name", "")
        kind = self._object_kind_v2_for_node(node)
        columns = self._extract_columns_v2(node, catalog_node)
        description = node.get("description") or None
        tags = tuple(node.get("tags", []))
        return SchemaObject(
            schema_name=schema_name,
            object_name=object_name,
            kind=kind,
            columns=columns,
            description=description if description else None,
            tags=tags,
        )

    def _make_meta(
        self,
        adapter: PersistedSourceAdapter,
        capability: AdapterCapability,
        row_count: int,
        duration_ms: float,
    ) -> ExtractionMeta:
        return ExtractionMeta(
            adapter_key=adapter.key,
            adapter_kind=SourceAdapterKindV2.DBT,
            capability=capability,
            scope_context=ScopeContext(
                scope=ExtractionScope.GLOBAL,
                identifiers={"manifest_path": self._manifest_path},
            ),
            captured_at=datetime.now(UTC),
            duration_ms=duration_ms,
            row_count=row_count,
        )

    # ------------------------------------------------------------------
    # v2 — protocol methods
    # ------------------------------------------------------------------

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        """Probe availability of declared capabilities.

        All capabilities gate on manifest.json being present and parseable.
        """
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities

        try:
            self._load_manifest()
            available = True
            message = None
        except FileNotFoundError as exc:
            available = False
            message = str(exc)
        except ValueError as exc:
            available = False
            message = str(exc)

        return tuple(
            CapabilityProbeResult(
                capability=cap,
                available=available,
                scope=ExtractionScope.GLOBAL,
                scope_context=ScopeContext(
                    scope=ExtractionScope.GLOBAL,
                    identifiers={"manifest_path": self._manifest_path},
                ),
                message=message,
            )
            for cap in caps_to_probe
        )

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        """DISCOVER: dbt project as top-level container, each unique schema as sub-containers."""
        t0 = time.monotonic()
        manifest = self._load_manifest()
        meta_block = manifest.get("metadata", {})
        project_name = self._project_name or meta_block.get("project_name") or "unknown"

        # One container for the dbt project itself.
        containers: list[DiscoveredContainer] = [
            DiscoveredContainer(
                container_id=f"dbt://{project_name}",
                container_type="project",
                display_name=project_name,
                metadata={
                    "dbt_schema_version": meta_block.get("dbt_schema_version", ""),
                    "dbt_version": meta_block.get("dbt_version", ""),
                    "adapter_type": meta_block.get("adapter_type", ""),
                    "node_count": len(manifest.get("nodes", {})),
                    "source_count": len(manifest.get("sources", {})),
                },
            )
        ]

        # One container per unique schema encountered across models and sources.
        seen_schemas: set[str] = set()
        for node in manifest.get("nodes", {}).values():
            if node.get("resource_type", "") in _MODEL_RESOURCE_TYPES:
                schema = node.get("schema", "")
                if schema:
                    seen_schemas.add(schema)
        for source in manifest.get("sources", {}).values():
            schema = source.get("schema", "")
            if schema:
                seen_schemas.add(schema)

        for schema in sorted(seen_schemas):
            containers.append(
                DiscoveredContainer(
                    container_id=f"dbt://{project_name}/{schema}",
                    container_type="schema",
                    display_name=schema,
                    metadata={"project": project_name},
                )
            )

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.DISCOVER, len(containers), duration_ms)
        return DiscoverySnapshot(meta=meta, containers=tuple(containers))

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        """SCHEMA: parse catalog.json (if available) → SchemaSnapshotV2.

        Falls back to manifest-only schema when no catalog is provided.
        Sources are represented as EXTERNAL_TABLE.
        """
        t0 = time.monotonic()
        manifest = self._load_manifest()
        catalog = self._load_catalog()

        catalog_lookup: dict[str, dict[str, Any]] = {}
        if catalog:
            for uid, node_data in catalog.get("nodes", {}).items():
                catalog_lookup[uid] = node_data
            for uid, node_data in catalog.get("sources", {}).items():
                catalog_lookup[uid] = node_data

        objects: list[SchemaObject] = []

        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type", "") not in _MODEL_RESOURCE_TYPES:
                continue
            objects.append(self._node_to_schema_object_v2(node, catalog_lookup.get(uid)))

        for uid, source in manifest.get("sources", {}).items():
            objects.append(self._node_to_schema_object_v2(source, catalog_lookup.get(uid)))

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.SCHEMA, len(objects), duration_ms)
        return SchemaSnapshotV2(meta=meta, objects=tuple(objects))

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """DEFINITIONS: compiled SQL from manifest nodes → DefinitionSnapshot.

        Only nodes with non-empty compiled_code (or compiled_sql for older dbt) are included.
        """
        t0 = time.monotonic()
        manifest = self._load_manifest()

        definitions: list[ObjectDefinition] = []
        for _uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type", "") not in _MODEL_RESOURCE_TYPES:
                continue
            compiled = node.get("compiled_code") or node.get("compiled_sql") or ""
            if not compiled or not compiled.strip():
                continue
            schema_name = node.get("schema", "")
            object_name = node.get("alias") or node.get("name", "")
            if not schema_name or not object_name:
                continue
            kind = self._object_kind_v2_for_node(node)
            definitions.append(
                ObjectDefinition(
                    schema_name=schema_name,
                    object_name=object_name,
                    object_kind=kind,
                    definition_text=compiled.strip(),
                    definition_language="sql",
                )
            )

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.DEFINITIONS, len(definitions), duration_ms)
        return DefinitionSnapshot(meta=meta, definitions=tuple(definitions))

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """LINEAGE: ref() and source() graph from manifest depends_on → LineageSnapshot.

        All edges have edge_kind=DECLARED and confidence=1.0.
        source() declarations produce cross-system edges (target is the external source).
        Object identifiers use the format ``schema.name``.
        """
        t0 = time.monotonic()
        manifest = self._load_manifest()

        # Build lookup: unique_id → (schema, name).
        node_lookup: dict[str, tuple[str, str]] = {}
        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type", "") not in _MODEL_RESOURCE_TYPES:
                continue
            schema = node.get("schema", "")
            name = node.get("alias") or node.get("name", "")
            if schema and name:
                node_lookup[uid] = (schema, name)
        for uid, source in manifest.get("sources", {}).items():
            schema = source.get("schema", "")
            name = source.get("alias") or source.get("name", "")
            if schema and name:
                node_lookup[uid] = (schema, name)

        edges: list[LineageEdge] = []
        for uid, node in manifest.get("nodes", {}).items():
            if node.get("resource_type", "") not in _MODEL_RESOURCE_TYPES:
                continue
            target_schema = node.get("schema", "")
            target_name = node.get("alias") or node.get("name", "")
            if not target_schema or not target_name:
                continue
            target_fqn = f"{target_schema}.{target_name}"

            for dep_uid in node.get("depends_on", {}).get("nodes", []):
                resolved = node_lookup.get(dep_uid)
                if resolved is None:
                    logger.debug("Skipping unknown lineage dep %s for node %s", dep_uid, uid)
                    continue
                src_schema, src_name = resolved
                edges.append(
                    LineageEdge(
                        source_object=f"{src_schema}.{src_name}",
                        target_object=target_fqn,
                        edge_kind=LineageEdgeKind.DECLARED,
                        confidence=1.0,
                    )
                )

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.LINEAGE, len(edges), duration_ms)
        return LineageSnapshot(meta=meta, edges=tuple(edges))

    async def extract_traffic(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficExtractionResult:
        """Not supported — dbt has no live database.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("dbt adapter does not support TRAFFIC extraction (no live database)")

    async def extract_orchestration(
        self,
        adapter: PersistedSourceAdapter,
    ) -> OrchestrationSnapshot:
        """Not supported — dbt has no orchestration primitives.

        Raises:
            NotImplementedError: Always.
        """
        raise NotImplementedError("dbt adapter does not support ORCHESTRATION extraction")
