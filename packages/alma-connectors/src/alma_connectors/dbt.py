"""dbt source adapter for alma-connectors.

Reads a compiled dbt ``manifest.json`` to discover models, sources, seeds,
exposures, and their lineage. No external dependencies required beyond
standard library JSON parsing.
"""

from __future__ import annotations

import json
from pathlib import Path

from alma_connectors.domain import ColumnDef, SchemaSnapshot, TrafficObservationResult


class DbtAdapter:
    """Source adapter for dbt projects.

    Parses a dbt ``manifest.json`` artifact to extract:
    - Models, sources, seeds, and snapshots as assets
    - Column-level schemas from the manifest catalog
    - Compiled SQL for each model (for lineage analysis)

    No query traffic is available from dbt directly — traffic data must
    come from the underlying warehouse connector.
    """

    def __init__(self, manifest_path: str | Path, project_name: str | None = None) -> None:
        """Initialize the dbt adapter.

        Args:
            manifest_path: Path to a compiled dbt ``manifest.json`` file.
            project_name: Override for the dbt project name. Auto-detected from manifest if None.
        """
        self._manifest_path = Path(manifest_path)
        if not self._manifest_path.exists():
            raise FileNotFoundError(f"dbt manifest not found: {manifest_path}")

        with self._manifest_path.open() as f:
            self._manifest: dict = json.load(f)

        self._project_name = project_name or self._manifest.get("metadata", {}).get("project_name", "dbt")

    @property
    def source_id(self) -> str:
        return f"dbt:{self._project_name}"

    @property
    def source_type(self) -> str:
        return "dbt"

    def list_assets(self) -> list[dict]:
        """Return all models, sources, seeds, and snapshots from the manifest."""
        assets: list[dict] = []

        for node_id, node in self._manifest.get("nodes", {}).items():
            if node.get("resource_type") not in ("model", "seed", "snapshot"):
                continue
            assets.append(
                {
                    "id": f"dbt:{self._project_name}.{node['unique_id']}",
                    "name": node.get("name", node_id),
                    "kind": node.get("resource_type", "model"),
                    "source": self.source_id,
                    "metadata": {
                        "database": node.get("database"),
                        "schema": node.get("schema"),
                        "alias": node.get("alias"),
                        "fqn": node.get("fqn"),
                        "compiled_path": node.get("compiled_path"),
                        "depends_on": node.get("depends_on", {}).get("nodes", []),
                    },
                }
            )

        for source_id, source in self._manifest.get("sources", {}).items():
            assets.append(
                {
                    "id": f"dbt:{self._project_name}.{source['unique_id']}",
                    "name": source.get("name", source_id),
                    "kind": "source",
                    "source": self.source_id,
                    "metadata": {
                        "database": source.get("database"),
                        "schema": source.get("schema"),
                        "source_name": source.get("source_name"),
                        "identifier": source.get("identifier"),
                    },
                }
            )

        return assets

    def get_schema(self, asset_id: str) -> SchemaSnapshot | None:
        """Return column schema from the manifest node definition."""
        unique_id = asset_id.split(".", 2)[-1] if "." in asset_id else asset_id

        node = self._manifest.get("nodes", {}).get(unique_id) or self._manifest.get("sources", {}).get(unique_id)
        if not node:
            return None

        columns_raw = node.get("columns", {})
        columns = [
            ColumnDef(
                name=col["name"],
                type=col.get("data_type", "unknown"),
                description=col.get("description"),
            )
            for col in columns_raw.values()
        ]
        return SchemaSnapshot(asset_id=asset_id, source_type=self.source_type, columns=columns)

    def get_traffic(self) -> TrafficObservationResult:
        """dbt does not provide query traffic — returns empty result."""
        return TrafficObservationResult(
            source_id=self.source_id,
            source_type=self.source_type,
            queries=[],
        )
