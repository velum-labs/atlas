"""Edge storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable

type DataEdgeRow = dict[str, Any]
type EdgeAnalysisEventRow = dict[str, Any]
type EdgeContractVersionRow = dict[str, Any]
type EdgeProbeStateRow = dict[str, Any]
type EdgeTransportGroupMembershipRow = dict[str, Any]
type EdgeWaypointRow = dict[str, Any]
type PipelineAnalysisReviewRow = dict[str, Any]
type TransportGroupRow = dict[str, Any]


@runtime_checkable
class EdgeReader(Protocol):
    """Read-only access to persisted data edges and related records."""

    def list_data_edges(
        self,
        *,
        source_adapter_key: str | None = None,
        dest_adapter_key: str | None = None,
        status: str | None = None,
        discovery_method: str | None = None,
        limit: int = 100,
    ) -> list[DataEdgeRow]: ...

    def get_data_edge(self, *, edge_id: str) -> DataEdgeRow | None: ...

    def get_data_edge_by_objects(
        self,
        *,
        source_adapter_key: str,
        source_object: str,
        dest_adapter_key: str,
        dest_object: str,
    ) -> DataEdgeRow | None: ...

    def get_edge_contract_version(
        self,
        *,
        edge_id: str,
        version: str,
    ) -> EdgeContractVersionRow | None: ...

    def get_current_edge_contract_version(
        self,
        *,
        edge_id: str,
    ) -> EdgeContractVersionRow | None: ...

    def get_latest_edge_contract_version(
        self,
        *,
        edge_id: str,
    ) -> EdgeContractVersionRow | None: ...

    def list_edge_contract_versions(
        self,
        *,
        edge_id: str | None = None,
        status: str | None = None,
        only_current: bool | None = None,
        limit: int = 100,
    ) -> list[EdgeContractVersionRow]: ...

    def list_current_edge_contract_versions(
        self,
        *,
        target_id: str,
        edge_id: str | None = None,
        table_name: str | None = None,
        limit: int = 100,
    ) -> list[EdgeContractVersionRow]: ...

    def list_edge_waypoints(
        self,
        *,
        edge_id: str,
        limit: int = 100,
    ) -> list[EdgeWaypointRow]: ...

    def list_edge_analysis_events(
        self,
        *,
        edge_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[EdgeAnalysisEventRow]: ...

    def list_transport_groups(
        self,
        *,
        kind: str | None = None,
        limit: int = 100,
    ) -> list[TransportGroupRow]: ...

    def get_transport_group(self, *, group_id: str) -> TransportGroupRow | None: ...

    def list_edge_transport_group_memberships(
        self,
        *,
        group_id: str | None = None,
        edge_id: str | None = None,
        limit: int = 500,
    ) -> list[EdgeTransportGroupMembershipRow]: ...

    def get_edge_probe_state(
        self,
        *,
        edge_id: str,
        probe_type: str,
    ) -> EdgeProbeStateRow | None: ...

    def list_edge_probe_states(
        self,
        *,
        edge_id: str | None = None,
        limit: int = 500,
    ) -> list[EdgeProbeStateRow]: ...

    def list_pipeline_analysis_reviews(
        self,
        *,
        target_id: str | None = None,
        status: str | None = None,
        edge_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[PipelineAnalysisReviewRow]: ...

    def get_pipeline_analysis_review(
        self,
        *,
        review_id: str,
    ) -> PipelineAnalysisReviewRow | None: ...

    def get_pipeline_analysis_review_by_edge_id(
        self,
        *,
        edge_id: str,
    ) -> PipelineAnalysisReviewRow | None: ...


@runtime_checkable
class EdgeWriter(Protocol):
    """Write access to persisted data edges and related records."""

    def upsert_data_edge(
        self,
        *,
        edge_id: str | None = None,
        source_adapter_key: str,
        source_object: str,
        dest_adapter_key: str,
        dest_object: str,
        discovery_method: str,
        confidence: float,
        status: str = "discovered",
        transport_json: dict[str, object] | None = None,
        contract_json: dict[str, object] | None = None,
    ) -> DataEdgeRow | None: ...

    def insert_edge_contract_version(
        self,
        *,
        edge_id: str,
        version: str,
        status: str = "proposed",
        is_current: bool = False,
        enforcement_mode: str = "warn",
        transport_json: dict[str, object],
        contract_json: dict[str, object],
        contract_yaml: str,
        contract_yaml_path: str,
        artifact_sha256: str | None = None,
        previous_version: str | None = None,
        bump_type: str = "initial",
        bump_reason: str = "initial_analysis",
        diff_json: dict[str, object] | None = None,
        source_git_sha: str | None = None,
        activated_at: datetime | None = None,
    ) -> EdgeContractVersionRow | None: ...

    def update_edge_contract_version_fields(
        self,
        *,
        edge_id: str,
        version: str,
        updates: dict[str, Any],
    ) -> EdgeContractVersionRow | None: ...

    def upsert_edge_waypoint(
        self,
        *,
        waypoint_id: str | None = None,
        edge_id: str,
        ordinal: int,
        kind: str,
        location: str,
        observable: bool,
        probe_method: str = "none",
        last_seen_at: datetime | None = None,
        last_size_bytes: int | None = None,
        last_row_count: int | None = None,
        status: str = "unknown",
    ) -> EdgeWaypointRow | None: ...

    def replace_edge_waypoints(
        self,
        *,
        edge_id: str,
        waypoint_rows: tuple[dict[str, object], ...],
    ) -> list[EdgeWaypointRow]: ...

    def record_edge_analysis_event(
        self,
        *,
        event_id: str | None = None,
        edge_id: str,
        event_type: str,
        details_json: dict[str, object] | None = None,
        created_at: datetime | None = None,
    ) -> EdgeAnalysisEventRow | None: ...

    def upsert_transport_group(
        self,
        *,
        group_id: str | None = None,
        kind: str,
        group_key: str,
        display_name: str,
        metadata_json: dict[str, object] | None = None,
    ) -> TransportGroupRow | None: ...

    def replace_edge_transport_group_memberships(
        self,
        *,
        group_id: str,
        edge_ids: tuple[str, ...],
    ) -> list[EdgeTransportGroupMembershipRow]: ...

    def upsert_edge_probe_state(
        self,
        *,
        edge_id: str,
        probe_type: str,
        consecutive_failures: int = 0,
        last_success_at: datetime | None = None,
        last_failure_at: datetime | None = None,
        last_error_json: dict[str, object] | None = None,
    ) -> EdgeProbeStateRow | None: ...

    def upsert_pipeline_analysis_review(
        self,
        *,
        review_id: str | None = None,
        edge_id: str,
        target_id: str,
        source_adapter_key: str,
        source_object: str,
        dest_adapter_key: str,
        dest_object: str,
        status: str = "open",
        review_reason: str,
        candidate_confidence: float = 0.0,
        candidate_transport_json: dict[str, object] | None = None,
        candidate_waypoints_json: list[dict[str, object]] | None = None,
        evidence_json: dict[str, object] | None = None,
        retry_count: int = 0,
        reviewed_by: str | None = None,
        reviewed_at: datetime | None = None,
    ) -> PipelineAnalysisReviewRow | None: ...

    def update_pipeline_analysis_review_fields(
        self,
        *,
        review_id: str,
        updates: dict[str, Any],
    ) -> PipelineAnalysisReviewRow | None: ...


@runtime_checkable
class EdgeRepository(EdgeReader, EdgeWriter, Protocol):
    """Full edge storage."""

    ...
