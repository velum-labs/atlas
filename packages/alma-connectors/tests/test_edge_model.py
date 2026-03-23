from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from alma_connectors import (
    ColumnParity,
    CopyStrategy,
    DataEdge,
    EdgeContract,
    EdgeDiscoveryMethod,
    EdgeStatus,
    EdgeTransport,
    EdgeWaypoint,
    NullWarning,
    ProbeError,
    ProbeErrorType,
    ProbeMethod,
    ProbeState,
    ProbeSuccess,
    SerializationFormat,
    TransportGroup,
    TransportGroupKind,
    TransportKind,
    TypeCompatibility,
    TypeMapping,
    TypeWarning,
    WaypointKind,
    WaypointStatus,
    WriteDisposition,
    data_edge_from_row,
    data_edge_to_row_payload,
    edge_contract_from_json,
    edge_contract_to_json,
    edge_waypoint_from_row,
    edge_waypoint_to_row_payload,
    probe_state_from_row,
    probe_state_to_row_payload,
    transport_group_from_row,
    transport_group_to_row_payload,
)

_EDGE_ID = "00000000-0000-0000-0000-000000000111"
_WAYPOINT_ID = "00000000-0000-0000-0000-000000000112"
_GROUP_ID = "00000000-0000-0000-0000-000000000113"
_NOW = datetime(2026, 3, 18, 12, 0, tzinfo=UTC)


def _make_contract() -> EdgeContract:
    return EdgeContract(
        column_parity=ColumnParity.DIVERGED,
        source_only_columns=("new_col",),
        dest_only_columns=("legacy_col",),
        type_mappings=(
            TypeMapping(
                column_name="amount",
                source_type="numeric",
                dest_type="BIGNUMERIC",
                compatibility=TypeCompatibility.SAFE,
            ),
        ),
        type_warnings=(
            TypeWarning(
                column_name="payload",
                source_type="jsonb",
                dest_type="STRING",
                risk="JSON queryability is lost during serialization",
            ),
        ),
        null_warnings=(
            NullWarning(
                column_name="address2",
                issue="Empty strings collapse to NULL in the destination",
            ),
        ),
        freshness_sla=timedelta(hours=2),
        observed_lag=timedelta(minutes=45),
        analyzed_at=_NOW,
        schema_hash_source="src-hash",
        schema_hash_dest="dest-hash",
    )


def _make_transport() -> EdgeTransport:
    return EdgeTransport(
        kind=TransportKind.CUSTOM_SCRIPT,
        schedule=" 25 * * * * ",
        strategy=CopyStrategy.INCREMENTAL,
        write_disposition=WriteDisposition.MERGE,
        serialization_format=SerializationFormat.CSV,
        watermark_column=" updated_at ",
        select_mode=" star ",
        metadata={"airflow_dag": "dag_update_tables_hourly_incremental_copy"},
    )


def test_data_edge_normalizes_adapter_keys_and_transport_strings() -> None:
    edge = DataEdge(
        id=_EDGE_ID,
        source_adapter_key=" Monolith Core ",
        source_object="public.users",
        dest_adapter_key=" Analytics Warehouse ",
        dest_object="staging.users",
        discovery_method=EdgeDiscoveryMethod.SCHEMA_MATCH,
        confidence=0.87,
        transport=_make_transport(),
        contract=_make_contract(),
        status=EdgeStatus.VERIFIED,
        created_at=_NOW.isoformat(),
        updated_at=_NOW.isoformat(),
    )

    assert edge.source_adapter_key == "monolith-core"
    assert edge.dest_adapter_key == "analytics-warehouse"
    assert edge.transport.schedule == "25 * * * *"
    assert edge.transport.watermark_column == "updated_at"
    assert edge.transport.select_mode == "star"
    assert edge.created_at == _NOW
    assert edge.updated_at == _NOW


def test_data_edge_rejects_invalid_confidence() -> None:
    with pytest.raises(ValueError, match="confidence must be in \\[0.0, 1.0\\]"):
        DataEdge(
            id=_EDGE_ID,
            source_adapter_key="core-db",
            source_object="public.users",
            dest_adapter_key="warehouse",
            dest_object="staging.users",
            discovery_method=EdgeDiscoveryMethod.SCHEMA_MATCH,
            confidence=1.5,
        )


def test_edge_contract_json_round_trip_preserves_nested_payloads() -> None:
    contract = _make_contract()

    restored = edge_contract_from_json(edge_contract_to_json(contract))

    assert restored == contract


def test_edge_contract_from_json_defaults_missing_sequences() -> None:
    restored = edge_contract_from_json({"column_parity": "strict"})

    assert restored == EdgeContract(column_parity=ColumnParity.STRICT)


def test_edge_contract_from_json_rejects_string_column_sequences() -> None:
    with pytest.raises(ValueError, match="source_only_columns must be a sequence of strings"):
        edge_contract_from_json(
            {
                "column_parity": "strict",
                "source_only_columns": "new_col",
            }
        )


def test_data_edge_row_round_trip_preserves_transport_and_contract() -> None:
    edge = DataEdge(
        id=_EDGE_ID,
        source_adapter_key="core-db",
        source_object="public.users",
        dest_adapter_key="warehouse",
        dest_object="staging.users",
        discovery_method=EdgeDiscoveryMethod.PIPELINE_ANALYSIS,
        confidence=0.91,
        transport=_make_transport(),
        contract=_make_contract(),
        status=EdgeStatus.MONITORED,
        created_at=_NOW,
        updated_at=_NOW,
    )

    restored = data_edge_from_row(data_edge_to_row_payload(edge))

    assert restored == edge


def test_edge_waypoint_row_round_trip_preserves_observation_fields() -> None:
    waypoint = EdgeWaypoint(
        id=_WAYPOINT_ID,
        edge_id=_EDGE_ID,
        ordinal=1,
        kind=WaypointKind.STAGING_TABLE,
        location="project.dataset.users_temp",
        observable=True,
        probe_method=ProbeMethod.BQ_TABLE_META,
        last_seen_at=_NOW,
        last_size_bytes=1_024,
        last_row_count=512,
        status=WaypointStatus.HEALTHY,
    )

    restored = edge_waypoint_from_row(edge_waypoint_to_row_payload(waypoint))

    assert restored == waypoint


def test_transport_group_row_round_trip_accepts_external_membership_list() -> None:
    group = TransportGroup(
        id=_GROUP_ID,
        kind=TransportGroupKind.AIRFLOW_DAG,
        group_key="dag_update_tables_hourly_incremental_copy",
        display_name="Hourly Incremental Copy",
        edge_ids=(_EDGE_ID, _EDGE_ID),
        metadata={"airflow_dag": "dag_update_tables_hourly_incremental_copy"},
    )

    restored = transport_group_from_row(
        transport_group_to_row_payload(group),
        edge_ids=group.edge_ids,
    )

    assert restored.edge_ids == (_EDGE_ID,)
    assert restored == TransportGroup(
        id=_GROUP_ID,
        kind=TransportGroupKind.AIRFLOW_DAG,
        group_key="dag_update_tables_hourly_incremental_copy",
        display_name="Hourly Incremental Copy",
        edge_ids=(_EDGE_ID,),
        metadata={"airflow_dag": "dag_update_tables_hourly_incremental_copy"},
    )


def test_probe_state_row_round_trip_preserves_last_error() -> None:
    state = ProbeState(
        edge_id=_EDGE_ID,
        probe_type="freshness",
        consecutive_failures=2,
        last_success_at=_NOW - timedelta(hours=1),
        last_failure_at=_NOW,
        last_error=ProbeError(
            error_type=ProbeErrorType.TIMEOUT,
            message="Probe timed out after 30 seconds",
            probed_at=_NOW,
            retryable=True,
        ),
    )

    restored = probe_state_from_row(probe_state_to_row_payload(state))

    assert restored == state


def test_probe_success_rejects_negative_duration() -> None:
    with pytest.raises(ValueError, match="duration_ms must be >= 0"):
        ProbeSuccess(value=1, duration_ms=-1, probed_at=_NOW)
