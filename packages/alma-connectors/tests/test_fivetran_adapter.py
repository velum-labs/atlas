"""Tests for FivetranAdapter."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from alma_connectors import (
    ExternalSecretRef,
    PersistedSourceAdapter,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.adapters.fivetran import FivetranAdapter
from alma_connectors.source_adapter_v2 import AdapterCapability, LineageEdgeKind

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ADAPTER_ID = "ffffffff-eeee-dddd-cccc-bbbbbbbbbbbb"
_FIVETRAN_MODULE = "alma_connectors.adapters.fivetran.httpx"


def _make_persisted() -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=_ADAPTER_ID,
        key="fivetran-prod",
        display_name="Fivetran Prod",
        kind=SourceAdapterKind.POSTGRES,  # v1 kind used as placeholder
        target_id="fivetran-prod",
        status=SourceAdapterStatus.READY,
        config=ExternalSecretRef(provider="env", reference="UNUSED"),
    )


def _make_adapter() -> FivetranAdapter:
    return FivetranAdapter(api_key="key123", api_secret="secret456")


def _mock_response(data: object, *, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


# Sample connector list response (single page, no cursor)
_CONNECTORS_RESPONSE = {
    "data": {
        "items": [
            {
                "id": "iodize_impressive",
                "service": "postgres",
                "schema": "production_pg",
                "group_id": "projected_morning",
                "status": {"sync_state": "scheduled", "setup_state": "connected"},
                "succeeded_at": "2024-01-15T10:00:00.000Z",
                "sync_frequency": 360,
                "schedule_type": "auto",
            },
            {
                "id": "singing_modular",
                "service": "salesforce",
                "schema": "salesforce_crm",
                "group_id": "projected_morning",
                "status": {"sync_state": "paused", "setup_state": "connected"},
                "succeeded_at": None,
                "sync_frequency": 1440,
                "schedule_type": "manual",
            },
        ],
        "next_cursor": None,
    }
}

_SCHEMAS_RESPONSE = {
    "data": {
        "schemas": {
            "public": {
                "name_in_destination": "public",
                "enabled": True,
                "tables": {
                    "orders": {
                        "name_in_destination": "orders",
                        "enabled": True,
                    },
                    "users": {
                        "name_in_destination": "users",
                        "enabled": True,
                    },
                    "internal_audit": {
                        "name_in_destination": "internal_audit",
                        "enabled": False,  # disabled — should be skipped
                    },
                },
            }
        }
    }
}

_CONNECTOR_DETAIL_RESPONSE = {
    "data": {
        "id": "iodize_impressive",
        "service": "postgres",
        "schema": "production_pg",
        "group_id": "projected_morning",
        "sync_frequency": 360,
        "schedule_type": "auto",
        "succeeded_at": "2024-01-15T10:00:00.000Z",
        "status": {"sync_state": "scheduled"},
    }
}


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_requires_api_key() -> None:
    with pytest.raises(ValueError, match="api_key"):
        FivetranAdapter(api_key="", api_secret="s")


def test_constructor_requires_api_secret() -> None:
    with pytest.raises(ValueError, match="api_secret"):
        FivetranAdapter(api_key="k", api_secret="")


def test_constructor_strips_trailing_slash_from_base() -> None:
    adapter = FivetranAdapter(api_key="k", api_secret="s", api_base="https://api.fivetran.com/")
    assert adapter._api_base == "https://api.fivetran.com"


# ---------------------------------------------------------------------------
# _api_get — basic auth
# ---------------------------------------------------------------------------


def test_api_get_uses_basic_auth() -> None:
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response({"data": {}})
        adapter._api_get("v1/connectors")
        _, kwargs = mock_httpx.get.call_args
        assert kwargs["auth"] == ("key123", "secret456")


def test_api_get_raises_on_http_error() -> None:
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        resp = _mock_response({}, status_code=401)
        resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        mock_httpx.get.return_value = resp
        with pytest.raises(Exception, match="401"):
            adapter._api_get("v1/connectors")


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(
            {"data": {"account_name": "Acme Corp"}}
        )
        result = asyncio.run(adapter.test_connection(persisted))
    assert result.success is True
    assert "Acme Corp" in result.message


def test_test_connection_failure() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.side_effect = Exception("Connection refused")
        result = asyncio.run(adapter.test_connection(persisted))
    assert result.success is False
    assert "Connection refused" in result.message


# ---------------------------------------------------------------------------
# probe
# ---------------------------------------------------------------------------


def test_probe_returns_one_result_per_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        results = asyncio.run(adapter.probe(persisted))
    assert len(results) == 3
    caps = {r.capability for r in results}
    assert caps == {AdapterCapability.DISCOVER, AdapterCapability.LINEAGE, AdapterCapability.ORCHESTRATION}


def test_probe_unavailable_when_api_down() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.side_effect = Exception("timeout")
        results = asyncio.run(adapter.probe(persisted))
    assert all(not r.available for r in results)


def test_probe_subset_capabilities() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    caps = frozenset({AdapterCapability.LINEAGE})
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        results = asyncio.run(adapter.probe(persisted, capabilities=caps))
    assert len(results) == 1
    assert results[0].capability == AdapterCapability.LINEAGE


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------


def test_discover_returns_one_container_per_connector() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        snapshot = asyncio.run(adapter.discover(persisted))
    assert len(snapshot.containers) == 2


def test_discover_container_id_format() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        snapshot = asyncio.run(adapter.discover(persisted))
    ids = {c.container_id for c in snapshot.containers}
    assert "fivetran://connector/iodize_impressive" in ids
    assert "fivetran://connector/singing_modular" in ids


def test_discover_container_type_is_connector() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        snapshot = asyncio.run(adapter.discover(persisted))
    assert all(c.container_type == "connector" for c in snapshot.containers)


def test_discover_display_name_uses_schema() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        snapshot = asyncio.run(adapter.discover(persisted))
    names = {c.display_name for c in snapshot.containers}
    assert "production_pg" in names
    assert "salesforce_crm" in names


def test_discover_metadata_includes_service() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(_CONNECTORS_RESPONSE)
        snapshot = asyncio.run(adapter.discover(persisted))
    pg = next(c for c in snapshot.containers if c.display_name == "production_pg")
    assert pg.metadata["service"] == "postgres"


def test_discover_paginates_when_cursor_present() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    page1 = {
        "data": {
            "items": [{"id": "c1", "service": "postgres", "schema": "s1", "group_id": "g", "status": {}}],
            "next_cursor": "cursor_abc",
        }
    }
    page2 = {
        "data": {
            "items": [{"id": "c2", "service": "mysql", "schema": "s2", "group_id": "g", "status": {}}],
            "next_cursor": None,
        }
    }
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.side_effect = [_mock_response(page1), _mock_response(page2)]
        snapshot = asyncio.run(adapter.discover(persisted))
    assert len(snapshot.containers) == 2


def test_discover_meta_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(
            {"data": {"items": [], "next_cursor": None}}
        )
        snapshot = asyncio.run(adapter.discover(persisted))
    assert snapshot.meta.capability == AdapterCapability.DISCOVER


# ---------------------------------------------------------------------------
# extract_lineage
# ---------------------------------------------------------------------------


def test_extract_lineage_maps_source_to_destination() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "schemas" in url:
                return _mock_response(_SCHEMAS_RESPONSE)
            return _mock_response(_CONNECTORS_RESPONSE)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_lineage(persisted))

    # 2 enabled tables × 2 connectors (same mock applies to both)
    assert len(snapshot.edges) == 4


def test_extract_lineage_edge_kinds() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "schemas" in url:
                return _mock_response(_SCHEMAS_RESPONSE)
            return _mock_response(_CONNECTORS_RESPONSE)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_lineage(persisted))

    assert all(e.edge_kind == LineageEdgeKind.CONNECTOR_API for e in snapshot.edges)
    assert all(e.confidence == 1.0 for e in snapshot.edges)


def test_extract_lineage_skips_disabled_tables() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    single_connector = {
        "data": {
            "items": [_CONNECTORS_RESPONSE["data"]["items"][0]],
            "next_cursor": None,
        }
    }
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "schemas" in url:
                return _mock_response(_SCHEMAS_RESPONSE)
            return _mock_response(single_connector)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_lineage(persisted))

    table_names = [e.source_object for e in snapshot.edges]
    assert not any("internal_audit" in t for t in table_names)


def test_extract_lineage_skips_connector_on_error() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        schemas_fail = _mock_response({}, status_code=404)
        schemas_fail.raise_for_status.side_effect = Exception("404")

        def get_side_effect(url, **kwargs):
            if "schemas" in url:
                return schemas_fail
            return _mock_response(_CONNECTORS_RESPONSE)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_lineage(persisted))

    assert len(snapshot.edges) == 0


def test_extract_lineage_meta_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(
            {"data": {"items": [], "next_cursor": None}}
        )
        snapshot = asyncio.run(adapter.extract_lineage(persisted))
    assert snapshot.meta.capability == AdapterCapability.LINEAGE


# ---------------------------------------------------------------------------
# extract_orchestration
# ---------------------------------------------------------------------------


def test_extract_orchestration_maps_sync_schedule() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if url.endswith("/iodize_impressive") or "/connectors/iodize_impressive" in url:
                return _mock_response(_CONNECTOR_DETAIL_RESPONSE)
            if url.endswith("/singing_modular") or "/connectors/singing_modular" in url:
                return _mock_response({
                    "data": {
                        "id": "singing_modular",
                        "service": "salesforce",
                        "schema": "salesforce_crm",
                        "group_id": "g",
                        "sync_frequency": 1440,
                        "schedule_type": "manual",
                        "succeeded_at": None,
                        "status": {"sync_state": "paused"},
                    }
                })
            return _mock_response(_CONNECTORS_RESPONSE)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_orchestration(persisted))

    assert len(snapshot.units) == 2
    pg_unit = next(u for u in snapshot.units if "iodize_impressive" in u.unit_id)
    assert pg_unit.schedule == "every_360min"
    assert pg_unit.last_run_at == datetime(2024, 1, 15, 10, 0, 0, tzinfo=UTC)


def test_extract_orchestration_unit_type() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/connectors/" in url and not url.endswith("/connectors"):
                return _mock_response(_CONNECTOR_DETAIL_RESPONSE)
            return _mock_response(_CONNECTORS_RESPONSE)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_orchestration(persisted))

    assert all(u.unit_type == "connector_sync" for u in snapshot.units)


def test_extract_orchestration_last_run_status() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/connectors/" in url and not url.endswith("/connectors"):
                return _mock_response(_CONNECTOR_DETAIL_RESPONSE)
            return _mock_response(_CONNECTORS_RESPONSE)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_orchestration(persisted))

    pg_unit = next(u for u in snapshot.units if "iodize_impressive" in u.unit_id)
    assert pg_unit.last_run_status == "scheduled"


def test_extract_orchestration_meta_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter()
    with patch(_FIVETRAN_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response(
            {"data": {"items": [], "next_cursor": None}}
        )
        snapshot = asyncio.run(adapter.extract_orchestration(persisted))
    assert snapshot.meta.capability == AdapterCapability.ORCHESTRATION


# ---------------------------------------------------------------------------
# Unsupported capabilities
# ---------------------------------------------------------------------------


def test_extract_schema_raises() -> None:
    adapter = _make_adapter()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_schema(_make_persisted()))


def test_extract_definitions_raises() -> None:
    adapter = _make_adapter()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_definitions(_make_persisted()))


def test_extract_traffic_raises() -> None:
    adapter = _make_adapter()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_traffic(_make_persisted()))


def test_execute_query_raises() -> None:
    adapter = _make_adapter()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.execute_query(_make_persisted(), "SELECT 1"))


# ---------------------------------------------------------------------------
# get_setup_instructions
# ---------------------------------------------------------------------------


def test_get_setup_instructions_returns_title() -> None:
    adapter = _make_adapter()
    instructions = adapter.get_setup_instructions()
    assert "Fivetran" in instructions.title
    assert len(instructions.steps) > 0
