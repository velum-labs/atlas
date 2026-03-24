"""Tests for MetabaseAdapter."""

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
from alma_connectors.adapters.metabase import MetabaseAdapter
from alma_connectors.source_adapter_v2 import AdapterCapability

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ADAPTER_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
_INSTANCE_URL = "https://metabase.example.com"
_METABASE_MODULE = "alma_connectors.adapters.metabase.httpx"


def _make_persisted() -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=_ADAPTER_ID,
        key="metabase-prod",
        display_name="Metabase Prod",
        kind=SourceAdapterKind.POSTGRES,  # v1 kind used as placeholder
        target_id="metabase-prod",
        status=SourceAdapterStatus.READY,
        config=ExternalSecretRef(provider="env", reference="UNUSED"),
    )


def _make_adapter_apikey() -> MetabaseAdapter:
    return MetabaseAdapter(instance_url=_INSTANCE_URL, api_key="mb_test_key")


def _make_adapter_userpass() -> MetabaseAdapter:
    return MetabaseAdapter(instance_url=_INSTANCE_URL, username="admin@example.com", password="s3cr3t")


def _mock_response(data: object, *, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = data
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_requires_instance_url() -> None:
    with pytest.raises(ValueError, match="instance_url"):
        MetabaseAdapter(instance_url="", api_key="key")


def test_constructor_requires_credentials() -> None:
    with pytest.raises(ValueError, match="api_key or both username and password"):
        MetabaseAdapter(instance_url=_INSTANCE_URL)


def test_constructor_username_without_password_raises() -> None:
    with pytest.raises(ValueError):
        MetabaseAdapter(instance_url=_INSTANCE_URL, username="user")


def test_constructor_password_without_username_raises() -> None:
    with pytest.raises(ValueError):
        MetabaseAdapter(instance_url=_INSTANCE_URL, password="pass")


def test_constructor_strips_trailing_slash() -> None:
    adapter = MetabaseAdapter(instance_url="https://mb.example.com/", api_key="k")
    assert adapter._instance_url == "https://mb.example.com"


# ---------------------------------------------------------------------------
# Auth headers — api_key path
# ---------------------------------------------------------------------------


def test_get_auth_headers_api_key() -> None:
    adapter = _make_adapter_apikey()
    headers = adapter._get_auth_headers()
    assert headers == {"x-api-key": "mb_test_key"}


def test_get_auth_headers_api_key_does_not_call_session_endpoint() -> None:
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        adapter._get_auth_headers()
        mock_httpx.post.assert_not_called()


# ---------------------------------------------------------------------------
# Auth headers — session token path
# ---------------------------------------------------------------------------


def test_get_auth_headers_username_password_fetches_token() -> None:
    adapter = _make_adapter_userpass()
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.post.return_value = _mock_response({"id": "tok_abc123"})
        headers = adapter._get_auth_headers()
    assert headers == {"X-Metabase-Session": "tok_abc123"}
    assert adapter._session_token == "tok_abc123"


def test_get_auth_headers_session_token_cached() -> None:
    adapter = _make_adapter_userpass()
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.post.return_value = _mock_response({"id": "tok_xyz"})
        adapter._get_auth_headers()
        adapter._get_auth_headers()
        assert mock_httpx.post.call_count == 1


# ---------------------------------------------------------------------------
# _api_get
# ---------------------------------------------------------------------------


def test_api_get_builds_correct_url() -> None:
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response({"data": []})
        adapter._api_get("database")
        mock_httpx.get.assert_called_once()
        call_args = mock_httpx.get.call_args
        assert call_args[0][0] == f"{_INSTANCE_URL}/api/database"


def test_api_get_raises_on_http_error() -> None:
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        resp = _mock_response({}, status_code=401)
        resp.raise_for_status.side_effect = Exception("401 Unauthorized")
        mock_httpx.get.return_value = resp
        with pytest.raises(Exception, match="401"):
            adapter._api_get("database")


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response({"id": 1, "email": "admin@example.com"})
        result = asyncio.run(adapter.test_connection(persisted))
    assert result.success is True
    assert "admin@example.com" in result.message


def test_test_connection_failure() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response({}, status_code=401)
        mock_httpx.get.return_value.raise_for_status.side_effect = Exception("Unauthorized")
        result = asyncio.run(adapter.test_connection(persisted))
    assert result.success is False
    assert "Unauthorized" in result.message


# ---------------------------------------------------------------------------
# probe
# ---------------------------------------------------------------------------


def test_probe_all_available() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        # /api/database succeeds, /api/setting fails (OSS)
        db_resp = _mock_response({"data": []})
        setting_resp = _mock_response({}, status_code=403)
        setting_resp.raise_for_status.side_effect = Exception("403")

        def get_side_effect(url, **kwargs):
            if "setting" in url:
                return setting_resp
            return db_resp

        mock_httpx.get.side_effect = get_side_effect
        results = asyncio.run(adapter.probe(persisted))

    assert len(results) == 3
    assert all(r.available for r in results)
    traffic = next(r for r in results if r.capability == AdapterCapability.TRAFFIC)
    assert traffic.fallback_used is True


def test_probe_unavailable_when_api_down() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.get.side_effect = Exception("Connection refused")
        results = asyncio.run(adapter.probe(persisted))
    assert all(not r.available for r in results)


def test_probe_subset_of_capabilities() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    caps = frozenset({AdapterCapability.DISCOVER})
    with patch(_METABASE_MODULE) as mock_httpx:
        mock_httpx.get.return_value = _mock_response({"data": []})
        results = asyncio.run(adapter.probe(persisted, capabilities=caps))
    assert len(results) == 1
    assert results[0].capability == AdapterCapability.DISCOVER


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------

_DB_LIST = {
    "data": [
        {
            "id": 1,
            "name": "Production PostgreSQL",
            "engine": "postgres",
            "is_full_sync": True,
            "is_sample": False,
        },
        {
            "id": 2,
            "name": "Analytics DWH",
            "engine": "bigquery",
            "is_full_sync": True,
            "is_sample": False,
        },
    ]
}

_COLLECTION_LIST = [
    {"id": 1, "name": "Our analytics", "slug": "our_analytics", "location": "/", "archived": False},
    {"id": 2, "name": "Archived", "slug": "archived", "location": "/", "archived": True},
]


def test_discover_returns_databases_and_collections() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/api/database" in url:
                return _mock_response(_DB_LIST)
            if "/api/collection" in url:
                return _mock_response(_COLLECTION_LIST)
            return _mock_response({})

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.discover(persisted))

    # 2 databases + 1 non-archived collection
    assert len(snapshot.containers) == 3
    types = {c.container_type for c in snapshot.containers}
    assert types == {"database", "collection"}


def test_discover_skips_archived_collections() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/api/database" in url:
                return _mock_response({"data": []})
            return _mock_response(_COLLECTION_LIST)

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.discover(persisted))

    collection_ids = [c.container_id for c in snapshot.containers]
    assert "metabase://collection/2" not in collection_ids
    assert "metabase://collection/1" in collection_ids


def test_discover_database_container_id_format() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/api/database" in url:
                return _mock_response(_DB_LIST)
            return _mock_response([])

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.discover(persisted))

    db_containers = [c for c in snapshot.containers if c.container_type == "database"]
    assert any(c.container_id == "metabase://database/1" for c in db_containers)


def test_discover_meta_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/api/database" in url:
                return _mock_response({"data": []})
            return _mock_response([])

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.discover(persisted))

    assert snapshot.meta.capability == AdapterCapability.DISCOVER
    assert snapshot.meta.adapter_kind.value == "metabase"


# ---------------------------------------------------------------------------
# extract_schema
# ---------------------------------------------------------------------------

_DB_METADATA = {
    "id": 1,
    "name": "Production PostgreSQL",
    "tables": [
        {
            "id": 12,
            "name": "orders",
            "schema": "public",
            "display_name": "Orders",
            "entity_type": "entity/TransactionTable",
            "description": "Customer orders",
            "fields": [
                {
                    "id": 101,
                    "name": "id",
                    "base_type": "type/Integer",
                    "semantic_type": "type/PK",
                    "description": None,
                },
                {
                    "id": 102,
                    "name": "user_id",
                    "base_type": "type/Integer",
                    "semantic_type": "type/FK",
                },
            ],
        },
        {
            "id": 13,
            "name": "user_view",
            "schema": "public",
            "entity_type": "entity/View",
            "fields": [],
        },
    ],
}


def test_extract_schema_maps_tables_and_columns() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "metadata" in url:
                return _mock_response(_DB_METADATA)
            if "/api/database" in url:
                return _mock_response(_DB_LIST)
            return _mock_response({})

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_schema(persisted))

    assert len(snapshot.objects) == 4  # 2 tables per 2 DBs (second DB same metadata)
    orders = next(o for o in snapshot.objects if o.object_name == "orders")
    assert orders.schema_name == "public"
    col_names = [c.name for c in orders.columns]
    assert "id" in col_names
    assert "user_id" in col_names
    assert orders.columns[0].data_type == "Integer"  # "type/" prefix stripped


def test_extract_schema_view_kind() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "metadata" in url:
                return _mock_response(_DB_METADATA)
            if "/api/database" in url:
                return _mock_response({"data": [{"id": 1, "name": "DB", "engine": "postgres"}]})
            return _mock_response({})

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_schema(persisted))

    from alma_connectors.source_adapter_v2 import SchemaObjectKind
    view = next(o for o in snapshot.objects if o.object_name == "user_view")
    assert view.kind == SchemaObjectKind.VIEW


def test_extract_schema_skips_failed_databases() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        metadata_fail = _mock_response({}, status_code=403)
        metadata_fail.raise_for_status.side_effect = Exception("403 Forbidden")

        def get_side_effect(url, **kwargs):
            if "metadata" in url:
                return metadata_fail
            if "/api/database" in url:
                return _mock_response({"data": [{"id": 1, "name": "DB", "engine": "postgres"}]})
            return _mock_response({})

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_schema(persisted))

    assert len(snapshot.objects) == 0


def test_extract_schema_meta_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "/api/database" in url:
                return _mock_response({"data": []})
            return _mock_response({})

        mock_httpx.get.side_effect = get_side_effect
        snapshot = asyncio.run(adapter.extract_schema(persisted))

    assert snapshot.meta.capability == AdapterCapability.SCHEMA


# ---------------------------------------------------------------------------
# extract_traffic — OSS activity feed
# ---------------------------------------------------------------------------

_ACTIVITY_FEED = [
    {
        "id": 1234,
        "topic": "card-query",
        "timestamp": "2024-01-15T10:23:45.123Z",
        "user": {"id": 5, "email": "analyst@example.com"},
        "model": "card",
        "model_id": 77,
        "details": {"running_time": 1250, "result_rows": 42},
    },
    {
        "id": 1235,
        "topic": "user-joined",  # non-query topic, should be skipped
        "timestamp": "2024-01-15T10:00:00.000Z",
        "user": {"id": 1, "email": "admin@example.com"},
        "details": {},
    },
]


def test_extract_traffic_oss_activity_feed() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        ee_resp = _mock_response({}, status_code=404)
        ee_resp.raise_for_status.side_effect = Exception("404")
        activity_resp = _mock_response(_ACTIVITY_FEED)

        def get_side_effect(url, **kwargs):
            if "ee/audit" in url:
                return ee_resp
            if "/api/activity" in url:
                return activity_resp
            return _mock_response({})

        mock_httpx.get.side_effect = get_side_effect
        result = asyncio.run(adapter.extract_traffic(persisted))

    # Only card-query topic should be included
    assert len(result.events) == 1
    event = result.events[0]
    assert event.event_id == "1234"
    assert event.database_user == "analyst@example.com"
    assert event.duration_ms == 1250.0
    assert event.source_name == _INSTANCE_URL


def test_extract_traffic_since_filter() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    since = datetime(2024, 1, 15, 11, 0, 0, tzinfo=UTC)  # after both events
    with patch(_METABASE_MODULE) as mock_httpx:
        ee_resp = _mock_response({}, status_code=404)
        ee_resp.raise_for_status.side_effect = Exception("404")

        def get_side_effect(url, **kwargs):
            if "ee/audit" in url:
                return ee_resp
            return _mock_response(_ACTIVITY_FEED)

        mock_httpx.get.side_effect = get_side_effect
        result = asyncio.run(adapter.extract_traffic(persisted, since=since))

    assert len(result.events) == 0


def test_extract_traffic_enterprise_endpoint_preferred() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    ee_data = {
        "data": [
            {
                "query_hash": "abc123",
                "started_at": "2024-01-15T10:23:45.123Z",
                "running_time": 500,
                "result_rows": 10,
                "native": "SELECT 1",
                "executor_id": 5,
                "card_name": "Revenue",
                "database_id": 1,
            }
        ]
    }
    with patch(_METABASE_MODULE) as mock_httpx:
        def get_side_effect(url, **kwargs):
            if "ee/audit" in url:
                return _mock_response(ee_data)
            return _mock_response(_ACTIVITY_FEED)

        mock_httpx.get.side_effect = get_side_effect
        result = asyncio.run(adapter.extract_traffic(persisted))

    assert len(result.events) == 1
    assert result.events[0].sql == "SELECT 1"
    assert result.events[0].event_id == "abc123"


def test_extract_traffic_meta_capability() -> None:
    persisted = _make_persisted()
    adapter = _make_adapter_apikey()
    with patch(_METABASE_MODULE) as mock_httpx:
        ee_resp = _mock_response({}, status_code=404)
        ee_resp.raise_for_status.side_effect = Exception("404")

        def get_side_effect(url, **kwargs):
            if "ee/audit" in url:
                return ee_resp
            return _mock_response([])

        mock_httpx.get.side_effect = get_side_effect
        result = asyncio.run(adapter.extract_traffic(persisted))

    assert result.meta.capability == AdapterCapability.TRAFFIC


# ---------------------------------------------------------------------------
# Unsupported capabilities
# ---------------------------------------------------------------------------


def test_extract_definitions_raises() -> None:
    adapter = _make_adapter_apikey()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_definitions(_make_persisted()))


def test_extract_lineage_raises() -> None:
    adapter = _make_adapter_apikey()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_lineage(_make_persisted()))


def test_extract_orchestration_raises() -> None:
    adapter = _make_adapter_apikey()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_orchestration(_make_persisted()))


def test_execute_query_raises() -> None:
    adapter = _make_adapter_apikey()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.execute_query(_make_persisted(), "SELECT 1"))


# ---------------------------------------------------------------------------
# get_setup_instructions
# ---------------------------------------------------------------------------


def test_get_setup_instructions_returns_title() -> None:
    adapter = _make_adapter_apikey()
    instructions = adapter.get_setup_instructions()
    assert "Metabase" in instructions.title
    assert len(instructions.steps) > 0
