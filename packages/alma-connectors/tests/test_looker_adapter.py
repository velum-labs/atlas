"""Tests for LookerAdapter."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

from alma_connectors.adapters.looker import LookerAdapter
from alma_connectors.source_adapter import (
    ExternalSecretRef,
    LookerAdapterConfig,
    PersistedSourceAdapter,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    LineageEdgeKind,
    SchemaObjectKind,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ADAPTER_ID = "12345678-1234-5678-1234-567812345678"
_BASE_URL = "https://mycompany.looker.com:19999/api/4.0"
_TOKEN_RESP = {"access_token": "test-token-abc", "expires_in": 3600}


def _make_persisted() -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id=_ADAPTER_ID,
        key="looker-prod",
        display_name="Looker Prod",
        kind=SourceAdapterKind.LOOKER,
        target_id="looker-prod",
        status=SourceAdapterStatus.READY,
        config=LookerAdapterConfig(
            instance_url="https://mycompany.looker.com",
            client_id=ExternalSecretRef(provider="literal", reference="cid"),
            client_secret=ExternalSecretRef(provider="literal", reference="csecret"),
        ),
    )


def _make_adapter(
    instance_url: str = "https://mycompany.looker.com",
    client_id: str = "cid",
    client_secret: str = "csecret",
    port: int = 19999,
) -> LookerAdapter:
    return LookerAdapter(
        instance_url=instance_url,
        client_id=client_id,
        client_secret=client_secret,
        port=port,
    )


def _mock_response(json_data: object, *, status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=resp,
        )
    return resp


def _make_mock_client(
    *,
    post_return=None,
    get_return=None,
    get_side_effect=None,
    post_side_effect=None,
) -> AsyncMock:
    """Create an AsyncMock httpx client with pre-configured responses."""
    mock_client = AsyncMock()
    if post_return is not None:
        mock_client.post.return_value = post_return
    if post_side_effect is not None:
        mock_client.post.side_effect = post_side_effect
    if get_return is not None:
        mock_client.get.return_value = get_return
    if get_side_effect is not None:
        mock_client.get.side_effect = get_side_effect
    return mock_client


# ---------------------------------------------------------------------------
# Constructor validation
# ---------------------------------------------------------------------------


def test_constructor_rejects_empty_instance_url() -> None:
    with pytest.raises(ValueError, match="instance_url"):
        LookerAdapter(instance_url="", client_id="x", client_secret="y")


def test_constructor_rejects_empty_client_id() -> None:
    with pytest.raises(ValueError, match="client_id"):
        LookerAdapter(instance_url="https://x.looker.com", client_id="", client_secret="y")


def test_constructor_rejects_empty_client_secret() -> None:
    with pytest.raises(ValueError, match="client_secret"):
        LookerAdapter(instance_url="https://x.looker.com", client_id="x", client_secret="")


# ---------------------------------------------------------------------------
# OAuth token flow
# ---------------------------------------------------------------------------


def test_get_access_token_posts_credentials() -> None:
    adapter = _make_adapter()
    mock_client = _make_mock_client(post_return=_mock_response(_TOKEN_RESP))
    adapter._client = mock_client

    token = asyncio.run(adapter._get_access_token())

    assert token == "test-token-abc"
    mock_client.post.assert_called_once()
    call_kwargs = mock_client.post.call_args
    assert call_kwargs[1]["data"]["client_id"] == "cid"
    assert call_kwargs[1]["data"]["client_secret"] == "csecret"
    assert "/api/4.0/login" in call_kwargs[0][0]


def test_get_access_token_caches_token() -> None:
    adapter = _make_adapter()
    mock_client = _make_mock_client(post_return=_mock_response(_TOKEN_RESP))
    adapter._client = mock_client

    asyncio.run(adapter._get_access_token())
    asyncio.run(adapter._get_access_token())  # second call — should use cache

    mock_client.post.assert_called_once()


def test_get_access_token_refreshes_when_expired() -> None:
    import time

    adapter = _make_adapter()
    mock_client = _make_mock_client(post_return=_mock_response(_TOKEN_RESP))
    adapter._client = mock_client

    asyncio.run(adapter._get_access_token())
    # Force expiry
    adapter._token_expires_at = time.monotonic() - 1
    asyncio.run(adapter._get_access_token())

    assert mock_client.post.call_count == 2


def test_api_get_uses_bearer_token() -> None:
    adapter = _make_adapter()
    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_return=_mock_response({"email": "svc@co.com"}),
    )
    adapter._client = mock_client

    result = asyncio.run(adapter._api_get("user"))

    assert result == {"email": "svc@co.com"}
    _, kwargs = mock_client.get.call_args
    assert kwargs["headers"]["Authorization"] == "token test-token-abc"


def test_api_get_refreshes_token_on_401() -> None:
    """On a 401, the adapter should clear the token cache and retry once."""
    adapter = _make_adapter()
    # Seed a stale token
    adapter._access_token = "old-token"
    adapter._token_expires_at = float("inf")

    new_token_resp = {"access_token": "new-token-xyz", "expires_in": 3600}
    success_resp = _mock_response({"email": "svc@co.com"})

    mock_client = _make_mock_client(
        post_return=_mock_response(new_token_resp),
        get_side_effect=[_mock_response({}, status_code=401), success_resp],
    )
    adapter._client = mock_client

    result = asyncio.run(adapter._api_get("user"))

    assert result == {"email": "svc@co.com"}
    mock_client.post.assert_called_once()  # token was refreshed
    assert mock_client.get.call_count == 2
    # Second call must use new token
    second_call_headers = mock_client.get.call_args_list[1][1]["headers"]
    assert second_call_headers["Authorization"] == "token new-token-xyz"


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


def test_test_connection_success() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_return=_mock_response({"email": "svc@company.com"}),
    )
    adapter._client = mock_client

    result = asyncio.run(adapter.test_connection(persisted))

    assert result.success is True
    assert "svc@company.com" in result.message


def test_test_connection_failure() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(post_side_effect=httpx.ConnectError("unreachable"))
    adapter._client = mock_client

    result = asyncio.run(adapter.test_connection(persisted))

    assert result.success is False
    assert "unreachable" in result.message


# ---------------------------------------------------------------------------
# probe
# ---------------------------------------------------------------------------

_LOOKML_MODELS_LIST = [
    {"name": "ecommerce", "project_name": "my_project", "explores": [{"name": "orders"}]},
]


def test_probe_all_capabilities_available() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response({"email": "svc@co.com"}),  # GET user
            _mock_response(_LOOKML_MODELS_LIST),        # GET lookml_models
        ],
    )
    adapter._client = mock_client

    results = asyncio.run(adapter.probe(persisted))

    assert len(results) == len(adapter.declared_capabilities)
    assert all(r.available for r in results)


def test_probe_unavailable_when_auth_fails() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(post_side_effect=httpx.ConnectError("refused"))
    adapter._client = mock_client

    results = asyncio.run(adapter.probe(persisted))

    assert all(not r.available for r in results)
    assert all("auth check failed" in (r.message or "") for r in results)


def test_probe_subset_of_capabilities() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()
    caps = frozenset({AdapterCapability.DISCOVER})

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response({"email": "svc@co.com"}),
            _mock_response(_LOOKML_MODELS_LIST),
        ],
    )
    adapter._client = mock_client

    results = asyncio.run(adapter.probe(persisted, capabilities=caps))

    assert len(results) == 1
    assert results[0].capability == AdapterCapability.DISCOVER
    assert results[0].available is True


# ---------------------------------------------------------------------------
# discover
# ---------------------------------------------------------------------------

_MODELS_RESPONSE = [
    {
        "name": "ecommerce",
        "project_name": "my_project",
        "explores": [
            {"name": "orders", "label": "Orders"},
            {"name": "users", "label": "Users"},
        ],
    },
    {
        "name": "finance",
        "project_name": "my_project",
        "explores": [{"name": "invoices", "label": ""}],
    },
]


def test_discover_builds_project_model_explore_containers() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_return=_mock_response(_MODELS_RESPONSE),
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.discover(persisted))

    ids = {c.container_id for c in snapshot.containers}
    types = {c.container_type for c in snapshot.containers}

    # One project (deduplicated), two models, three explores
    assert "looker://project/my_project" in ids
    assert "looker://model/ecommerce" in ids
    assert "looker://model/finance" in ids
    assert "looker://explore/ecommerce/orders" in ids
    assert "looker://explore/ecommerce/users" in ids
    assert "looker://explore/finance/invoices" in ids

    assert types == {"project", "model", "explore"}


def test_discover_deduplicates_projects() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_return=_mock_response(_MODELS_RESPONSE),
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.discover(persisted))

    project_containers = [c for c in snapshot.containers if c.container_type == "project"]
    assert len(project_containers) == 1


def test_discover_explore_label_fallback() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_return=_mock_response(_MODELS_RESPONSE),
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.discover(persisted))

    # "invoices" explore has empty label — should fall back to name
    invoices = next(c for c in snapshot.containers if c.container_id == "looker://explore/finance/invoices")
    assert invoices.display_name == "invoices"


def test_discover_meta() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_return=_mock_response(_MODELS_RESPONSE),
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.discover(persisted))

    assert snapshot.meta.capability == AdapterCapability.DISCOVER
    assert snapshot.meta.adapter_key == "looker-prod"
    assert snapshot.meta.row_count == len(snapshot.containers)


# ---------------------------------------------------------------------------
# extract_schema
# ---------------------------------------------------------------------------

_EXPLORE_FIELDS_RESP = {
    "fields": {
        "dimensions": [
            {"name": "orders.id", "type": "number", "description": "Order PK"},
            {"name": "orders.created_date", "type": "date", "description": None},
        ],
        "measures": [
            {"name": "orders.count", "type": "count", "description": "Total orders"},
        ],
    }
}


def test_extract_schema_maps_dimensions_and_measures() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "my_project", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),         # GET lookml_models
            _mock_response(_EXPLORE_FIELDS_RESP), # GET explore fields
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_schema(persisted))

    assert len(snapshot.objects) == 1
    obj = snapshot.objects[0]
    assert obj.schema_name == "ecommerce"
    assert obj.object_name == "orders"
    assert obj.kind == SchemaObjectKind.SEMANTIC_MODEL

    col_names = {c.name for c in obj.columns}
    assert col_names == {"orders.id", "orders.created_date", "orders.count"}


def test_extract_schema_dimension_type_and_description() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_FIELDS_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_schema(persisted))

    id_col = next(c for c in snapshot.objects[0].columns if c.name == "orders.id")
    assert id_col.data_type == "number"
    assert id_col.description == "Order PK"

    date_col = next(c for c in snapshot.objects[0].columns if c.name == "orders.created_date")
    assert date_col.description is None


def test_extract_schema_meta() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_FIELDS_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_schema(persisted))

    assert snapshot.meta.capability == AdapterCapability.SCHEMA
    assert snapshot.meta.row_count == 1


# ---------------------------------------------------------------------------
# extract_definitions
# ---------------------------------------------------------------------------

_EXPLORE_SQL_RESP = {
    "fields": {
        "dimensions": [
            {"name": "orders.id", "sql": "${TABLE}.id", "description": None},
            {"name": "orders.status", "sql": "", "description": None},  # no sql → skip
        ],
        "measures": [
            {"name": "orders.count", "sql": "COUNT(*)", "description": None},
        ],
    }
}


def test_extract_definitions_includes_sql_expressions() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_SQL_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_definitions(persisted))

    assert len(snapshot.definitions) == 1
    defn = snapshot.definitions[0]
    assert defn.schema_name == "ecommerce"
    assert defn.object_name == "orders"
    assert defn.definition_language == "lookml"
    assert defn.object_kind == SchemaObjectKind.SEMANTIC_MODEL
    assert "${TABLE}.id" in defn.definition_text
    assert "COUNT(*)" in defn.definition_text
    # Empty sql field should be skipped
    assert "orders.status" not in defn.definition_text


def test_extract_definitions_fallback_when_no_sql() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    no_sql_resp = {"fields": {"dimensions": [], "measures": []}}
    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(no_sql_resp),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_definitions(persisted))

    # definition_text must be non-empty (falls back to explore name comment)
    assert snapshot.definitions[0].definition_text.strip() != ""


def test_extract_definitions_meta() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_SQL_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_definitions(persisted))

    assert snapshot.meta.capability == AdapterCapability.DEFINITIONS


# ---------------------------------------------------------------------------
# extract_lineage
# ---------------------------------------------------------------------------

_EXPLORE_LINEAGE_RESP = {
    "name": "orders",
    "model_name": "ecommerce",
    "view_name": "orders",
    "sql_table_name": "analytics.orders",
    "joins": [
        {
            "name": "users",
            "sql_table_name": "analytics.users",
            "type": "left_outer",
        },
        {
            "name": "products",
            "sql_table_name": "",  # empty → no edge
            "type": "left_outer",
        },
    ],
}


def test_extract_lineage_primary_view_edge() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_LINEAGE_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_lineage(persisted))

    primary_edge = next(
        e for e in snapshot.edges if e.source_object == "analytics.orders"
    )
    assert primary_edge.target_object == "looker://explore/ecommerce/orders"
    assert primary_edge.edge_kind == LineageEdgeKind.DECLARED
    assert primary_edge.confidence == 0.95


def test_extract_lineage_join_edges() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_LINEAGE_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_lineage(persisted))

    sources = {e.source_object for e in snapshot.edges}
    assert "analytics.users" in sources
    # Empty sql_table_name should produce no edge
    assert "" not in sources
    # Total: primary view + users join (products has empty table)
    assert len(snapshot.edges) == 2


def test_extract_lineage_edge_kind_is_declared() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_LINEAGE_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_lineage(persisted))

    assert all(e.edge_kind == LineageEdgeKind.DECLARED for e in snapshot.edges)


def test_extract_lineage_no_sql_table_name_produces_no_edge() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    no_table_resp = {
        "name": "orders",
        "model_name": "ecommerce",
        "view_name": "orders",
        "sql_table_name": "",
        "joins": [],
    }
    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(no_table_resp),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_lineage(persisted))

    assert len(snapshot.edges) == 0


def test_extract_lineage_meta() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()

    simple_model = [
        {"name": "ecommerce", "project_name": "p", "explores": [{"name": "orders"}]},
    ]

    mock_client = _make_mock_client(
        post_return=_mock_response(_TOKEN_RESP),
        get_side_effect=[
            _mock_response(simple_model),
            _mock_response(_EXPLORE_LINEAGE_RESP),
        ],
    )
    adapter._client = mock_client

    snapshot = asyncio.run(adapter.extract_lineage(persisted))

    assert snapshot.meta.capability == AdapterCapability.LINEAGE
    assert snapshot.meta.row_count == len(snapshot.edges)


# ---------------------------------------------------------------------------
# Not-implemented stubs
# ---------------------------------------------------------------------------


def test_extract_traffic_raises() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_traffic(persisted))


def test_extract_orchestration_raises() -> None:
    adapter = _make_adapter()
    persisted = _make_persisted()
    with pytest.raises(NotImplementedError):
        asyncio.run(adapter.extract_orchestration(persisted))
