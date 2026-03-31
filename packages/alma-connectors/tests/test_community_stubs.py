"""Smoke tests for community connector stubs — Airflow, Looker, Metabase.

These tests verify that:
  - Each adapter module imports without error.
  - Each adapter instantiates with valid credentials.
  - declared_capabilities contains the expected AdapterCapability values.
  - Unimplemented extraction methods raise NotImplementedError (not some
    other error like AttributeError or ImportError).
  - Constructor validation rejects missing/empty credentials.
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from alma_connectors.adapters.airflow import AirflowAdapter
from alma_connectors.adapters.looker import LookerAdapter
from alma_connectors.adapters.metabase import MetabaseAdapter
from alma_connectors.source_adapter import (
    AirflowAdapterConfig,
    ExternalSecretRef,
    PersistedSourceAdapter,
    SourceAdapterKind,
    SourceAdapterStatus,
)
from alma_connectors.source_adapter_v2 import AdapterCapability

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_FAKE_SECRET = ExternalSecretRef(provider="vault", reference="path/to/secret")


def _fake_persisted(key: str = "test-adapter") -> PersistedSourceAdapter:
    """Return a minimal PersistedSourceAdapter for use in stub tests."""
    return PersistedSourceAdapter(
        id=str(uuid4()),
        key=key,
        display_name="Test Adapter",
        kind=SourceAdapterKind.AIRFLOW,
        target_id="target-stub",
        config=AirflowAdapterConfig(
            base_url="https://airflow.example.com",
            auth_token_secret=ExternalSecretRef(provider="literal", reference="tok-abc"),
        ),
        status=SourceAdapterStatus.READY,
    )


# ---------------------------------------------------------------------------
# AirflowAdapter — import and instantiation
# ---------------------------------------------------------------------------


class TestAirflowAdapterStub:
    def test_import(self) -> None:
        assert AirflowAdapter is not None

    def test_instantiate_with_token(self) -> None:
        adapter = AirflowAdapter(
            base_url="https://airflow.example.com",
            auth_token="tok-abc",
        )
        assert adapter is not None

    def test_instantiate_with_basic_auth(self) -> None:
        adapter = AirflowAdapter(
            base_url="https://airflow.example.com",
            username="admin",
            password="s3cr3t",
        )
        assert adapter is not None

    def test_declared_capabilities(self) -> None:
        adapter = AirflowAdapter(
            base_url="https://airflow.example.com",
            auth_token="tok",
        )
        assert AdapterCapability.DISCOVER in adapter.declared_capabilities
        assert AdapterCapability.TRAFFIC in adapter.declared_capabilities
        assert AdapterCapability.LINEAGE in adapter.declared_capabilities
        assert AdapterCapability.ORCHESTRATION in adapter.declared_capabilities
        # SCHEMA and DEFINITIONS are intentionally absent
        assert AdapterCapability.SCHEMA not in adapter.declared_capabilities
        assert AdapterCapability.DEFINITIONS not in adapter.declared_capabilities

    def test_missing_credentials_raises(self) -> None:
        with pytest.raises(ValueError, match="auth_token or both username and password"):
            AirflowAdapter(base_url="https://airflow.example.com")

    def test_empty_base_url_raises(self) -> None:
        with pytest.raises(ValueError, match="base_url"):
            AirflowAdapter(base_url="", auth_token="tok")

    def test_extract_schema_raises_not_implemented(self) -> None:
        adapter = AirflowAdapter(
            base_url="https://airflow.example.com",
            auth_token="tok",
        )
        with pytest.raises(NotImplementedError):
            asyncio.run(adapter.extract_schema(_fake_persisted("airflow-test")))

    def test_get_setup_instructions(self) -> None:
        adapter = AirflowAdapter(
            base_url="https://airflow.example.com",
            auth_token="tok",
        )
        instructions = adapter.get_setup_instructions()
        assert instructions.title
        assert instructions.summary
        assert len(instructions.steps) > 0


# ---------------------------------------------------------------------------
# LookerAdapter — import and instantiation
# ---------------------------------------------------------------------------


class TestLookerAdapterStub:
    def test_import(self) -> None:
        assert LookerAdapter is not None

    def test_instantiate(self) -> None:
        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        assert adapter is not None

    def test_declared_capabilities(self) -> None:
        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        assert AdapterCapability.DISCOVER in adapter.declared_capabilities
        assert AdapterCapability.SCHEMA in adapter.declared_capabilities
        assert AdapterCapability.DEFINITIONS in adapter.declared_capabilities
        assert AdapterCapability.LINEAGE in adapter.declared_capabilities
        # TRAFFIC and ORCHESTRATION are intentionally absent
        assert AdapterCapability.TRAFFIC not in adapter.declared_capabilities
        assert AdapterCapability.ORCHESTRATION not in adapter.declared_capabilities

    def test_empty_instance_url_raises(self) -> None:
        with pytest.raises(ValueError, match="instance_url"):
            LookerAdapter(instance_url="", client_id="cid", client_secret="csec")

    def test_empty_client_id_raises(self) -> None:
        with pytest.raises(ValueError, match="client_id"):
            LookerAdapter(instance_url="https://myco.looker.com", client_id="", client_secret="csec")

    def test_empty_client_secret_raises(self) -> None:
        with pytest.raises(ValueError, match="client_secret"):
            LookerAdapter(instance_url="https://myco.looker.com", client_id="cid", client_secret="")

    def test_discover_is_implemented(self) -> None:
        """DISCOVER is declared and implemented — verify it returns a snapshot."""
        import time
        from unittest.mock import AsyncMock, MagicMock

        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        mock_client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = []
        mock_client.get.return_value = resp
        adapter._client = mock_client
        adapter._access_token = "test-token"
        adapter._token_expires_at = time.time() + 3600

        snapshot = asyncio.run(adapter.discover(_fake_persisted("looker-test")))
        assert snapshot is not None

    def test_extract_schema_is_implemented(self) -> None:
        """SCHEMA is declared and implemented — verify it returns a snapshot."""
        import time
        from unittest.mock import AsyncMock, MagicMock

        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        mock_client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = []
        mock_client.get.return_value = resp
        adapter._client = mock_client
        adapter._access_token = "test-token"
        adapter._token_expires_at = time.time() + 3600

        snapshot = asyncio.run(adapter.extract_schema(_fake_persisted("looker-test")))
        assert snapshot is not None

    def test_extract_definitions_is_implemented(self) -> None:
        """DEFINITIONS is declared and implemented — verify it returns a snapshot."""
        import time
        from unittest.mock import AsyncMock, MagicMock

        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        mock_client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = []
        mock_client.get.return_value = resp
        adapter._client = mock_client
        adapter._access_token = "test-token"
        adapter._token_expires_at = time.time() + 3600

        snapshot = asyncio.run(adapter.extract_definitions(_fake_persisted("looker-test")))
        assert snapshot is not None

    def test_extract_lineage_is_implemented(self) -> None:
        """LINEAGE is declared and implemented — verify it returns a snapshot."""
        import time
        from unittest.mock import AsyncMock, MagicMock

        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        mock_client = AsyncMock()
        resp = MagicMock()
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = []
        mock_client.get.return_value = resp
        adapter._client = mock_client
        adapter._access_token = "test-token"
        adapter._token_expires_at = time.time() + 3600

        snapshot = asyncio.run(adapter.extract_lineage(_fake_persisted("looker-test")))
        assert snapshot is not None

    def test_extract_traffic_raises_not_implemented(self) -> None:
        """TRAFFIC is not declared — must still raise NotImplementedError, not AttributeError."""
        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        with pytest.raises(NotImplementedError):
            asyncio.run(adapter.extract_traffic(_fake_persisted("looker-test")))

    def test_get_setup_instructions(self) -> None:
        adapter = LookerAdapter(
            instance_url="https://myco.looker.com",
            client_id="cid",
            client_secret="csec",
        )
        instructions = adapter.get_setup_instructions()
        assert instructions.title
        assert instructions.summary
        assert len(instructions.steps) > 0


# ---------------------------------------------------------------------------
# MetabaseAdapter — import and instantiation
# ---------------------------------------------------------------------------


class TestMetabaseAdapterStub:
    def test_import(self) -> None:
        assert MetabaseAdapter is not None

    def test_instantiate_with_api_key(self) -> None:
        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc123",
        )
        assert adapter is not None

    def test_instantiate_with_username_password(self) -> None:
        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            username="admin@example.com",
            password="s3cr3t",
        )
        assert adapter is not None

    def test_declared_capabilities(self) -> None:
        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc",
        )
        assert AdapterCapability.DISCOVER in adapter.declared_capabilities
        assert AdapterCapability.SCHEMA in adapter.declared_capabilities
        assert AdapterCapability.TRAFFIC in adapter.declared_capabilities
        # DEFINITIONS, LINEAGE, ORCHESTRATION are intentionally absent
        assert AdapterCapability.DEFINITIONS not in adapter.declared_capabilities
        assert AdapterCapability.LINEAGE not in adapter.declared_capabilities
        assert AdapterCapability.ORCHESTRATION not in adapter.declared_capabilities

    def test_missing_credentials_raises(self) -> None:
        with pytest.raises(ValueError, match="api_key or both username and password"):
            MetabaseAdapter(instance_url="https://metabase.example.com")

    def test_empty_instance_url_raises(self) -> None:
        with pytest.raises(ValueError, match="instance_url"):
            MetabaseAdapter(instance_url="", api_key="mb_abc")

    def test_discover_is_implemented(self) -> None:
        """DISCOVER is declared and implemented — verify it returns a snapshot."""
        from unittest.mock import AsyncMock, MagicMock

        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc",
        )

        def _get_side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/api/database" in url:
                resp.json.return_value = {"data": []}
            else:
                resp.json.return_value = []
            return resp

        mock_client = AsyncMock()
        mock_client.get.side_effect = _get_side_effect
        adapter._client = mock_client
        adapter._session_token = "test-session"

        snapshot = asyncio.run(adapter.discover(_fake_persisted("metabase-test")))
        assert snapshot is not None

    def test_extract_schema_is_implemented(self) -> None:
        """SCHEMA is declared and implemented — verify it returns a snapshot."""
        from unittest.mock import AsyncMock, MagicMock

        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc",
        )
        mock_client = AsyncMock()
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"data": []}
        mock_client.get.return_value = resp
        adapter._client = mock_client
        adapter._session_token = "test-session"

        snapshot = asyncio.run(adapter.extract_schema(_fake_persisted("metabase-test")))
        assert snapshot is not None

    def test_extract_traffic_is_implemented(self) -> None:
        """TRAFFIC is declared and implemented — verify it returns a result."""
        from unittest.mock import AsyncMock, MagicMock

        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc",
        )

        def _get_side_effect(url, **kwargs):
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "ee/audit" in url:
                resp.raise_for_status.side_effect = Exception("404")
            resp.json.return_value = []
            return resp

        mock_client = AsyncMock()
        mock_client.get.side_effect = _get_side_effect
        adapter._client = mock_client
        adapter._session_token = "test-session"

        result = asyncio.run(adapter.extract_traffic(_fake_persisted("metabase-test")))
        assert result is not None

    def test_extract_lineage_raises_not_implemented(self) -> None:
        """LINEAGE is not declared — must still raise NotImplementedError."""
        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc",
        )
        with pytest.raises(NotImplementedError):
            asyncio.run(adapter.extract_lineage(_fake_persisted("metabase-test")))

    def test_get_setup_instructions(self) -> None:
        adapter = MetabaseAdapter(
            instance_url="https://metabase.example.com",
            api_key="mb_abc",
        )
        instructions = adapter.get_setup_instructions()
        assert instructions.title
        assert instructions.summary
        assert len(instructions.steps) > 0


# ---------------------------------------------------------------------------
# Cross-adapter — adapters __init__ exports
# ---------------------------------------------------------------------------


def test_adapters_init_exports() -> None:
    from alma_connectors.adapters.airflow import AirflowAdapter
    from alma_connectors.adapters.looker import LookerAdapter
    from alma_connectors.adapters.metabase import MetabaseAdapter

    assert AirflowAdapter is not None
    assert LookerAdapter is not None
    assert MetabaseAdapter is not None
