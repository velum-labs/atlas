"""Tests for the Observatory push bridge."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from alma_atlas.bridge.observatory_push import ObservatoryBridge, ObservatoryRpcError
from alma_ports.asset import Asset
from alma_ports.query import QueryObservation


def _make_assets(n: int) -> list[Asset]:
    return [
        Asset(id=f"db.schema.table_{i}", source="pg", kind="table", name=f"table_{i}")
        for i in range(n)
    ]


def _make_queries(n: int) -> list[QueryObservation]:
    return [
        QueryObservation(
            fingerprint=f"fp_{i}",
            sql_text=f"SELECT * FROM t_{i}",
            tables=[f"db.schema.t_{i}"],
            source="analytics",
            execution_count=i + 1,
        )
        for i in range(n)
    ]


def _ok_response(json_body: dict | None = None) -> httpx.Response:
    return httpx.Response(200, json=json_body or {})


def _err_response(status: int = 500, text: str = "internal error") -> httpx.Response:
    return httpx.Response(status, text=text)


class TestCallRpc:
    @pytest.mark.asyncio
    async def test_success(self) -> None:
        async with ObservatoryBridge("http://obs:8000") as bridge:
            bridge._client = AsyncMock(spec=httpx.AsyncClient)
            bridge._client.post = AsyncMock(return_value=_ok_response({"ok": True}))

            result = await bridge._call_rpc("Ping", {"hello": "world"})

            assert result == {"ok": True}
            bridge._client.post.assert_called_once_with(
                "http://obs:8000/velum.observatory.v1.ObservatoryService/Ping",
                json={"hello": "world"},
            )

    @pytest.mark.asyncio
    async def test_error_raises(self) -> None:
        async with ObservatoryBridge("http://obs:8000") as bridge:
            bridge._client = AsyncMock(spec=httpx.AsyncClient)
            bridge._client.post = AsyncMock(return_value=_err_response(400, "bad request"))

            with pytest.raises(ObservatoryRpcError, match="Ping failed.*400.*bad request"):
                await bridge._call_rpc("Ping", {})

    @pytest.mark.asyncio
    async def test_auth_header_set(self) -> None:
        bridge = ObservatoryBridge("http://obs:8000", auth_token="tok-123")
        assert bridge._client.headers["Authorization"] == "Bearer tok-123"
        await bridge.aclose()

    @pytest.mark.asyncio
    async def test_trailing_slash_stripped(self) -> None:
        async with ObservatoryBridge("http://obs:8000/") as bridge:
            bridge._client = AsyncMock(spec=httpx.AsyncClient)
            bridge._client.post = AsyncMock(return_value=_ok_response())

            await bridge._call_rpc("Test", {})

            url = bridge._client.post.call_args[0][0]
            assert url.startswith("http://obs:8000/velum.")


class TestUpsertAssets:
    @pytest.mark.asyncio
    async def test_upserts_all_assets(self) -> None:
        assets = _make_assets(3)
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.AssetRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = assets

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                count = await bridge.upsert_assets_from_db(db, target_id="t1")

        assert count == 3
        assert bridge._call_rpc.call_count == 3
        for i, call in enumerate(bridge._call_rpc.call_args_list):
            assert call[0][0] == "UpsertAsset"
            payload = call[0][1]
            assert "asset" in payload
            assert payload["asset"]["canonicalName"] == f"db.schema.table_{i}"
            assert payload["asset"]["layer"] == "raw"
            assert payload["asset"]["contractStatus"] == "unobserved"
            assert payload["asset"]["sourceProvenance"] == "atlas"
            assert payload["asset"]["physicalNames"][0]["targetId"] == "t1"

    @pytest.mark.asyncio
    async def test_zero_assets(self) -> None:
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.AssetRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = []

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                count = await bridge.upsert_assets_from_db(db, target_id="t1")

        assert count == 0
        bridge._call_rpc.assert_not_called()


class TestIngestQueries:
    @pytest.mark.asyncio
    async def test_ingests_all_queries(self) -> None:
        queries = _make_queries(3)
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.QueryRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = queries

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                count = await bridge.ingest_queries_from_db(db, target_id="t1", backend_system="pg")

        assert count == 3
        bridge._call_rpc.assert_called_once()
        method, payload = bridge._call_rpc.call_args[0]
        assert method == "Ingest"
        assert "batch" in payload
        assert payload["batch"]["ingestSourceId"] == "atlas"
        assert payload["batch"]["ingestSourceKind"] == "atlas_scan"
        assert payload["batch"]["targetId"] == "t1"
        assert payload["batch"]["backendSystem"] == "pg"
        assert len(payload["batch"]["events"]) == 3

    @pytest.mark.asyncio
    async def test_batching(self) -> None:
        queries = _make_queries(5)
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.QueryRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = queries

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                count = await bridge.ingest_queries_from_db(db, target_id="t1", backend_system="pg", batch_size=2)

        assert count == 5
        assert bridge._call_rpc.call_count == 3
        batch_sizes = [len(c[0][1]["batch"]["events"]) for c in bridge._call_rpc.call_args_list]
        assert batch_sizes == [2, 2, 1]

    @pytest.mark.asyncio
    async def test_zero_queries(self) -> None:
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.QueryRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = []

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                count = await bridge.ingest_queries_from_db(db, target_id="t1", backend_system="pg")

        assert count == 0
        bridge._call_rpc.assert_not_called()

    @pytest.mark.asyncio
    async def test_event_fields(self) -> None:
        queries = _make_queries(1)
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.QueryRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = queries

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                await bridge.ingest_queries_from_db(db, target_id="t1", backend_system="pg")

        event = bridge._call_rpc.call_args[0][1]["batch"]["events"][0]
        assert event["id"] == "fp_0"
        assert event["sql"] == "SELECT * FROM t_0"
        assert event["sourceName"] == "analytics"
        assert event["targetId"] == "t1"
        assert event["backendSystem"] == "pg"
        assert event["fingerprintHash"] == "fp_0"
        assert event["captureSourceId"] == "atlas"
        assert event["captureSourceKind"] == "atlas_scan"
        assert "capturedAt" in event
        # execution_count=1 -> no metadata.executionCount
        assert "executionCount" not in event.get("metadata", {})
        assert event["metadata"]["tables"] == "db.schema.t_0"

    @pytest.mark.asyncio
    async def test_execution_count_in_metadata(self) -> None:
        queries = [
            QueryObservation(
                fingerprint="fp_x",
                sql_text="SELECT 1",
                tables=[],
                source="src",
                execution_count=42,
            )
        ]
        db = MagicMock()

        with patch("alma_atlas.bridge.observatory_push.QueryRepository") as MockRepo:
            MockRepo.return_value.list_all.return_value = queries

            async with ObservatoryBridge("http://obs:8000") as bridge:
                bridge._call_rpc = AsyncMock(return_value={})
                await bridge.ingest_queries_from_db(db, target_id="t1", backend_system="pg")

        event = bridge._call_rpc.call_args[0][1]["batch"]["events"][0]
        assert event["metadata"]["executionCount"] == "42"
        assert "tables" not in event.get("metadata", {})


class TestAnalyzeAndDerive:
    @pytest.mark.asyncio
    async def test_calls_analyze_then_derive(self) -> None:
        async with ObservatoryBridge("http://obs:8000") as bridge:
            bridge._call_rpc = AsyncMock(side_effect=[{"analysisRunId": "run-1"}, {"proposals": []}])

            result = await bridge.analyze_and_derive(min_cluster_size=20)

        assert bridge._call_rpc.call_count == 2

        analyze_call = bridge._call_rpc.call_args_list[0]
        assert analyze_call[0][0] == "Analyze"
        assert analyze_call[0][1]["minClusterSize"] == 20

        derive_call = bridge._call_rpc.call_args_list[1]
        assert derive_call[0][0] == "DeriveProposals"
        assert derive_call[0][1] == {"analysisRunId": "run-1", "dryRun": False}

        assert result == {"analyze": {"analysisRunId": "run-1"}, "derive": {"proposals": []}}

    @pytest.mark.asyncio
    async def test_support_threshold_passed(self) -> None:
        async with ObservatoryBridge("http://obs:8000") as bridge:
            bridge._call_rpc = AsyncMock(side_effect=[{"analysisRunId": "run-1"}, {}])
            await bridge.analyze_and_derive(support_threshold=0.5)

        payload = bridge._call_rpc.call_args_list[0][0][1]
        assert payload["supportThreshold"] == 0.5

    @pytest.mark.asyncio
    async def test_missing_analysis_run_id_raises(self) -> None:
        async with ObservatoryBridge("http://obs:8000") as bridge:
            bridge._call_rpc = AsyncMock(return_value={})
            with pytest.raises(ObservatoryRpcError, match="analysisRunId"):
                await bridge.analyze_and_derive()
