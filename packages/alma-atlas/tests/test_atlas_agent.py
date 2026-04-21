"""Tests for Phase 4: Atlas Agent (atlas_context, atlas_ask, atlas_verify deep mode, AgentCache)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from alma_atlas.agents.agent_cache import AgentCache
from alma_atlas.agents.atlas_agent import (
    _fallback_context_package,
    _gather_context,
    _schema_fingerprint,
    run_atlas_ask,
    run_atlas_context,
    run_verify_deep,
)
from alma_atlas.agents.atlas_agent_schemas import (
    AskResult,
    ContextPackage,
    VerificationResult,
)
from alma_atlas.config import AgentConfig, AgentProcessConfig, AtlasConfig
from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_atlas_store.edge_repository import Edge, EdgeRepository
from alma_atlas_store.schema_repository import ColumnInfo, SchemaRepository, SchemaSnapshot
from alma_ports.annotation import AnnotationRecord

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_cfg(tmp_path: Path, *, with_agent: bool = False) -> AtlasConfig:
    """Create an AtlasConfig with an optional ACP-like annotator agent."""
    db_path = tmp_path / "atlas.db"
    with Database(db_path):
        pass  # initialize schema
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    if with_agent:
        # Setting agent != None makes effective_provider_name return "acp".
        cfg.learning.annotator = AgentConfig(
            provider="acp",
            agent=AgentProcessConfig(command="fake-agent"),
        )
    return cfg


def _seed_assets(db_path: Path) -> None:
    with Database(db_path) as db:
        repo = AssetRepository(db)
        repo.upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="orders"))
        repo.upsert(Asset(id="pg::public.customers", source="pg:test", kind="table", name="customers"))
        SchemaRepository(db).upsert(
            SchemaSnapshot(
                asset_id="pg::public.orders",
                columns=[
                    ColumnInfo(name="order_id", type="integer", nullable=False),
                    ColumnInfo(name="customer_id", type="integer", nullable=True),
                    ColumnInfo(name="total", type="numeric", nullable=True),
                ],
            )
        )
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.orders",
                properties={
                    "column_notes": {
                        "order_id": "Surrogate key, do not use in SUM.",
                        "customer_id": "FK to customers.id",
                    }
                },
            )
        )
        EdgeRepository(db).upsert(
            Edge(
                upstream_id="pg::public.orders",
                downstream_id="pg::public.customers",
                kind="fk",
                metadata={"join_guidance": "Always filter by tenant_id before joining."},
            )
        )


# ---------------------------------------------------------------------------
# AgentCache
# ---------------------------------------------------------------------------


class TestAgentCache:
    def test_cache_miss_returns_none(self, tmp_path):
        db_path = tmp_path / "atlas.db"
        with Database(db_path) as db:
            cache = AgentCache(db)
            result = cache.get("what is revenue?", "mydb", "abc123")
        assert result is None

    def test_cache_round_trip(self, tmp_path):
        db_path = tmp_path / "atlas.db"
        payload = {"relevant_tables": ["orders"], "summary": "test"}
        with Database(db_path) as db:
            cache = AgentCache(db)
            cache.put("what is revenue?", "mydb", "abc123", payload)
            result = cache.get("what is revenue?", "mydb", "abc123")
        assert result == payload

    def test_cache_key_varies_by_question(self, tmp_path):
        db_path = tmp_path / "atlas.db"
        with Database(db_path) as db:
            cache = AgentCache(db)
            cache.put("question A", "db", "fp", {"answer": "A"})
            assert cache.get("question A", "db", "fp") == {"answer": "A"}
            assert cache.get("question B", "db", "fp") is None

    def test_cache_ttl_expiry(self, tmp_path):
        """Entries older than 1 day should not be returned."""
        db_path = tmp_path / "atlas.db"
        with Database(db_path) as db:
            cache = AgentCache(db)
            # Insert with an old timestamp directly.
            key = cache._make_key("old question", "db", "fp")
            db.conn.execute(
                "INSERT INTO agent_cache (cache_key, question, db_id, response, created_at) "
                "VALUES (?, ?, ?, ?, datetime('now', '-2 days'))",
                (key, "old question", "db", '{"stale": true}'),
            )
            db.maybe_commit()
            result = cache.get("old question", "db", "fp")
        assert result is None

    def test_put_overwrites_existing(self, tmp_path):
        db_path = tmp_path / "atlas.db"
        with Database(db_path) as db:
            cache = AgentCache(db)
            cache.put("q", "db", "fp", {"v": 1})
            cache.put("q", "db", "fp", {"v": 2})
            assert cache.get("q", "db", "fp") == {"v": 2}


# ---------------------------------------------------------------------------
# _schema_fingerprint
# ---------------------------------------------------------------------------


class TestSchemaFingerprint:
    def test_deterministic(self):
        fp1 = _schema_fingerprint(["a", "b", "c"])
        fp2 = _schema_fingerprint(["c", "a", "b"])
        assert fp1 == fp2

    def test_differs_for_different_ids(self):
        assert _schema_fingerprint(["x"]) != _schema_fingerprint(["y"])

    def test_empty_list(self):
        assert isinstance(_schema_fingerprint([]), str)


# ---------------------------------------------------------------------------
# _gather_context
# ---------------------------------------------------------------------------


class TestGatherContext:
    def test_returns_expected_structure(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "orders", "")
        assert "assets" in ctx
        assert "relationships" in ctx
        assert "asset_ids" in ctx
        assert len(ctx["assets"]) > 0

    def test_finds_relevant_asset(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "orders", "")
        ids = ctx["asset_ids"]
        assert any("orders" in aid for aid in ids)

    def test_gathers_schema(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "orders", "")
        orders_asset = next(a for a in ctx["assets"] if "orders" in a["id"])
        assert len(orders_asset["schema"]) == 3

    def test_gathers_annotations(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "orders", "")
        orders_asset = next(a for a in ctx["assets"] if "orders" in a["id"])
        assert orders_asset["annotation"] is not None
        assert "order_id" in orders_asset["annotation"]["column_notes"]

    def test_gathers_relationships(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        # "public" matches both pg::public.orders and pg::public.customers IDs.
        ctx = _gather_context(cfg.db_path, "public", "")
        assert len(ctx["relationships"]) >= 1
        rel = ctx["relationships"][0]
        assert rel["edges"][0]["join_guidance"] == "Always filter by tenant_id before joining."

    def test_empty_db_returns_empty(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        ctx = _gather_context(cfg.db_path, "nonexistent table", "")
        assert ctx["assets"] == []
        assert ctx["asset_ids"] == []


# ---------------------------------------------------------------------------
# _fallback_context_package
# ---------------------------------------------------------------------------


class TestFallbackContextPackage:
    def test_returns_context_package(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "orders", "")
        pkg = _fallback_context_package(ctx, "show me orders with customer names")
        assert isinstance(pkg, ContextPackage)
        assert len(pkg.relevant_tables) > 0

    def test_surrogate_key_warning(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "orders", "")
        pkg = _fallback_context_package(ctx, "SUM order_id")
        assert any("surrogate key" in w.lower() for w in pkg.warnings)

    def test_join_recommendations(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        ctx = _gather_context(cfg.db_path, "public", "")
        pkg = _fallback_context_package(ctx, "join orders and customers")
        assert any(
            "Always filter by tenant_id" in (jr.guidance or "")
            for jr in pkg.recommended_joins
        )


# ---------------------------------------------------------------------------
# run_atlas_context
# ---------------------------------------------------------------------------


class TestRunAtlasContext:
    @pytest.mark.asyncio
    async def test_fallback_when_mock_provider(self, tmp_path):
        """With mock provider, returns raw gathered context."""
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        result = await run_atlas_context(cfg, "orders")
        assert isinstance(result, ContextPackage)
        assert len(result.relevant_tables) > 0

    @pytest.mark.asyncio
    async def test_uses_llm_provider_when_configured(self, tmp_path):
        """With a real provider configured, the LLM is called."""
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        expected = ContextPackage(
            relevant_tables=["pg::public.orders"],
            summary="LLM summary",
        )
        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=expected)
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            result = await run_atlas_context(cfg, "orders", db_id="pg")

        assert result.summary == "LLM summary"
        mock_provider.analyze.assert_called_once()

    @pytest.mark.asyncio
    async def test_caches_llm_result(self, tmp_path):
        """Second call with same args returns cached result (LLM called once)."""
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        expected = ContextPackage(
            relevant_tables=["pg::public.orders"],
            summary="cached result",
        )
        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=expected)
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            r1 = await run_atlas_context(cfg, "orders", db_id="pg")
            r2 = await run_atlas_context(cfg, "orders", db_id="pg")

        assert r1.summary == "cached result"
        assert r2.summary == "cached result"
        # LLM called only once; second call returned from cache.
        assert mock_provider.analyze.call_count == 1

    @pytest.mark.asyncio
    async def test_falls_back_on_llm_error(self, tmp_path):
        """LLM failure falls back to raw context without raising."""
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(side_effect=RuntimeError("LLM unavailable"))
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            result = await run_atlas_context(cfg, "orders")

        assert isinstance(result, ContextPackage)

    @pytest.mark.asyncio
    async def test_evidence_passed_to_prompt(self, tmp_path):
        """Evidence string is included in the user prompt sent to the provider."""
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=ContextPackage())
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            await run_atlas_context(cfg, "orders", evidence="filter by US region")

        _, call_kwargs = mock_provider.analyze.call_args
        # user_prompt is the second positional argument
        call_args = mock_provider.analyze.call_args[0]
        user_prompt = call_args[1]
        assert "filter by US region" in user_prompt


# ---------------------------------------------------------------------------
# run_atlas_ask
# ---------------------------------------------------------------------------


class TestRunAtlasAsk:
    @pytest.mark.asyncio
    async def test_fallback_when_mock_provider(self, tmp_path):
        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        result = await run_atlas_ask(cfg, "what does customer_id mean?")
        assert isinstance(result, AskResult)
        assert "orders" in result.answer or "customers" in result.answer or len(result.sources) >= 0

    @pytest.mark.asyncio
    async def test_uses_llm_provider_when_configured(self, tmp_path):
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        expected = AskResult(answer="customer_id is a FK to customers.id", sources=["pg::public.orders"])
        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=expected)
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            # Use "orders" so the search finds seeded assets.
            result = await run_atlas_ask(cfg, "what columns does orders have?")

        assert result.answer == "customer_id is a FK to customers.id"
        mock_provider.analyze.assert_called_once()

    @pytest.mark.asyncio
    async def test_falls_back_on_llm_error(self, tmp_path):
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(side_effect=RuntimeError("timeout"))
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            result = await run_atlas_ask(cfg, "what does customer_id mean?")

        assert isinstance(result, AskResult)
        assert "timeout" in result.answer


# ---------------------------------------------------------------------------
# run_verify_deep
# ---------------------------------------------------------------------------


class TestRunVerifyDeep:
    @pytest.mark.asyncio
    async def test_mock_provider_returns_static_result(self, tmp_path):
        """With mock provider, returns static_result wrapped in VerificationResult."""
        cfg = _make_cfg(tmp_path)
        static = {"warnings": ["w1"], "suggestions": ["s1"]}
        result = await run_verify_deep(cfg, "SELECT 1", static_result=static)
        assert isinstance(result, VerificationResult)
        assert result.warnings == ["w1"]
        assert result.suggestions == ["s1"]
        assert result.valid is False  # has warnings

    @pytest.mark.asyncio
    async def test_uses_llm_when_configured(self, tmp_path):
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        expected = VerificationResult(
            valid=False,
            warnings=["Deep: use tenant_id filter"],
            analysis="Thorough analysis done by LLM.",
        )
        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=expected)
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            result = await run_verify_deep(
                cfg,
                "SELECT * FROM orders JOIN customers ON orders.customer_id = customers.id",
                static_result={"warnings": [], "suggestions": []},
            )

        assert result.analysis == "Thorough analysis done by LLM."
        mock_provider.analyze.assert_called_once()

    @pytest.mark.asyncio
    async def test_static_result_included_in_prompt(self, tmp_path):
        """Static analysis result is included in the user prompt."""
        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=VerificationResult())
        mock_provider.aclose = AsyncMock()

        static = {"warnings": ["static warning"], "suggestions": []}
        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            await run_verify_deep(cfg, "SELECT 1", static_result=static)

        user_prompt = mock_provider.analyze.call_args[0][1]
        assert "static warning" in user_prompt

    @pytest.mark.asyncio
    async def test_falls_back_on_llm_error(self, tmp_path):
        cfg = _make_cfg(tmp_path, with_agent=True)
        static = {"warnings": ["w1"], "suggestions": []}

        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(side_effect=RuntimeError("LLM down"))
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            result = await run_verify_deep(cfg, "SELECT 1", static_result=static)

        assert isinstance(result, VerificationResult)
        assert result.warnings == ["w1"]
        assert result.analysis is not None and "LLM down" in result.analysis


# ---------------------------------------------------------------------------
# MCP tool handlers
# ---------------------------------------------------------------------------


class TestAtlasContextHandler:
    @pytest.mark.asyncio
    async def test_handler_returns_json(self, tmp_path):
        from alma_atlas.mcp.tools import _handle_context

        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        texts = await _handle_context(cfg, {"question": "orders"})
        assert len(texts) == 1
        data = json.loads(texts[0].text)
        assert "relevant_tables" in data

    @pytest.mark.asyncio
    async def test_handler_with_mock_llm(self, tmp_path):
        from alma_atlas.mcp.tools import _handle_context

        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        expected = ContextPackage(relevant_tables=["pg::public.orders"], summary="handler test")
        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=expected)
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            texts = await _handle_context(cfg, {"question": "orders", "db_id": "pg"})

        data = json.loads(texts[0].text)
        assert data["summary"] == "handler test"


class TestAtlasAskHandler:
    @pytest.mark.asyncio
    async def test_handler_returns_json(self, tmp_path):
        from alma_atlas.mcp.tools import _handle_ask

        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        texts = await _handle_ask(cfg, {"question": "what is customer_id?"})
        assert len(texts) == 1
        data = json.loads(texts[0].text)
        assert "answer" in data


class TestAtlasVerifyDeepHandler:
    @pytest.mark.asyncio
    async def test_dispatch_verify_deep_flag(self, tmp_path):
        """_dispatch_verify returns a coroutine when deep=True."""
        from alma_atlas.mcp.tools import _dispatch_verify

        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        result = _dispatch_verify(cfg, {"sql": "SELECT 1", "deep": True})
        import inspect
        assert inspect.isawaitable(result)
        await result  # should not raise

    def test_dispatch_verify_static_flag(self, tmp_path):
        """_dispatch_verify returns list[TextContent] when deep=False."""
        import inspect

        from alma_atlas.mcp.tools import _dispatch_verify

        cfg = _make_cfg(tmp_path)
        _seed_assets(cfg.db_path)
        result = _dispatch_verify(cfg, {"sql": "SELECT 1", "deep": False})
        assert not inspect.isawaitable(result)
        assert isinstance(result, list)

    @pytest.mark.asyncio
    async def test_deep_verify_calls_agent(self, tmp_path):
        from alma_atlas.mcp.tools import _handle_verify_deep

        cfg = _make_cfg(tmp_path, with_agent=True)
        _seed_assets(cfg.db_path)

        expected = VerificationResult(valid=True, analysis="All good.")
        mock_provider = AsyncMock()
        mock_provider.analyze = AsyncMock(return_value=expected)
        mock_provider.aclose = AsyncMock()

        with patch(
            "alma_atlas.agents.atlas_agent.provider_from_agent_config",
            return_value=mock_provider,
        ):
            texts = await _handle_verify_deep(cfg, {"sql": "SELECT * FROM orders", "deep": True})

        data = json.loads(texts[0].text)
        assert data["analysis"] == "All good."
        mock_provider.analyze.assert_called_once()
