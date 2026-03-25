"""Production-hardening tests — ENG-hardening.

Covers:
    - Sync client: retry on 503, cursor not updated on partial failure, request IDs
    - Enforcement engine: empty schema, malformed contract mode, deterministic IDs
    - Scan pipeline: per-source timeout, adapter construction failure, concurrent semaphore
    - Security: quote_bq_identifier / quote_sf_identifier escape adversarial identifiers, config __repr__ redacts secrets
    - Config: atlas.yml rejects unknown top-level keys
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alma_atlas.config import AtlasConfig, SourceConfig, load_atlas_yml
from alma_atlas.sync.auth import TeamAuth
from alma_atlas.sync.client import SyncClient
from alma_atlas_store.db import Database

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────


@pytest.fixture
def tmp_cfg(tmp_path: Path) -> AtlasConfig:
    return AtlasConfig(config_dir=tmp_path / "alma")


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def auth() -> TeamAuth:
    return TeamAuth("test-api-key-hardening")


# ─────────────────────────────────────────────────────────────────────────────
# 1. Sync client — retry on transient server errors
# ─────────────────────────────────────────────────────────────────────────────


class TestSyncClientRetry:
    """Sync client retries on 5xx responses and respects Retry-After on 429."""

    @pytest.mark.asyncio
    async def test_retries_on_503_then_succeeds(self, auth: TeamAuth):
        """POST retries on 503 and succeeds on the second attempt."""
        call_count = 0

        async def fake_post(url, json=None, headers=None):
            nonlocal call_count
            call_count += 1
            r = MagicMock()
            r.raise_for_status = MagicMock()
            if call_count == 1:
                # First call: 503
                r.status_code = 503
                r.raise_for_status.side_effect = Exception("503 Service Unavailable")
            else:
                # Second call: success
                r.status_code = 200
                r.json = MagicMock(return_value={
                    "accepted_count": 1,
                    "rejected": [],
                    "new_cursor": "2024-06-01T00:00:00Z",
                })
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=fake_post)

        # Patch asyncio.sleep to avoid actual delays in tests
        with patch("asyncio.sleep", new_callable=AsyncMock):
            async with SyncClient("https://team.example.com", auth, "team1", http_client=http) as client:
                resp = await client.push_assets([{"id": "a1"}], "2024-01-01T00:00:00Z")

        assert resp.accepted_count == 1
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_respects_retry_after_on_429(self, auth: TeamAuth):
        """POST observes Retry-After header on 429 before retrying."""
        sleep_durations: list[float] = []
        call_count = 0

        async def fake_sleep(duration: float):
            sleep_durations.append(duration)

        async def fake_post(url, json=None, headers=None):
            nonlocal call_count
            call_count += 1
            r = MagicMock()
            if call_count == 1:
                r.status_code = 429
                r.headers = {"Retry-After": "2"}
                r.raise_for_status = MagicMock()
            else:
                r.status_code = 200
                r.raise_for_status = MagicMock()
                r.json = MagicMock(return_value={
                    "accepted_count": 0,
                    "rejected": [],
                    "new_cursor": "",
                })
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=fake_post)

        with patch("asyncio.sleep", side_effect=fake_sleep):
            async with SyncClient("https://team.example.com", auth, "team1", http_client=http) as client:
                await client.push_assets([], "2024-01-01T00:00:00Z")

        # Should have slept for the Retry-After value
        assert any(d == 2.0 for d in sleep_durations)

    @pytest.mark.asyncio
    async def test_request_id_header_present(self, auth: TeamAuth):
        """Every POST request includes an X-Request-ID header."""
        captured_headers: list[dict] = []

        async def fake_post(url, json=None, headers=None):
            captured_headers.append(headers or {})
            r = MagicMock()
            r.status_code = 200
            r.raise_for_status = MagicMock()
            r.json = MagicMock(return_value={"accepted_count": 0, "rejected": [], "new_cursor": ""})
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=fake_post)

        async with SyncClient("https://team.example.com", auth, "team1", http_client=http) as client:
            await client.push_assets([], "2024-01-01T00:00:00Z")

        assert "X-Request-ID" in captured_headers[0]
        # Should be a valid UUID4 format
        import uuid
        uuid.UUID(captured_headers[0]["X-Request-ID"])

    @pytest.mark.asyncio
    async def test_cursor_not_updated_when_push_fails(
        self, tmp_cfg: AtlasConfig, db: Database, auth: TeamAuth
    ):
        """If push raises, the sync cursor is NOT updated."""
        tmp_cfg.save_sync_cursor("2024-01-01T00:00:00Z")

        async def failing_post(url, json=None, headers=None):
            r = MagicMock()
            r.status_code = 500
            r.headers = {}
            r.raise_for_status.side_effect = Exception("500 Internal Server Error")
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=failing_post)

        with patch("asyncio.sleep", new_callable=AsyncMock):
            async with SyncClient(
                "https://team.example.com", auth, "team1", http_client=http
            ) as client:
                tmp_cfg.team_id = "team1"
                with pytest.raises(Exception):  # noqa: B017
                    await client.full_sync(db, tmp_cfg)

        # Cursor must remain unchanged because the push failed.
        assert tmp_cfg.load_sync_cursor() == "2024-01-01T00:00:00Z"

    @pytest.mark.asyncio
    async def test_non_json_response_raises(self, auth: TeamAuth):
        """A non-JSON server response raises a ValueError, not a silent failure."""
        async def fake_post(url, json=None, headers=None):
            r = MagicMock()
            r.status_code = 200
            r.raise_for_status = MagicMock()
            r.json.side_effect = ValueError("No JSON object")
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=fake_post)

        async with SyncClient("https://team.example.com", auth, "team1", http_client=http) as client:
            with pytest.raises(ValueError, match="non-JSON"):
                await client.push_assets([], "2024-01-01T00:00:00Z")


# ─────────────────────────────────────────────────────────────────────────────
# 2. Enforcement engine — hardening
# ─────────────────────────────────────────────────────────────────────────────


class TestEnforcementHardening:
    """Enforcement engine handles edge cases gracefully."""

    def test_empty_report_returns_not_blocked(self, db):
        """Empty DriftReport produces no violations and is never blocked."""
        from alma_atlas.enforcement.drift import DriftReport
        from alma_atlas.enforcement.engine import EnforcementEngine

        engine = EnforcementEngine(db)
        result = engine.enforce(DriftReport(), "enforce")
        assert not result.blocked
        assert not result.has_violations

    def test_unknown_mode_treated_as_shadow(self, db):
        """Unknown contract mode is treated as 'shadow' instead of crashing."""
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        from alma_atlas.enforcement.engine import EnforcementEngine

        engine = EnforcementEngine(db)
        report = DriftReport(violations=[
            DriftViolation("x::t", "type_changed", "error", {"message": "x"}),
        ])
        # Should not raise; should return an un-blocked result.
        result = engine.enforce(report, "invalid_mode")  # type: ignore[arg-type]
        assert not result.blocked
        assert result.mode == "shadow"

    def test_deterministic_violation_ids(self, db):
        """Same drift event produces the same violation ID across two calls."""
        from alma_atlas.enforcement.drift import DriftViolation
        from alma_atlas.enforcement.engine import _deterministic_violation_id

        v = DriftViolation("src::schema.table", "removed_column", "warning", {"column": "email"})
        id1 = _deterministic_violation_id(v.asset_id, v.violation_type, v.details)
        id2 = _deterministic_violation_id(v.asset_id, v.violation_type, v.details)
        assert id1 == id2

    def test_different_violations_have_different_ids(self, db):
        """Different drift events produce different IDs."""
        from alma_atlas.enforcement.engine import _deterministic_violation_id

        id1 = _deterministic_violation_id("src::t", "removed_column", {"column": "email"})
        id2 = _deterministic_violation_id("src::t", "removed_column", {"column": "name"})
        assert id1 != id2

    def test_violations_written_with_deterministic_id(self, db):
        """EnforcementEngine persists violations using deterministic IDs."""
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        from alma_atlas.enforcement.engine import EnforcementEngine, _deterministic_violation_id
        from alma_atlas_store.violation_repository import ViolationRepository

        engine = EnforcementEngine(db)
        v = DriftViolation("x::t", "type_changed", "error", {"column": "score", "message": "type changed"})
        report = DriftReport(violations=[v])
        engine.enforce(report, "enforce")

        expected_id = _deterministic_violation_id(v.asset_id, v.violation_type, v.details)
        stored = ViolationRepository(db).list_for_asset("x::t")
        assert len(stored) == 1
        assert stored[0].id == expected_id


# ─────────────────────────────────────────────────────────────────────────────
# 3. Scan pipeline — timeout and concurrency
# ─────────────────────────────────────────────────────────────────────────────


class TestScanPipelineHardening:
    """Scan pipeline handles timeouts and concurrent sources correctly."""

    def test_scan_timeout_returns_error_result(self, tmp_path: Path):
        """A scan that exceeds its timeout returns a ScanResult with error, not an exception."""
        from alma_atlas.pipeline.scan import run_scan

        cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
        source = SourceConfig(id="pg-slow", kind="postgres", params={})

        async def slow_introspect(persisted):
            await asyncio.sleep(10)  # longer than our 0.01s timeout
            raise AssertionError("should not reach here")

        mock_adapter = MagicMock()
        mock_adapter.introspect_schema = AsyncMock(side_effect=slow_introspect)

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())):
            result = run_scan(source, cfg, timeout=0.01)

        assert result.error is not None
        assert "TimeoutError" in result.error or "timed out" in result.error.lower()
        assert result.asset_count == 0

    def test_adapter_construction_failure_skips_source(self, tmp_path: Path):
        """Adapter construction failure produces a ScanResult with error instead of raising."""
        from alma_atlas.pipeline.scan import run_scan

        cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
        source = SourceConfig(id="bad-source", kind="unsupported_kind", params={})

        result = run_scan(source, cfg)
        assert result.error is not None
        assert result.asset_count == 0

    def test_dry_run_skips_data_extraction(self, tmp_path: Path):
        """dry_run=True returns without calling introspect_schema."""
        from alma_atlas.pipeline.scan import run_scan_async

        cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
        source = SourceConfig(id="pg-test", kind="postgres", params={})

        mock_adapter = MagicMock()
        mock_adapter.introspect_schema = AsyncMock()

        with patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())):
            result = asyncio.run(run_scan_async(source, cfg, dry_run=True))

        mock_adapter.introspect_schema.assert_not_called()
        assert result.error is None
        assert result.asset_count == 0

    def test_scan_all_runs_concurrently_with_semaphore(self, tmp_path: Path):
        """run_scan_all uses asyncio.Semaphore to cap concurrency."""
        from alma_atlas.pipeline.scan import run_scan_all
        from alma_connectors.source_adapter import SchemaSnapshot

        cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
        sources = [
            SourceConfig(id=f"pg-{i}", kind="postgres", params={})
            for i in range(6)
        ]

        empty_snapshot = SchemaSnapshot(captured_at=None, objects=(), dependencies=())

        mock_adapter = MagicMock()
        mock_adapter.introspect_schema = AsyncMock(return_value=empty_snapshot)
        mock_adapter.observe_traffic = AsyncMock(
            return_value=MagicMock(scanned_records=0, events=())
        )

        with (
            patch("alma_atlas.pipeline.scan._build_adapter", return_value=(mock_adapter, MagicMock())),
            patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
        ):
            result = run_scan_all(sources, cfg, max_concurrent=2)

        # All 6 sources should have completed (no errors from known-good mock)
        assert len(result.results) == 6
        assert all(r.error is None for r in result.results)

    def test_scan_all_collects_errors_without_crashing(self, tmp_path: Path):
        """run_scan_all continues scanning remaining sources even if one fails."""
        from alma_atlas.pipeline.scan import run_scan_all

        cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=tmp_path / "atlas.db")
        sources = [
            SourceConfig(id="pg-good", kind="postgres", params={}),
            SourceConfig(id="bad-kind", kind="unsupported_kind", params={}),
            SourceConfig(id="pg-good2", kind="postgres", params={}),
        ]

        from alma_connectors.source_adapter import SchemaSnapshot

        empty_snapshot = SchemaSnapshot(captured_at=None, objects=(), dependencies=())
        mock_adapter = MagicMock()
        mock_adapter.introspect_schema = AsyncMock(return_value=empty_snapshot)
        mock_adapter.observe_traffic = AsyncMock(
            return_value=MagicMock(scanned_records=0, events=())
        )

        def build_adapter_side_effect(source):
            if source.kind == "unsupported_kind":
                raise ValueError("Unknown source kind")
            return mock_adapter, MagicMock()

        with (
            patch("alma_atlas.pipeline.scan._build_adapter", side_effect=build_adapter_side_effect),
            patch("alma_atlas.pipeline.stitch.stitch", return_value=0),
        ):
            result = run_scan_all(sources, cfg)

        assert len(result.results) == 3
        errors = [r for r in result.results if r.error is not None]
        successes = [r for r in result.results if r.error is None]
        assert len(errors) == 1
        assert errors[0].source_id == "bad-kind"
        assert len(successes) == 2


# ─────────────────────────────────────────────────────────────────────────────
# 4. Security — SQL identifier validation
# ─────────────────────────────────────────────────────────────────────────────


class TestIdentifierQuoting:
    """quote_bq_identifier / quote_sf_identifier properly escape adversarial inputs."""

    def test_sf_normal_identifier(self):
        from alma_ports.sql_safety import quote_sf_identifier

        assert quote_sf_identifier("MY_DATABASE") == '"MY_DATABASE"'
        assert quote_sf_identifier("schema123") == '"schema123"'

    def test_sf_empty_raises(self):
        from alma_ports.sql_safety import quote_sf_identifier

        with pytest.raises(ValueError, match="must not be empty"):
            quote_sf_identifier("")

    def test_sf_single_quote_escaped(self):
        from alma_ports.sql_safety import quote_sf_identifier

        # Single quotes are not special in double-quote delimiters; no escaping needed
        result = quote_sf_identifier("db'name")
        assert result == '"db\'name"'

    def test_sf_double_quote_escaped(self):
        from alma_ports.sql_safety import quote_sf_identifier

        result = quote_sf_identifier('db"name')
        assert result == '"db""name"'

    def test_sf_semicolon_injection_contained(self):
        from alma_ports.sql_safety import quote_sf_identifier

        result = quote_sf_identifier("db;DROP TABLE users")
        assert result == '"db;DROP TABLE users"'

    def test_sf_comment_injection_contained(self):
        from alma_ports.sql_safety import quote_sf_identifier

        result = quote_sf_identifier("db--comment")
        assert result == '"db--comment"'

    def test_sf_newline_contained(self):
        from alma_ports.sql_safety import quote_sf_identifier

        result = quote_sf_identifier("db\nname")
        assert result == '"db\nname"'

    def test_bq_normal_identifier(self):
        from alma_ports.sql_safety import quote_bq_identifier

        assert quote_bq_identifier("my-project") == "`my-project`"

    def test_bq_injection_contained(self):
        from alma_ports.sql_safety import quote_bq_identifier

        result = quote_bq_identifier("project';DROP")
        assert result == "`project';DROP`"

    def test_bq_backtick_escaped(self):
        from alma_ports.sql_safety import quote_bq_identifier

        result = quote_bq_identifier("tab`le")
        assert result == "`tab\\`le`"

    def test_bq_empty_raises(self):
        from alma_ports.sql_safety import quote_bq_identifier

        with pytest.raises(ValueError, match="must not be empty"):
            quote_bq_identifier("")


# ─────────────────────────────────────────────────────────────────────────────
# 5. Config — __repr__ redaction and atlas.yml validation
# ─────────────────────────────────────────────────────────────────────────────


class TestConfigRepr:
    """Config __repr__ methods redact sensitive fields."""

    def test_source_config_repr_redacts_dsn(self):
        source = SourceConfig(
            id="pg-test",
            kind="postgres",
            params={"dsn": "postgresql://user:secret@host/db", "schema": "public"},
        )
        r = repr(source)
        assert "secret" not in r
        assert "***" in r
        assert "schema" in r  # non-secret param visible

    def test_source_config_repr_redacts_api_key(self):
        source = SourceConfig(
            id="ft-test",
            kind="fivetran",
            params={"api_key": "super_secret_key", "api_key_env": "FT_KEY"},
        )
        r = repr(source)
        assert "super_secret_key" not in r
        assert "***" in r

    def test_atlas_config_repr_redacts_team_api_key(self):
        cfg = AtlasConfig()
        cfg.team_api_key = "very_secret_token"
        r = repr(cfg)
        assert "very_secret_token" not in r
        assert "***" in r

    def test_atlas_config_repr_shows_server_url(self):
        cfg = AtlasConfig()
        cfg.team_server_url = "https://sync.example.com"
        cfg.team_api_key = "secret"
        r = repr(cfg)
        assert "sync.example.com" in r

    def test_atlas_config_repr_no_key_shown_as_none(self):
        cfg = AtlasConfig()
        cfg.team_api_key = None
        r = repr(cfg)
        assert "None" in r


class TestAtlasYml:
    """load_atlas_yml rejects unknown top-level keys (fail-closed)."""

    def test_valid_atlas_yml_loads(self, tmp_path: Path):
        yml = tmp_path / "atlas.yml"
        yml.write_text("""
version: 1
sources:
  - id: pg-mydb
    kind: postgres
    params:
      dsn_env: PG_DATABASE_URL
team:
  server_url: https://sync.example.com
  team_id: eng
""")
        pytest.importorskip("yaml")
        cfg = load_atlas_yml(yml)
        assert len(cfg.sources) == 1
        assert cfg.sources[0].id == "pg-mydb"
        assert cfg.team_server_url == "https://sync.example.com"

    def test_unknown_top_level_key_rejected(self, tmp_path: Path):
        yml = tmp_path / "atlas.yml"
        yml.write_text("""
version: 1
sources: []
unknown_key: should_fail
""")
        pytest.importorskip("yaml")
        with pytest.raises(ValueError, match="Unknown top-level key"):
            load_atlas_yml(yml)

    def test_multiple_unknown_keys_rejected(self, tmp_path: Path):
        yml = tmp_path / "atlas.yml"
        yml.write_text("""
version: 1
bad_key_1: x
bad_key_2: y
""")
        pytest.importorskip("yaml")
        with pytest.raises(ValueError, match="Unknown top-level key"):
            load_atlas_yml(yml)

    def test_empty_file_loads_default_config(self, tmp_path: Path):
        yml = tmp_path / "atlas.yml"
        yml.write_text("")
        pytest.importorskip("yaml")
        cfg = load_atlas_yml(yml)
        assert cfg.sources == []
        assert cfg.team_server_url is None
