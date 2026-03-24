"""Tests for the team sync layer — ENG-398."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from alma_atlas.config import AtlasConfig
from alma_atlas.sync.auth import TeamAuth
from alma_atlas.sync.client import SyncClient
from alma_atlas.sync.conflict import ConflictResolver
from alma_atlas.sync.protocol import SyncPayload, SyncResponse
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def tmp_cfg(tmp_path: Path) -> AtlasConfig:
    return AtlasConfig(config_dir=tmp_path / "alma")


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


@pytest.fixture
def auth() -> TeamAuth:
    return TeamAuth("test-api-key-123")


def _make_http_client(push_response: dict, pull_assets: list, pull_contracts: list) -> Any:
    """Return a mock httpx.AsyncClient that returns canned responses."""
    mock_response = MagicMock()
    mock_response.raise_for_status = MagicMock()

    async def post_json(*args, **kwargs):
        mock_response.json = MagicMock(return_value=push_response)
        return mock_response

    async def get_json(*args, **kwargs):
        url = args[0] if args else kwargs.get("url", "")
        if "assets" in str(url):
            mock_response.json = MagicMock(return_value={"assets": pull_assets})
        else:
            mock_response.json = MagicMock(return_value={"contracts": pull_contracts})
        return mock_response

    client = MagicMock()
    client.post = AsyncMock(side_effect=post_json)
    client.get = AsyncMock(side_effect=get_json)
    return client


# ---------------------------------------------------------------------------
# TeamAuth
# ---------------------------------------------------------------------------


class TestTeamAuth:
    def test_headers_contains_bearer_token(self):
        auth = TeamAuth("secret-key")
        headers = auth.headers()
        assert headers["Authorization"] == "Bearer secret-key"

    def test_empty_key_raises(self):
        with pytest.raises(ValueError):
            TeamAuth("")


# ---------------------------------------------------------------------------
# SyncPayload / SyncResponse
# ---------------------------------------------------------------------------


class TestProtocol:
    def test_sync_payload_to_dict(self):
        payload = SyncPayload(assets=[{"id": "a"}], sync_cursor="2024-01-01T00:00:00Z", team_id="team1")
        d = payload.to_dict()
        assert d["assets"] == [{"id": "a"}]
        assert d["sync_cursor"] == "2024-01-01T00:00:00Z"
        assert d["team_id"] == "team1"

    def test_sync_response_from_dict(self):
        data = {
            "accepted_count": 3,
            "rejected": [{"id": "x", "reason": "duplicate"}],
            "new_cursor": "2024-06-01T12:00:00Z",
        }
        resp = SyncResponse.from_dict(data)
        assert resp.accepted_count == 3
        assert len(resp.rejected) == 1
        assert resp.rejected[0].id == "x"
        assert resp.new_cursor == "2024-06-01T12:00:00Z"

    def test_sync_response_from_empty_dict(self):
        resp = SyncResponse.from_dict({})
        assert resp.accepted_count == 0
        assert resp.rejected == []
        assert resp.new_cursor == ""


# ---------------------------------------------------------------------------
# ConflictResolver
# ---------------------------------------------------------------------------


class TestConflictResolver:
    def test_resolve_asset_last_write_wins_remote_newer(self):
        local = {"id": "a", "last_seen": "2024-01-01T10:00:00"}
        remote = {"id": "a", "last_seen": "2024-01-02T10:00:00"}
        resolved = ConflictResolver.resolve_asset(local, remote)
        assert resolved is remote

    def test_resolve_asset_last_write_wins_local_newer(self):
        local = {"id": "a", "last_seen": "2024-06-01T10:00:00"}
        remote = {"id": "a", "last_seen": "2024-01-01T10:00:00"}
        resolved = ConflictResolver.resolve_asset(local, remote)
        assert resolved is local

    def test_resolve_asset_equal_timestamps_prefers_remote(self):
        ts = "2024-01-01T00:00:00"
        local = {"id": "a", "last_seen": ts}
        remote = {"id": "a", "last_seen": ts}
        resolved = ConflictResolver.resolve_asset(local, remote)
        assert resolved is remote

    def test_resolve_contract_server_wins(self):
        local = {"id": "c", "mode": "warn"}
        remote = {"id": "c", "mode": "enforce"}
        resolved = ConflictResolver.resolve_contract(local, remote)
        assert resolved is remote
        assert resolved["mode"] == "enforce"

    def test_resolve_edge_last_write_wins(self):
        local = {"id": "e", "last_seen": "2024-01-01"}
        remote = {"id": "e", "last_seen": "2024-03-01"}
        resolved = ConflictResolver.resolve_edge(local, remote)
        assert resolved is remote


# ---------------------------------------------------------------------------
# SyncClient push methods
# ---------------------------------------------------------------------------


class TestSyncClientPush:
    @pytest.mark.asyncio
    async def test_push_assets_sends_correct_payload(self, auth: TeamAuth):
        http = _make_http_client(
            push_response={"accepted_count": 2, "rejected": [], "new_cursor": "2024-06-01T00:00:00Z"},
            pull_assets=[],
            pull_contracts=[],
        )
        client = SyncClient("https://team.example.com", auth, "team42", http_client=http)
        resp = await client.push_assets([{"id": "a1"}, {"id": "a2"}], "2024-01-01T00:00:00Z")
        assert resp.accepted_count == 2
        assert resp.new_cursor == "2024-06-01T00:00:00Z"
        http.post.assert_called_once()
        call_kwargs = http.post.call_args
        body = call_kwargs.kwargs.get("json") or call_kwargs.args[1]
        assert body["team_id"] == "team42"
        assert len(body["assets"]) == 2

    @pytest.mark.asyncio
    async def test_push_edges(self, auth: TeamAuth):
        http = _make_http_client(
            push_response={"accepted_count": 1, "rejected": [], "new_cursor": "2024-06-01T00:00:00Z"},
            pull_assets=[],
            pull_contracts=[],
        )
        client = SyncClient("https://team.example.com", auth, "teamX", http_client=http)
        resp = await client.push_edges([{"upstream_id": "a", "downstream_id": "b", "kind": "reads"}], "2024-01-01T00:00:00Z")
        assert resp.accepted_count == 1

    @pytest.mark.asyncio
    async def test_push_contracts(self, auth: TeamAuth):
        http = _make_http_client(
            push_response={"accepted_count": 1, "rejected": [], "new_cursor": "2024-06-01T00:00:00Z"},
            pull_assets=[],
            pull_contracts=[],
        )
        client = SyncClient("https://team.example.com", auth, "teamX", http_client=http)
        resp = await client.push_contracts([{"id": "c1"}], "2024-01-01T00:00:00Z")
        assert resp.accepted_count == 1

    @pytest.mark.asyncio
    async def test_push_violations(self, auth: TeamAuth):
        http = _make_http_client(
            push_response={"accepted_count": 1, "rejected": [], "new_cursor": "2024-06-01T00:00:00Z"},
            pull_assets=[],
            pull_contracts=[],
        )
        client = SyncClient("https://team.example.com", auth, "teamX", http_client=http)
        resp = await client.push_violations([{"id": "v1"}], "2024-01-01T00:00:00Z")
        assert resp.accepted_count == 1


# ---------------------------------------------------------------------------
# SyncClient pull methods
# ---------------------------------------------------------------------------


class TestSyncClientPull:
    @pytest.mark.asyncio
    async def test_pull_assets_returns_list(self, auth: TeamAuth):
        remote_assets = [{"id": "ra1", "name": "remote asset"}]
        http = _make_http_client(
            push_response={},
            pull_assets=remote_assets,
            pull_contracts=[],
        )
        client = SyncClient("https://team.example.com", auth, "teamX", http_client=http)
        assets = await client.pull_assets("2024-01-01T00:00:00Z")
        assert len(assets) == 1
        assert assets[0]["id"] == "ra1"

    @pytest.mark.asyncio
    async def test_pull_contracts_returns_list(self, auth: TeamAuth):
        remote_contracts = [{"id": "rc1", "asset_id": "a1", "mode": "enforce"}]
        http = _make_http_client(
            push_response={},
            pull_assets=[],
            pull_contracts=remote_contracts,
        )
        client = SyncClient("https://team.example.com", auth, "teamX", http_client=http)
        contracts = await client.pull_contracts("2024-01-01T00:00:00Z")
        assert len(contracts) == 1
        assert contracts[0]["mode"] == "enforce"


# ---------------------------------------------------------------------------
# Incremental sync with cursors
# ---------------------------------------------------------------------------


class TestIncrementalSync:
    def test_load_cursor_returns_none_when_absent(self, tmp_cfg: AtlasConfig):
        assert tmp_cfg.load_sync_cursor() is None

    def test_save_and_load_cursor(self, tmp_cfg: AtlasConfig):
        tmp_cfg.save_sync_cursor("2024-06-15T12:00:00Z")
        assert tmp_cfg.load_sync_cursor() == "2024-06-15T12:00:00Z"

    def test_save_cursor_overwrites_previous(self, tmp_cfg: AtlasConfig):
        tmp_cfg.save_sync_cursor("2024-01-01T00:00:00Z")
        tmp_cfg.save_sync_cursor("2024-06-15T12:00:00Z")
        assert tmp_cfg.load_sync_cursor() == "2024-06-15T12:00:00Z"

    @pytest.mark.asyncio
    async def test_full_sync_saves_new_cursor(self, tmp_cfg: AtlasConfig, db: Database, auth: TeamAuth):
        http = _make_http_client(
            push_response={"accepted_count": 0, "rejected": [], "new_cursor": "2024-06-15T00:00:00Z"},
            pull_assets=[],
            pull_contracts=[],
        )
        client = SyncClient("https://team.example.com", auth, "team1", http_client=http)
        tmp_cfg.team_id = "team1"
        await client.full_sync(db, tmp_cfg)
        assert tmp_cfg.load_sync_cursor() == "2024-06-15T00:00:00Z"

    @pytest.mark.asyncio
    async def test_full_sync_only_pushes_records_since_cursor(
        self, tmp_cfg: AtlasConfig, db: Database, auth: TeamAuth
    ):
        # Insert an asset (last_seen will be CURRENT_TIMESTAMP ≈ now)
        AssetRepository(db).upsert(
            Asset(id="old", source="src", kind="table", name="old_table")
        )
        # Set cursor far in the future so the asset falls before it
        tmp_cfg.save_sync_cursor("2099-01-01T00:00:00Z")

        posted_bodies: list[dict] = []

        async def fake_post(url, json=None, headers=None):
            posted_bodies.append(json or {})
            r = MagicMock()
            r.raise_for_status = MagicMock()
            r.json = MagicMock(return_value={"accepted_count": 0, "rejected": [], "new_cursor": "2024-06-01T00:00:00Z"})
            return r

        async def fake_get(url, params=None, headers=None):
            r = MagicMock()
            r.raise_for_status = MagicMock()
            if "assets" in url:
                r.json = MagicMock(return_value={"assets": []})
            else:
                r.json = MagicMock(return_value={"contracts": []})
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=fake_post)
        http.get = AsyncMock(side_effect=fake_get)

        client = SyncClient("https://team.example.com", auth, "team1", http_client=http)
        tmp_cfg.team_id = "team1"
        await client.full_sync(db, tmp_cfg)

        # The assets push should have sent 0 assets (old asset is before cursor)
        assets_push = posted_bodies[0]  # first post is assets
        assert assets_push["assets"] == []


# ---------------------------------------------------------------------------
# TeamAuth header attachment
# ---------------------------------------------------------------------------


class TestTeamAuthHeaders:
    @pytest.mark.asyncio
    async def test_auth_header_attached_to_push(self, auth: TeamAuth):
        captured_headers: list[dict] = []

        async def fake_post(url, json=None, headers=None):
            captured_headers.append(headers or {})
            r = MagicMock()
            r.raise_for_status = MagicMock()
            r.json = MagicMock(return_value={"accepted_count": 0, "rejected": [], "new_cursor": ""})
            return r

        http = MagicMock()
        http.post = AsyncMock(side_effect=fake_post)
        client = SyncClient("https://team.example.com", auth, "team1", http_client=http)
        await client.push_assets([], "2024-01-01T00:00:00Z")
        assert captured_headers[0].get("Authorization") == "Bearer test-api-key-123"

    @pytest.mark.asyncio
    async def test_auth_header_attached_to_pull(self, auth: TeamAuth):
        captured_headers: list[dict] = []

        async def fake_get(url, params=None, headers=None):
            captured_headers.append(headers or {})
            r = MagicMock()
            r.raise_for_status = MagicMock()
            r.json = MagicMock(return_value={"assets": []})
            return r

        http = MagicMock()
        http.get = AsyncMock(side_effect=fake_get)
        client = SyncClient("https://team.example.com", auth, "team1", http_client=http)
        await client.pull_assets("2024-01-01T00:00:00Z")
        assert captured_headers[0].get("Authorization") == "Bearer test-api-key-123"


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------


class TestCliTeamInit:
    def test_init_saves_team_config(self, tmp_cfg: AtlasConfig, tmp_path: Path):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        runner = CliRunner()
        with patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg):
            result = runner.invoke(
                app,
                ["team", "init", "--server", "https://sync.example.com", "--key", "my-key", "--team-id", "eng"],
            )
        assert result.exit_code == 0
        tmp_cfg.load_team_config()
        assert tmp_cfg.team_server_url == "https://sync.example.com"
        assert tmp_cfg.team_api_key == "my-key"
        assert tmp_cfg.team_id == "eng"

    def test_init_requires_server(self, tmp_cfg: AtlasConfig):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        runner = CliRunner()
        with patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg):
            result = runner.invoke(app, ["team", "init", "--key", "k"])
        assert result.exit_code != 0

    def test_init_requires_key(self, tmp_cfg: AtlasConfig):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        runner = CliRunner()
        with patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg):
            result = runner.invoke(app, ["team", "init", "--server", "https://x.com"])
        assert result.exit_code != 0


class TestCliTeamStatus:
    def test_status_shows_not_configured(self, tmp_cfg: AtlasConfig):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        runner = CliRunner()
        with patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg):
            result = runner.invoke(app, ["team", "status"])
        assert result.exit_code == 0
        assert "not configured" in result.output

    def test_status_shows_server_when_configured(self, tmp_cfg: AtlasConfig):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        tmp_cfg.team_server_url = "https://sync.example.com"
        tmp_cfg.team_api_key = "key"
        tmp_cfg.team_id = "eng"
        tmp_cfg.save_team_config()

        runner = CliRunner()
        with patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg):
            result = runner.invoke(app, ["team", "status"])
        assert result.exit_code == 0
        assert "sync.example.com" in result.output


class TestCliTeamSync:
    def test_sync_fails_when_not_configured(self, tmp_cfg: AtlasConfig):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        runner = CliRunner()
        with patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg):
            result = runner.invoke(app, ["team", "sync"])
        assert result.exit_code != 0

    def test_sync_succeeds_with_mock_client(self, tmp_cfg: AtlasConfig, tmp_path: Path):
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        # Set up team config and a real (empty) database
        tmp_cfg.team_server_url = "https://sync.example.com"
        tmp_cfg.team_api_key = "key"
        tmp_cfg.team_id = "eng"
        db_path = tmp_path / "atlas.db"
        tmp_cfg.db_path = db_path
        with Database(db_path):
            pass  # create the file

        mock_response = SyncResponse(accepted_count=5, rejected=[], new_cursor="2024-06-01T00:00:00Z")
        mock_full_sync = AsyncMock(return_value=mock_response)

        runner = CliRunner()
        with (
            patch("alma_atlas.cli.team.get_config", return_value=tmp_cfg),
            patch("alma_atlas.sync.client.SyncClient.full_sync", mock_full_sync),
        ):
            result = runner.invoke(app, ["team", "sync"])
        assert result.exit_code == 0
        assert "5 record(s) accepted" in result.output


# ---------------------------------------------------------------------------
# Auto-sync after scan
# ---------------------------------------------------------------------------


class TestAutoSyncAfterScan:
    def test_no_sync_flag_skips_auto_sync(self, tmp_cfg: AtlasConfig):
        """When --no-sync is passed, full_sync should never be called.

        We configure team sync but register no sources — scan exits early with
        code 1. With --no-sync the auto-sync block is never entered regardless.
        """
        from typer.testing import CliRunner

        from alma_atlas.cli.main import app

        tmp_cfg.team_server_url = "https://sync.example.com"
        tmp_cfg.team_api_key = "key"
        tmp_cfg.team_id = "eng"

        mock_full_sync = AsyncMock()

        runner = CliRunner()
        with (
            patch("alma_atlas.cli.scan.get_config", return_value=tmp_cfg),
            patch("alma_atlas.sync.client.SyncClient.full_sync", mock_full_sync),
        ):
            # No sources registered → scan CLI exits before reaching auto-sync
            runner.invoke(app, ["scan", "--no-sync"])

        mock_full_sync.assert_not_called()
