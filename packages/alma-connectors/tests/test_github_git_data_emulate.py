"""Integration test for scan_mode='git' against an emulate.dev GitHub fixture.

Requires environment variables:
  EMULATE_GITHUB_BASE_URL  - e.g. http://127.0.0.1:4001
  EMULATE_GITHUB_TOKEN     - e.g. gho_test_token_admin
  EMULATE_GITHUB_OWNER     - e.g. octocat
  EMULATE_GITHUB_REPO      - e.g. atlas-ci-smoke
"""

from __future__ import annotations

import base64
import os

import httpx
import pytest

from alma_connectors.adapters.github import GitHubAdapter


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


_SKIP = not _env("EMULATE_GITHUB_BASE_URL")
_REASON = "Requires an external emulate GitHub fixture (EMULATE_GITHUB_BASE_URL)"


def _api() -> str:
    return _env("EMULATE_GITHUB_BASE_URL")


def _token() -> str:
    return _env("EMULATE_GITHUB_TOKEN", "gho_test_token_admin")


def _owner() -> str:
    return _env("EMULATE_GITHUB_OWNER", "octocat")


def _repo_name() -> str:
    return _env("EMULATE_GITHUB_REPO", "atlas-ci-smoke")


def _full_repo() -> str:
    return f"{_owner()}/{_repo_name()}"


def _branch() -> str:
    return _env("EMULATE_GITHUB_BRANCH", "main")


def _headers() -> dict[str, str]:
    return {
        "Authorization": f"Bearer {_token()}",
        "Accept": "application/vnd.github+json",
    }


def _seed_sql_file_via_git_data() -> None:
    """Use the Git Data API to write queries/test.sql into the repo.

    Creates a blob, tree, commit, and updates the branch ref so that
    the file exists for the adapter to discover.
    """
    api = _api()
    repo = _full_repo()
    branch = _branch()
    hdrs = _headers()

    sql_content = "SELECT * FROM analytics.users;"
    encoded = base64.b64encode(sql_content.encode()).decode()

    # 1. Create blob.
    resp = httpx.post(
        f"{api}/repos/{repo}/git/blobs",
        headers=hdrs,
        json={"content": encoded, "encoding": "base64"},
    )
    resp.raise_for_status()
    blob_sha = resp.json()["sha"]

    # 2. Get current branch HEAD to find the parent commit.
    resp = httpx.get(f"{api}/repos/{repo}/branches/{branch}", headers=hdrs)
    resp.raise_for_status()
    parent_sha = resp.json()["commit"]["sha"]

    # 3. Create tree with the new file.
    resp = httpx.post(
        f"{api}/repos/{repo}/git/trees",
        headers=hdrs,
        json={
            "base_tree": None,
            "tree": [
                {
                    "path": "queries/test.sql",
                    "mode": "100644",
                    "type": "blob",
                    "sha": blob_sha,
                },
            ],
        },
    )
    resp.raise_for_status()
    tree_sha = resp.json()["sha"]

    # 4. Create commit.
    resp = httpx.post(
        f"{api}/repos/{repo}/git/commits",
        headers=hdrs,
        json={
            "message": "seed: add queries/test.sql for git-data scan test",
            "tree": tree_sha,
            "parents": [parent_sha],
        },
    )
    resp.raise_for_status()
    commit_sha = resp.json()["sha"]

    # 5. Update branch ref.
    resp = httpx.patch(
        f"{api}/repos/{repo}/git/refs/heads/{branch}",
        headers=hdrs,
        json={"sha": commit_sha, "force": True},
    )
    resp.raise_for_status()


@pytest.mark.skipif(_SKIP, reason=_REASON)
class TestGitDataScanEmulate:
    """Integration: scan_mode='git' against a live emulate GitHub fixture."""

    @pytest.fixture(autouse=True, scope="class")
    def _seed(self) -> None:
        _seed_sql_file_via_git_data()

    @pytest.mark.asyncio
    async def test_git_scan_finds_seeded_table_ref(self) -> None:
        adapter = GitHubAdapter(
            token=_token(),
            repos=(_full_repo(),),
            branch=_branch(),
            scan_mode="git",
            base_url=_api(),
        )
        results = await adapter._scan_all_repos()

        assert _full_repo() in results
        tables = results[_full_repo()]
        table_names = set(tables.keys())
        assert "analytics.users" in table_names, (
            f"Expected analytics.users in {table_names}"
        )
        # Verify the file path is tracked.
        files = tables["analytics.users"]
        assert "queries/test.sql" in files
