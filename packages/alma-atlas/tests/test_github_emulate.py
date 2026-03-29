"""Optional integration tests against an emulate.dev GitHub fixture."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path

import pytest


def _load_github_reporting_module():
    script_path = (
        Path(__file__).resolve().parents[3]
        / ".github"
        / "actions"
        / "alma-ci"
        / "github_reporting.py"
    )
    spec = importlib.util.spec_from_file_location("alma_ci_github_reporting", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.skipif(
    not os.getenv("EMULATE_GITHUB_BASE_URL"),
    reason="Requires an external emulate GitHub fixture",
)
def test_github_reporting_round_trips_against_emulate() -> None:
    module = _load_github_reporting_module()
    api_url = os.environ["EMULATE_GITHUB_BASE_URL"]
    token = os.getenv("EMULATE_GITHUB_TOKEN", "gho_test_token_admin")
    owner = os.getenv("EMULATE_GITHUB_OWNER", "octocat")
    repo = os.getenv("EMULATE_GITHUB_REPO", "atlas-ci-smoke")
    repository = f"{owner}/{repo}"

    issue = module.create_issue(
        api_url=api_url,
        token=token,
        repository=repository,
        title="Alma CI emulate smoke",
        body="Seeded issue used for GitHub reporting smoke tests.",
    )
    issue_number = issue.get("number")
    assert isinstance(issue_number, int)

    comment = module.create_issue_comment(
        api_url=api_url,
        token=token,
        repository=repository,
        issue_number=issue_number,
        body="Emulate smoke comment",
    )
    assert str(comment.get("body", "")).strip() == "Emulate smoke comment"

    head_sha = module.get_branch_head_sha(
        api_url=api_url,
        token=token,
        repository=repository,
        branch=os.getenv("EMULATE_GITHUB_BRANCH", "main"),
    )
    assert head_sha

    check_run = module.create_check_run(
        api_url=api_url,
        token=token,
        repository=repository,
        head_sha=head_sha,
        payload={
            "name": "Alma Contract CI",
            "status": "completed",
            "conclusion": "success",
            "output": {
                "title": "Alma Contract CI: passed",
                "summary": "Mode: warn\nChecked: 1\nPassed: 1\nFailed: 0",
            },
        },
    )
    assert check_run.get("name") == "Alma Contract CI"
