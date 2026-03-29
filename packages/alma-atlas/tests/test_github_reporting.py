"""Tests for the Alma CI GitHub reporting helper."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace


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


def test_check_run_command_writes_step_output(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_github_reporting_module()
    results_file = tmp_path / "enforce-results.json"
    results_file.write_text(
        json.dumps({"status": "passed", "mode": "warn", "total": 1, "passed": 1, "failed": 0}),
        encoding="utf-8",
    )
    step_output_file = tmp_path / "github-output.txt"

    monkeypatch.setattr(module, "resolve_head_sha", lambda **kwargs: "abc123")  # noqa: ANN003
    monkeypatch.setattr(
        module,
        "create_check_run",
        lambda **kwargs: {"id": 99, "status": "completed"},  # noqa: ANN003
    )
    monkeypatch.setattr(
        module.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            command="check-run",
            api_url="https://api.github.com",
            token="secret",
            repository="octocat/atlas-ci-smoke",
            results_file=str(results_file),
            head_sha=None,
            branch="main",
            event_path=None,
            details_url="https://github.com/octocat/atlas-ci-smoke/actions/runs/123",
            external_id="123:1",
            step_output_file=str(step_output_file),
        ),
    )

    exit_code = module.main()

    assert exit_code == 0
    assert step_output_file.read_text(encoding="utf-8").strip() == "check_run_id=99"


def test_comment_command_uses_pull_request_number_from_event(
    tmp_path: Path,
    monkeypatch,
) -> None:
    module = _load_github_reporting_module()
    results_file = tmp_path / "enforce-results.json"
    event_file = tmp_path / "event.json"
    results_file.write_text(
        json.dumps({"status": "failed", "mode": "warn", "total": 1, "passed": 0, "failed": 1}),
        encoding="utf-8",
    )
    event_file.write_text(json.dumps({"pull_request": {"number": 42}}), encoding="utf-8")

    captured: dict[str, object] = {}

    def _fake_create_issue_comment(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return {"id": 1, "body": kwargs["body"]}

    monkeypatch.setattr(module, "create_issue_comment", _fake_create_issue_comment)
    monkeypatch.setattr(
        module.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            command="comment",
            api_url="https://api.github.com",
            token="secret",
            repository="octocat/atlas-ci-smoke",
            results_file=str(results_file),
            report_url="https://alma.example.com/report/run-1",
            issue_number=None,
            event_path=str(event_file),
        ),
    )

    exit_code = module.main()

    assert exit_code == 0
    assert captured["issue_number"] == 42
    assert "Open Alma report" in captured["body"]
