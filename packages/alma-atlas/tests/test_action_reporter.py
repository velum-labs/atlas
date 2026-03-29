"""Tests for the Alma CI action reporting helper."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def _load_reporter_module():
    script_path = (
        Path(__file__).resolve().parents[3]
        / ".github"
        / "actions"
        / "alma-ci"
        / "report_ci_run.py"
    )
    spec = importlib.util.spec_from_file_location("alma_ci_reporter", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_reporter_builds_create_ci_run_payload(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    reporter = _load_reporter_module()
    scan_results = tmp_path / "scan-results.json"
    enforce_results = tmp_path / "enforce-results.json"
    output_file = tmp_path / "report-response.json"
    scan_results.write_text(json.dumps({"status": "passed", "source_count": 1}), encoding="utf-8")
    enforce_results.write_text(
        json.dumps(
            {
                "status": "failed",
                "total": 2,
                "passed": 1,
                "failed": 1,
                "details": [
                    {
                        "contract_id": "contract.orders",
                        "asset_id": "pg:warehouse::public.orders",
                        "status": "failed",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    captured: dict[str, object] = {}

    def _fake_post_create_ci_run(**kwargs):  # noqa: ANN003
        captured.update(kwargs)
        return {"run": {"id": "run-123"}, "reportUrl": "https://alma.example.com/report/run-123"}

    monkeypatch.setattr(reporter, "_post_create_ci_run", _fake_post_create_ci_run)
    monkeypatch.setattr(
        reporter.argparse.ArgumentParser,
        "parse_args",
        lambda self: SimpleNamespace(
            api_url="https://alma.example.com",
            api_token="secret",
            scan_results=str(scan_results),
            enforce_results=str(enforce_results),
            enforcement_mode="warn",
            atlas_version="0.1.0",
            output_file=str(output_file),
            duration_ms=1200,
            github_check_run_id="456",
        ),
    )
    monkeypatch.setenv("GITHUB_REPOSITORY", "velum-labs/atlas")
    monkeypatch.setenv("GITHUB_SHA", "deadbeef")
    monkeypatch.setenv("GITHUB_HEAD_REF", "feature/eng-186")
    monkeypatch.setenv("GITHUB_EVENT_NAME", "pull_request")
    monkeypatch.setenv("GITHUB_RUN_ID", "123")
    event_path = tmp_path / "event.json"
    event_path.write_text(json.dumps({"pull_request": {"number": 42}}), encoding="utf-8")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event_path))

    exit_code = reporter.main()

    assert exit_code == 0
    payload = captured["payload"]
    assert isinstance(payload, dict)
    assert payload["repository"] == "velum-labs/atlas"
    assert payload["commitSha"] == "deadbeef"
    assert payload["branch"] == "feature/eng-186"
    assert payload["prNumber"] == 42
    assert payload["triggerType"] == "pull_request"
    assert payload["githubRunId"] == "123"
    assert payload["githubCheckRunId"] == "456"
    assert payload["scanResults"] == {"status": "passed", "source_count": 1}
    assert payload["enforceResults"]["failed"] == 1
    output_payload = json.loads(output_file.read_text(encoding="utf-8"))
    assert output_payload["ok"] is True
    assert output_payload["runId"] == "run-123"
    assert output_payload["reportUrl"] == "https://alma.example.com/report/run-123"
