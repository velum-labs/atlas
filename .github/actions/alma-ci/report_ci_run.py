#!/usr/bin/env python3
"""Best-effort reporter for Alma CI run tracking."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _load_json_object(path_value: str, *, field: str) -> dict[str, Any]:
    path = Path(path_value)
    if not path.exists():
        raise ValueError(f"{field} does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"{field} must be a file: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{field} must contain a JSON object")
    return payload


def _read_pull_request_number() -> int | None:
    event_path = os.getenv("GITHUB_EVENT_PATH", "").strip()
    if not event_path:
        return None
    path = Path(event_path)
    if not path.exists() or not path.is_file():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return None
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return None
    number = pull_request.get("number")
    if isinstance(number, int):
        return number
    if isinstance(number, str) and number.isdigit():
        return int(number)
    return None


def _resolve_branch() -> str:
    head_ref = os.getenv("GITHUB_HEAD_REF", "").strip()
    if head_ref:
        return head_ref
    ref_name = os.getenv("GITHUB_REF_NAME", "").strip()
    if ref_name:
        return ref_name
    return "unknown"


def _resolve_trigger_type() -> str:
    event_name = os.getenv("GITHUB_EVENT_NAME", "").strip()
    if event_name in {"push", "pull_request", "schedule"}:
        return event_name
    if event_name in {"workflow_dispatch", "repository_dispatch"}:
        return "manual"
    return "manual"


def _post_create_ci_run(
    *,
    api_url: str,
    api_token: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    normalized_base_url = api_url.rstrip("/")
    request = Request(
        url=f"{normalized_base_url}/velum.observatory.v1.ObservatoryService/CreateCIRun",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_token}",
        },
        method="POST",
    )
    with urlopen(request, timeout=15) as response:
        body = response.read().decode("utf-8")
    parsed = json.loads(body) if body else {}
    if not isinstance(parsed, dict):
        raise ValueError("CreateCIRun response must be a JSON object")
    return parsed


def _write_output(path_value: str, payload: dict[str, Any]) -> None:
    path = Path(path_value)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Report Atlas CI results to Alma")
    parser.add_argument("--api-url", required=True)
    parser.add_argument("--api-token", required=True)
    parser.add_argument("--scan-results", required=True)
    parser.add_argument("--enforce-results", required=True)
    parser.add_argument("--enforcement-mode", required=True)
    parser.add_argument("--atlas-version", required=True)
    parser.add_argument("--output-file", required=True)
    parser.add_argument("--duration-ms", type=int, default=0)
    parser.add_argument("--github-check-run-id", default="")
    args = parser.parse_args()

    scan_results = _load_json_object(args.scan_results, field="scan_results")
    enforce_results = _load_json_object(args.enforce_results, field="enforce_results")

    request_payload: dict[str, Any] = {
        "repository": os.getenv("GITHUB_REPOSITORY", "").strip(),
        "commitSha": os.getenv("GITHUB_SHA", "").strip(),
        "branch": _resolve_branch(),
        "triggerType": _resolve_trigger_type(),
        "githubRunId": os.getenv("GITHUB_RUN_ID", "").strip(),
        "enforcementMode": args.enforcement_mode.strip(),
        "scanResults": scan_results,
        "enforceResults": enforce_results,
        "atlasVersion": args.atlas_version.strip(),
    }
    if args.duration_ms > 0:
        request_payload["durationMs"] = args.duration_ms
    if args.github_check_run_id.strip():
        request_payload["githubCheckRunId"] = args.github_check_run_id.strip()
    pr_number = _read_pull_request_number()
    if pr_number is not None:
        request_payload["prNumber"] = pr_number

    result_payload: dict[str, Any] = {
        "ok": False,
        "runId": "",
        "reportUrl": "",
        "response": {},
    }
    try:
        response = _post_create_ci_run(
            api_url=args.api_url.strip(),
            api_token=args.api_token.strip(),
            payload=request_payload,
        )
        run = response.get("run")
        if isinstance(run, dict):
            run_id = run.get("id")
            if isinstance(run_id, str):
                result_payload["runId"] = run_id
        report_url = response.get("reportUrl")
        if isinstance(report_url, str):
            result_payload["reportUrl"] = report_url
        result_payload["ok"] = True
        result_payload["response"] = response
    except (HTTPError, URLError, ValueError, TimeoutError) as exc:
        print(f"warning: alma reporting skipped: {exc}", file=sys.stderr)
        result_payload["error"] = str(exc)

    _write_output(args.output_file, result_payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
