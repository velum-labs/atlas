#!/usr/bin/env python3
"""Best-effort GitHub reporting helpers for the Alma CI action."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from alma_atlas.ci_support import render_contract_summary_markdown


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


def _split_repository(repository: str) -> tuple[str, str]:
    normalized = repository.strip()
    if not normalized or "/" not in normalized:
        raise ValueError("repository must have the form owner/name")
    owner, repo = normalized.split("/", maxsplit=1)
    owner = owner.strip()
    repo = repo.strip()
    if not owner or not repo:
        raise ValueError("repository must have the form owner/name")
    return owner, repo


def _read_event_payload(event_path: str | None) -> dict[str, Any]:
    if event_path is None:
        return {}
    path = Path(event_path)
    if not path.exists() or not path.is_file():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        return {}
    return payload


def resolve_pull_request_number(event_path: str | None) -> int | None:
    payload = _read_event_payload(event_path)
    pull_request = payload.get("pull_request")
    if not isinstance(pull_request, dict):
        return None
    number = pull_request.get("number")
    if isinstance(number, int):
        return number
    if isinstance(number, str) and number.isdigit():
        return int(number)
    return None


def resolve_head_sha(
    *,
    api_url: str,
    token: str,
    repository: str,
    branch: str,
    event_path: str | None = None,
    explicit_head_sha: str | None = None,
) -> str:
    if explicit_head_sha is not None and explicit_head_sha.strip():
        return explicit_head_sha.strip()

    payload = _read_event_payload(event_path)
    pull_request = payload.get("pull_request")
    if isinstance(pull_request, dict):
        head = pull_request.get("head")
        if isinstance(head, dict):
            sha = head.get("sha")
            if isinstance(sha, str) and sha.strip():
                return sha.strip()

    return get_branch_head_sha(
        api_url=api_url,
        token=token,
        repository=repository,
        branch=branch,
    )


def _github_request(
    *,
    api_url: str,
    token: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> Any:
    normalized_base_url = api_url.rstrip("/")
    request_payload = None
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if payload is not None:
        request_payload = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"

    request = Request(
        url=f"{normalized_base_url}{path}",
        data=request_payload,
        headers=headers,
        method=method,
    )
    with urlopen(request, timeout=15) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def get_branch_head_sha(
    *,
    api_url: str,
    token: str,
    repository: str,
    branch: str = "main",
) -> str:
    owner, repo = _split_repository(repository)
    payload = _github_request(
        api_url=api_url,
        token=token,
        method="GET",
        path=f"/repos/{quote(owner)}/{quote(repo)}/git/ref/heads/{quote(branch)}",
    )
    if not isinstance(payload, dict):
        raise ValueError("GitHub ref response must be an object")
    obj = payload.get("object")
    if not isinstance(obj, dict):
        raise ValueError("GitHub ref response is missing object metadata")
    sha = obj.get("sha")
    if not isinstance(sha, str) or not sha.strip():
        raise ValueError("GitHub ref response is missing object.sha")
    return sha.strip()


def create_issue(
    *,
    api_url: str,
    token: str,
    repository: str,
    title: str,
    body: str,
) -> dict[str, Any]:
    owner, repo = _split_repository(repository)
    payload = _github_request(
        api_url=api_url,
        token=token,
        method="POST",
        path=f"/repos/{quote(owner)}/{quote(repo)}/issues",
        payload={"title": title, "body": body},
    )
    if not isinstance(payload, dict):
        raise ValueError("GitHub issue response must be an object")
    return payload


def create_issue_comment(
    *,
    api_url: str,
    token: str,
    repository: str,
    issue_number: int,
    body: str,
) -> dict[str, Any]:
    owner, repo = _split_repository(repository)
    payload = _github_request(
        api_url=api_url,
        token=token,
        method="POST",
        path=f"/repos/{quote(owner)}/{quote(repo)}/issues/{issue_number}/comments",
        payload={"body": body},
    )
    if not isinstance(payload, dict):
        raise ValueError("GitHub issue comment response must be an object")
    return payload


def create_check_run(
    *,
    api_url: str,
    token: str,
    repository: str,
    head_sha: str,
    payload: dict[str, Any],
) -> dict[str, Any]:
    owner, repo = _split_repository(repository)
    response = _github_request(
        api_url=api_url,
        token=token,
        method="POST",
        path=f"/repos/{quote(owner)}/{quote(repo)}/check-runs",
        payload={
            **payload,
            "head_sha": head_sha,
        },
    )
    if not isinstance(response, dict):
        raise ValueError("GitHub check run response must be an object")
    return response


def build_check_run_request(
    *,
    results_payload: dict[str, Any],
    details_url: str | None,
    external_id: str | None,
) -> dict[str, Any]:
    summary_lines = [
        f"Mode: {results_payload.get('mode', 'warn')}",
        f"Checked: {results_payload.get('total', 0)}",
        f"Passed: {results_payload.get('passed', 0)}",
        f"Failed: {results_payload.get('failed', 0)}",
    ]
    payload: dict[str, Any] = {
        "name": "Alma Contract CI",
        "status": "completed",
        "conclusion": "success" if results_payload.get("status") == "passed" else "failure",
        "output": {
            "title": f"Alma Contract CI: {results_payload.get('status', 'unknown')}",
            "summary": "\n".join(summary_lines),
        },
    }
    if details_url is not None and details_url.strip():
        payload["details_url"] = details_url.strip()
    if external_id is not None and external_id.strip():
        payload["external_id"] = external_id.strip()
    return payload


def build_comment_body(
    *,
    results_payload: dict[str, Any],
    report_url: str | None,
) -> str:
    comment_body = render_contract_summary_markdown(results_payload)
    if report_url is not None and report_url.strip():
        comment_body += f"\n\n[Open Alma report]({report_url.strip()})\n"
    return comment_body


def _write_step_output(path_value: str | None, *, key: str, value: str) -> None:
    if path_value is None or not path_value.strip():
        return
    with Path(path_value).open("a", encoding="utf-8") as handle:
        handle.write(f"{key}={value}\n")


def _run_check_run_command(args: argparse.Namespace) -> int:
    results_payload = _load_json_object(args.results_file, field="results_file")
    head_sha = resolve_head_sha(
        api_url=args.api_url,
        token=args.token,
        repository=args.repository,
        branch=args.branch,
        event_path=args.event_path,
        explicit_head_sha=args.head_sha,
    )
    response = create_check_run(
        api_url=args.api_url,
        token=args.token,
        repository=args.repository,
        head_sha=head_sha,
        payload=build_check_run_request(
            results_payload=results_payload,
            details_url=args.details_url,
            external_id=args.external_id,
        ),
    )
    check_run_id = str(response.get("id", "") or "")
    _write_step_output(args.step_output_file, key="check_run_id", value=check_run_id)
    return 0


def _run_comment_command(args: argparse.Namespace) -> int:
    issue_number = args.issue_number
    if issue_number is None:
        issue_number = resolve_pull_request_number(args.event_path)
    if issue_number is None:
        print("warning: GitHub comment skipped because no pull request number was available", file=sys.stderr)
        return 0

    results_payload = _load_json_object(args.results_file, field="results_file")
    body = build_comment_body(
        results_payload=results_payload,
        report_url=args.report_url,
    )
    delays = [1, 2, 4]
    last_error: Exception | None = None
    for attempt_index, delay_seconds in enumerate(delays):
        try:
            create_issue_comment(
                api_url=args.api_url,
                token=args.token,
                repository=args.repository,
                issue_number=issue_number,
                body=body,
            )
            return 0
        except (HTTPError, URLError, ValueError, TimeoutError) as exc:
            last_error = exc
            if attempt_index < len(delays) - 1:
                time.sleep(delay_seconds)

    if last_error is not None:
        print(f"warning: GitHub comment skipped: {last_error}", file=sys.stderr)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Best-effort GitHub reporting for Alma CI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    check_run_parser = subparsers.add_parser("check-run")
    check_run_parser.add_argument("--api-url", required=True)
    check_run_parser.add_argument("--token", required=True)
    check_run_parser.add_argument("--repository", required=True)
    check_run_parser.add_argument("--results-file", required=True)
    check_run_parser.add_argument("--head-sha", default=None)
    check_run_parser.add_argument("--branch", default="main")
    check_run_parser.add_argument("--event-path", default=None)
    check_run_parser.add_argument("--details-url", default=None)
    check_run_parser.add_argument("--external-id", default=None)
    check_run_parser.add_argument("--step-output-file", default=None)

    comment_parser = subparsers.add_parser("comment")
    comment_parser.add_argument("--api-url", required=True)
    comment_parser.add_argument("--token", required=True)
    comment_parser.add_argument("--repository", required=True)
    comment_parser.add_argument("--results-file", required=True)
    comment_parser.add_argument("--report-url", default=None)
    comment_parser.add_argument("--issue-number", type=int, default=None)
    comment_parser.add_argument("--event-path", default=None)

    args = parser.parse_args()
    try:
        if args.command == "check-run":
            return _run_check_run_command(args)
        return _run_comment_command(args)
    except (HTTPError, URLError, ValueError, TimeoutError) as exc:
        print(f"warning: GitHub reporting skipped: {exc}", file=sys.stderr)
        if args.command == "check-run":
            _write_step_output(args.step_output_file, key="check_run_id", value="")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
