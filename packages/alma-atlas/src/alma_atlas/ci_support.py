"""Utilities for machine-readable Atlas CI workflows."""

from __future__ import annotations

import json
from dataclasses import dataclass
from glob import glob
from pathlib import Path
from typing import Any, Literal

from alma_atlas.config import AtlasConfig, SourceConfig, get_config, load_atlas_yml
from alma_atlas.contract_service import validate_contract_document
from alma_atlas.contract_validation import (
    resolve_contract_columns as _resolve_contract_columns,
)
from alma_atlas.pipeline.scan import ScanAllResult
from alma_atlas_store.db import Database
from alma_atlas_store.schema_repository import SchemaRepository

ContractMode = Literal["shadow", "warn", "enforce"]


@dataclass(frozen=True)
class ContractDocument:
    """Normalized contract document used by the CI validation flow."""

    contract_id: str
    asset_id: str
    columns: list[dict[str, Any]]
    path: Path


def _require_yaml_module() -> Any:
    try:
        import yaml  # type: ignore[import-untyped]
    except ImportError as exc:  # pragma: no cover - exercised in packaging/runtime, not unit tests
        raise ValueError(
            "PyYAML is required for Atlas CI contract/config parsing. "
            "Install it with: pip install pyyaml"
        ) from exc
    return yaml


def _read_structured_input(raw_value: str) -> Any:
    candidate_path = Path(raw_value)
    raw_payload = candidate_path.read_text(encoding="utf-8") if candidate_path.exists() else raw_value

    try:
        return json.loads(raw_payload)
    except json.JSONDecodeError:
        yaml = _require_yaml_module()
        return yaml.safe_load(raw_payload)


def _coerce_source_configs(payload: Any) -> list[SourceConfig]:
    if isinstance(payload, dict):
        raw_sources = payload.get("sources")
        if isinstance(raw_sources, list):
            payload = raw_sources
        else:
            payload = [
                {"id": source_id, **source_payload}
                for source_id, source_payload in payload.items()
                if isinstance(source_payload, dict)
            ]

    if not isinstance(payload, list):
        raise ValueError("connections input must resolve to a list or {sources:[...]} object")

    sources: list[SourceConfig] = []
    for raw_source in payload:
        if not isinstance(raw_source, dict):
            raise ValueError("each source config must be an object")
        raw_id = raw_source.get("id")
        raw_kind = raw_source.get("kind")
        if not isinstance(raw_id, str) or not raw_id.strip():
            raise ValueError("each source config requires a non-empty id")
        if not isinstance(raw_kind, str) or not raw_kind.strip():
            raise ValueError(f"source {raw_id!r} requires a non-empty kind")

        raw_params = raw_source.get("params")
        params: dict[str, Any]
        if raw_params is None:
            params = {
                str(key): value
                for key, value in raw_source.items()
                if key not in {"id", "kind"}
            }
        elif isinstance(raw_params, dict):
            params = dict(raw_params)
        else:
            raise ValueError(f"source {raw_id!r} params must be an object when provided")

        sources.append(
            SourceConfig(
                id=raw_id.strip(),
                kind=raw_kind.strip(),
                params=params,
            )
        )
    return sources


def resolve_runtime_sources(
    *,
    config_file: str | None = None,
    connections: str | None = None,
) -> tuple[AtlasConfig, list[SourceConfig]]:
    """Resolve the runtime Atlas config plus the sources to scan."""

    if config_file is not None:
        cfg = load_atlas_yml(Path(config_file))
        sources = list(cfg.sources)
    else:
        cfg = get_config()
        sources = cfg.resolved_sources()

    if connections is not None and connections.strip():
        sources = _coerce_source_configs(_read_structured_input(connections))

    return cfg, sources


def write_payload(payload: dict[str, Any], *, output: str | None) -> None:
    """Write JSON payload to stdout or to a file."""

    serialized = json.dumps(payload, indent=2, sort_keys=True)
    if output is None:
        print(serialized)
        return

    output_path = Path(output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(serialized, encoding="utf-8")


def serialize_scan_result(result: ScanAllResult) -> dict[str, Any]:
    """Convert a scan result into a stable CI-friendly JSON payload."""

    source_results: list[dict[str, Any]] = []
    succeeded = 0
    failed = 0
    for item in result.results:
        source_status = "failed" if item.error else "passed"
        if item.error:
            failed += 1
        else:
            succeeded += 1
        source_results.append(
            {
                "source_id": item.source_id,
                "status": source_status,
                "asset_count": item.asset_count,
                "edge_count": item.edge_count,
                "error": item.error,
                "warnings": list(item.warnings),
            }
        )

    return {
        "status": "failed" if failed else "passed",
        "source_count": len(result.results),
        "sources_succeeded": succeeded,
        "sources_failed": failed,
        "cross_system_edge_count": result.cross_system_edge_count,
        "results": source_results,
    }


def serialize_dry_run_sources(sources: list[SourceConfig]) -> dict[str, Any]:
    """Return a stable JSON payload for scan dry-runs."""

    return {
        "status": "passed",
        "dry_run": True,
        "source_count": len(sources),
        "sources": [
            {
                "id": source.id,
                "kind": source.kind,
                "params": source.params,
            }
            for source in sources
        ],
    }


def render_contract_summary_markdown(payload: dict[str, Any]) -> str:
    """Render a compact markdown table for GitHub comments and check summaries."""

    details = payload.get("details", [])
    if not isinstance(details, list):
        details = []

    lines = [
        "## Alma Contract CI",
        "",
        f"- Status: **{payload.get('status', 'unknown')}**",
        f"- Mode: `{payload.get('mode', 'warn')}`",
        f"- Checked: **{payload.get('total', 0)}**",
        f"- Passed: **{payload.get('passed', 0)}**",
        f"- Failed: **{payload.get('failed', 0)}**",
        "",
        "| Contract | Asset | Status | Issues | Blocking |",
        "| --- | --- | --- | ---: | --- |",
    ]

    if not details:
        lines.append("| _none_ | _none_ | passed | 0 | no |")
        return "\n".join(lines)

    for detail in details:
        if not isinstance(detail, dict):
            continue
        contract_id = str(detail.get("contract_id", "unknown"))
        asset_id = str(detail.get("asset_id", ""))
        status = str(detail.get("status", "unknown"))
        issue_count = int(detail.get("issue_count", 0))
        blocking = "yes" if bool(detail.get("blocking")) else "no"
        lines.append(
            f"| `{contract_id}` | `{asset_id}` | `{status}` | {issue_count} | {blocking} |"
        )

    return "\n".join(lines)


def _load_contract_document(path: Path) -> ContractDocument:
    yaml = _require_yaml_module()
    raw_payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(raw_payload, dict):
        raise ValueError("contract document must contain an object payload")

    raw_asset = raw_payload.get("asset_id") or raw_payload.get("assetId")
    if not isinstance(raw_asset, str) or not raw_asset.strip():
        nested_asset = raw_payload.get("asset")
        if isinstance(nested_asset, dict):
            raw_asset = nested_asset.get("id") or nested_asset.get("asset_id")
    if not isinstance(raw_asset, str) or not raw_asset.strip():
        raise ValueError("contract document requires asset_id")

    columns = _resolve_contract_columns(raw_payload)
    if columns is None:
        raise ValueError("contract document requires columns or schema/spec.columns")

    raw_contract_id = (
        raw_payload.get("contract_id")
        or raw_payload.get("contractId")
        or raw_payload.get("id")
        or path.stem
    )
    if not isinstance(raw_contract_id, str) or not raw_contract_id.strip():
        raise ValueError("contract document requires a non-empty id or contract_id")

    return ContractDocument(
        contract_id=raw_contract_id.strip(),
        asset_id=raw_asset.strip(),
        columns=columns,
        path=path,
    )


def split_contract_patterns(raw_patterns: str) -> list[str]:
    """Split one contract-pattern input into concrete glob patterns."""

    patterns = [
        candidate.strip()
        for chunk in raw_patterns.splitlines()
        for candidate in chunk.split(",")
        if candidate.strip()
    ]
    return patterns or ["contracts/**/*.yaml"]


def validate_contracts(
    *,
    cfg: AtlasConfig,
    contract_patterns: list[str],
    mode: ContractMode,
) -> dict[str, Any]:
    """Validate contract files against the latest Atlas schema snapshots."""

    if cfg.db_path is None or not cfg.db_path.exists():
        raise ValueError("No Atlas database found. Run `alma-atlas scan` first.")

    matched_paths = sorted(
        {
            Path(match).resolve()
            for pattern in contract_patterns
            for match in glob(pattern, recursive=True)
            if Path(match).is_file()
        }
    )

    if not matched_paths:
        return {
            "mode": mode,
            "status": "passed",
            "total": 0,
            "passed": 0,
            "failed": 0,
            "blocked": 0,
            "details": [],
        }

    details: list[dict[str, Any]] = []
    with Database(cfg.db_path) as db:
        schema_repo = SchemaRepository(db)
        for path in matched_paths:
            contract_id = path.stem
            asset_id = ""
            try:
                document = _load_contract_document(path)
                contract_id = document.contract_id
                asset_id = document.asset_id
                check = validate_contract_document(
                    contract_id=document.contract_id,
                    asset_id=document.asset_id,
                    columns=document.columns,
                    snapshot=schema_repo.get_latest(document.asset_id),
                )
                issues = check.issues
            except Exception as exc:
                issues = [
                    {
                        "code": "invalid_contract",
                        "severity": "error",
                        "message": str(exc),
                    }
                ]

            failed = len(issues) > 0
            details.append(
                {
                    "contract_id": contract_id,
                    "contract_path": str(path),
                    "asset_id": asset_id,
                    "status": "failed" if failed else "passed",
                    "mode": mode,
                    "blocking": failed and mode == "enforce",
                    "issue_count": len(issues),
                    "issues": issues,
                }
            )

    passed = sum(1 for detail in details if detail["status"] == "passed")
    failed = len(details) - passed
    blocked = sum(1 for detail in details if detail["blocking"])
    return {
        "mode": mode,
        "status": "failed" if failed else "passed",
        "total": len(details),
        "passed": passed,
        "failed": failed,
        "blocked": blocked,
        "details": details,
    }

