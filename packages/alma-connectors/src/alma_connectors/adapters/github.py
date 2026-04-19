"""GitHub source adapter -- discovers table references in source code."""

from __future__ import annotations

import asyncio
import fnmatch
import logging
import os
import re
import shutil
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx

from alma_connectors.adapters._base import BaseAdapterV2
from alma_connectors.source_adapter import (
    ConnectionTestResult,
    GitHubAdapterConfig,
    PersistedSourceAdapter,
    SchemaObjectKind,
    SchemaSnapshot,
    SetupInstructions,
    SourceAdapterCapabilities,
    SourceAdapterKind,
    SourceColumnSchema,
    SourceTableSchema,
    TrafficObservationResult,
)
from alma_connectors.source_adapter_v2 import (
    AdapterCapability,
    CapabilityProbeResult,
    DefinitionSnapshot,
    DiscoveredContainer,
    DiscoverySnapshot,
    ExtractionScope,
    LineageEdge,
    LineageEdgeKind,
    LineageSnapshot,
    ObjectDefinition,
    SchemaObject,
    SchemaSnapshotV2,
    ScopeContext,
)
from alma_connectors.source_adapter_v2 import (
    SchemaObjectKind as SchemaObjectKindV2,
)
from alma_sqlkit.table_refs import TableRef, extract_tables_from_sql

logger = logging.getLogger(__name__)

# Regex patterns for Python files.
_SQLALCHEMY_TABLENAME = re.compile(r"""__tablename__\s*=\s*['"]([^'"]+)['"]""")
_PANDAS_READ_SQL = re.compile(
    r"""(?:read_sql|read_sql_query|read_sql_table)\s*\(\s*['\"]{1,3}(.*?)['\"]{1,3}""",
    re.DOTALL,
)
_PANDAS_TO_SQL = re.compile(r"""\.to_sql\s*\(\s*['"]([^'"]+)['"]""")
_RAW_SQL_STRING = re.compile(
    r"""(?:execute|text|raw_sql|cursor\.execute)\s*\(\s*['\"]{1,3}(.*?)['\"]{1,3}""",
    re.DOTALL,
)


async def _get_installation_token(
    app_id: str,
    private_key: str,
    installation_id: str,
    base_url: str,
) -> str:
    """Exchange a GitHub App private key for an installation access token."""
    import jwt as pyjwt

    now = int(time.time())
    payload = {"iat": now - 60, "exp": now + 600, "iss": app_id}
    encoded_jwt = pyjwt.encode(payload, private_key, algorithm="RS256")
    url = f"{base_url}/app/installations/{installation_id}/access_tokens"
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            url,
            headers={
                "Authorization": f"Bearer {encoded_jwt}",
                "Accept": "application/vnd.github+json",
            },
        )
        resp.raise_for_status()
        return resp.json()["token"]


async def _clone_repo(
    repo: str,
    token: str,
    branch: str,
    dest: str,
) -> None:
    """Shallow clone a GitHub repo into dest."""
    clone_url = f"https://x-access-token:{token}@github.com/{repo}.git"
    args = ["git", "clone", "--depth", "1"]
    if branch:
        args += ["--branch", branch]
    args += [clone_url, dest]
    proc = await asyncio.create_subprocess_exec(
        *args,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"git clone failed for {repo}: {stderr.decode(errors='replace').strip()}"
        )


def _matches_patterns(
    rel_path: str,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
) -> bool:
    """Check if a relative path matches include patterns and not exclude patterns."""
    included = any(fnmatch.fnmatch(rel_path, pat) for pat in include)
    if not included:
        return False
    excluded = any(fnmatch.fnmatch(rel_path, pat) for pat in exclude)
    return not excluded


def _extract_tables_from_sql_file(content: str) -> list[TableRef]:
    """Parse SQL content and return table references."""
    try:
        return extract_tables_from_sql(content, dialect="postgres")
    except Exception:
        logger.debug("Failed to parse SQL file, skipping")
        return []


def _extract_tables_from_python_file(content: str) -> list[str]:
    """Extract table names from Python source code."""
    tables: list[str] = []

    # SQLAlchemy __tablename__
    for match in _SQLALCHEMY_TABLENAME.finditer(content):
        tables.append(match.group(1))

    # pandas to_sql("table_name", ...)
    for match in _PANDAS_TO_SQL.finditer(content):
        tables.append(match.group(1))

    # pandas read_sql / raw SQL strings -- try parsing the SQL
    for pattern in (_PANDAS_READ_SQL, _RAW_SQL_STRING):
        for match in pattern.finditer(content):
            sql_fragment = match.group(1).strip()
            if not sql_fragment:
                continue
            refs = _extract_tables_from_sql_file(sql_fragment)
            for ref in refs:
                fqn = f"{ref.schema}.{ref.table}" if ref.schema else ref.table
                tables.append(fqn)

    return tables


def _scan_repo_dir(
    repo_dir: str,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    max_file_size: int,
) -> dict[str, set[str]]:
    """Walk a cloned repo and extract table references.

    Returns a dict mapping table FQN -> set of source file paths.
    """
    table_to_files: dict[str, set[str]] = {}
    root = Path(repo_dir)

    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = str(path.relative_to(root))
        if not _matches_patterns(rel, include_patterns, exclude_patterns):
            continue
        if path.stat().st_size > max_file_size:
            continue

        try:
            content = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue

        table_names: list[str] = []

        if path.suffix == ".sql":
            refs = _extract_tables_from_sql_file(content)
            for ref in refs:
                fqn = f"{ref.schema}.{ref.table}" if ref.schema else ref.table
                table_names.append(fqn)
        elif path.suffix == ".py":
            table_names = _extract_tables_from_python_file(content)

        for name in table_names:
            table_to_files.setdefault(name, set()).add(rel)

    return table_to_files


class GitHubAdapter(BaseAdapterV2):
    """GitHub source adapter.

    Clones repositories and scans source files (.sql, .py) for table
    references using sqlglot parsing and regex heuristics.  Produces
    schema objects for every discovered table and lineage edges from
    source files to tables.
    """

    kind = SourceAdapterKind.GITHUB
    capabilities = SourceAdapterCapabilities(
        can_test_connection=True,
        can_introspect_schema=True,
        can_observe_traffic=False,
        can_execute_query=False,
    )

    declared_capabilities: frozenset[AdapterCapability] = frozenset({
        AdapterCapability.DISCOVER,
        AdapterCapability.SCHEMA,
        AdapterCapability.DEFINITIONS,
        AdapterCapability.LINEAGE,
    })

    def __init__(
        self,
        *,
        token: str | None = None,
        app_id: str | None = None,
        private_key: str | None = None,
        installation_id: str | None = None,
        repos: tuple[str, ...] = (),
        branch: str = "",
        include_patterns: tuple[str, ...] = ("*.sql", "*.py"),
        exclude_patterns: tuple[str, ...] = ("**/node_modules/**", "**/.git/**", "**/venv/**"),
        max_file_size_bytes: int = 1_000_000,
        base_url: str = "https://api.github.com",
    ) -> None:
        self._token = token
        self._app_id = app_id
        self._private_key = private_key
        self._installation_id = installation_id
        self._repos = repos
        self._branch = branch
        self._include_patterns = include_patterns
        self._exclude_patterns = exclude_patterns
        self._max_file_size_bytes = max_file_size_bytes
        self._base_url = base_url

    async def _resolve_token(self) -> str:
        """Return a usable access token (PAT or GitHub App installation token)."""
        if self._token:
            return self._token
        if self._app_id and self._private_key and self._installation_id:
            return await _get_installation_token(
                self._app_id,
                self._private_key,
                self._installation_id,
                self._base_url,
            )
        raise ValueError("GitHub adapter requires either a token or app_id + private_key + installation_id")

    async def _scan_all_repos(self) -> dict[str, dict[str, set[str]]]:
        """Clone and scan all configured repos.

        Returns {repo: {table_fqn: {file_paths}}}.
        """
        token = await self._resolve_token()
        results: dict[str, dict[str, set[str]]] = {}

        for repo in self._repos:
            tmp_dir = tempfile.mkdtemp(prefix="atlas-github-")
            try:
                await _clone_repo(repo, token, self._branch, tmp_dir)
                tables = _scan_repo_dir(
                    tmp_dir,
                    self._include_patterns,
                    self._exclude_patterns,
                    self._max_file_size_bytes,
                )
                results[repo] = tables
            except Exception:
                logger.exception("Failed to scan repo %s", repo)
                results[repo] = {}
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

        return results

    def _scope_identifiers(self) -> dict[str, str]:
        return {"base_url": self._base_url, "repos": ",".join(self._repos)}

    # ------------------------------------------------------------------
    # v1 protocol methods
    # ------------------------------------------------------------------

    async def _validate_connection(
        self,
        adapter: PersistedSourceAdapter,
    ) -> ConnectionTestResult:
        """Verify GitHub credentials by listing repos via the API."""
        try:
            token = await self._resolve_token()
        except Exception as exc:
            return ConnectionTestResult(success=False, message=str(exc))

        async with httpx.AsyncClient() as client:
            for repo in self._repos:
                resp = await client.get(
                    f"{self._base_url}/repos/{repo}",
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Accept": "application/vnd.github+json",
                    },
                )
                if resp.status_code != 200:
                    return ConnectionTestResult(
                        success=False,
                        message=f"Cannot access repo {repo}: HTTP {resp.status_code}",
                    )

        return ConnectionTestResult(
            success=True,
            message=f"Authenticated and verified access to {len(self._repos)} repo(s)",
            resource_count=len(self._repos),
            resource_label="repositories",
        )

    async def _build_schema_snapshot_data(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshot:
        """Clone repos, scan files, and build schema snapshot."""
        scan_results = await self._scan_all_repos()

        # Merge all tables across repos.
        all_tables: dict[str, set[str]] = {}
        for repo, tables in scan_results.items():
            for fqn, files in tables.items():
                prefixed = {f"{repo}:{f}" for f in files}
                all_tables.setdefault(fqn, set()).update(prefixed)

        objects: list[SourceTableSchema] = []
        for fqn in sorted(all_tables):
            parts = fqn.rsplit(".", 1)
            schema_name = parts[0] if len(parts) > 1 else "public"
            table_name = parts[-1]
            objects.append(
                SourceTableSchema(
                    schema_name=schema_name,
                    object_name=table_name,
                    object_kind=SchemaObjectKind.TABLE,
                    columns=(),
                )
            )

        return SchemaSnapshot(
            captured_at=datetime.now(UTC),
            objects=tuple(objects),
            dependencies=(),
        )

    async def _empty_traffic_result(
        self,
        adapter: PersistedSourceAdapter,
        *,
        since: datetime | None = None,
    ) -> TrafficObservationResult:
        """GitHub adapter does not observe query traffic."""
        return TrafficObservationResult(scanned_records=0, events=())

    def get_setup_instructions(self) -> SetupInstructions:
        return SetupInstructions(
            title="GitHub Source Code Adapter",
            summary=(
                "Scan GitHub repositories for SQL and Python files to discover "
                "table references and build source code lineage."
            ),
            steps=(
                "Create a GitHub App or Personal Access Token with repo read permissions",
                "Configure repos list with org/repo format (e.g. 'myorg/analytics')",
                "Set file_patterns to control which files are scanned (default: *.sql, *.py)",
                "Run introspect_schema to clone repos and extract table references",
            ),
        )

    # ------------------------------------------------------------------
    # v2 protocol methods
    # ------------------------------------------------------------------

    async def probe(
        self,
        adapter: PersistedSourceAdapter,
        capabilities: frozenset[AdapterCapability] | None = None,
    ) -> tuple[CapabilityProbeResult, ...]:
        caps_to_probe = capabilities if capabilities is not None else self.declared_capabilities
        scope_ctx = ScopeContext(
            scope=ExtractionScope.GLOBAL,
            identifiers=self._scope_identifiers(),
        )

        try:
            token = await self._resolve_token()
            available = True
            message = None
        except Exception as exc:
            available = False
            message = str(exc)

        return self._make_probe_results(caps_to_probe, available, scope_ctx, message)

    async def discover(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DiscoverySnapshot:
        t0 = time.monotonic()

        containers: list[DiscoveredContainer] = []
        for repo in self._repos:
            containers.append(
                DiscoveredContainer(
                    container_id=f"github://{repo}",
                    container_type="repository",
                    display_name=repo,
                    metadata={"branch": self._branch or "default"},
                )
            )

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.DISCOVER, len(containers), duration_ms)
        return DiscoverySnapshot(meta=meta, containers=tuple(containers))

    async def extract_schema(
        self,
        adapter: PersistedSourceAdapter,
    ) -> SchemaSnapshotV2:
        t0 = time.monotonic()
        scan_results = await self._scan_all_repos()

        all_tables: dict[str, set[str]] = {}
        for _repo, tables in scan_results.items():
            for fqn, files in tables.items():
                all_tables.setdefault(fqn, set()).update(files)

        objects: list[SchemaObject] = []
        for fqn in sorted(all_tables):
            parts = fqn.rsplit(".", 1)
            schema_name = parts[0] if len(parts) > 1 else "public"
            table_name = parts[-1]
            objects.append(
                SchemaObject(
                    schema_name=schema_name,
                    object_name=table_name,
                    kind=SchemaObjectKindV2.EXTERNAL_TABLE,
                    columns=(),
                    description=None,
                    tags=(),
                )
            )

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.SCHEMA, len(objects), duration_ms)
        return SchemaSnapshotV2(meta=meta, objects=tuple(objects))

    async def extract_definitions(
        self,
        adapter: PersistedSourceAdapter,
    ) -> DefinitionSnapshot:
        """Extract SQL definitions from .sql files in scanned repos."""
        t0 = time.monotonic()
        token = await self._resolve_token()
        definitions: list[ObjectDefinition] = []

        for repo in self._repos:
            tmp_dir = tempfile.mkdtemp(prefix="atlas-github-def-")
            try:
                await _clone_repo(repo, token, self._branch, tmp_dir)
                root = Path(tmp_dir)
                for path in root.rglob("*.sql"):
                    rel = str(path.relative_to(root))
                    if not _matches_patterns(rel, self._include_patterns, self._exclude_patterns):
                        continue
                    if path.stat().st_size > self._max_file_size_bytes:
                        continue
                    try:
                        content = path.read_text(encoding="utf-8", errors="replace")
                    except OSError:
                        continue
                    if not content.strip():
                        continue
                    # Use the file stem as the object name.
                    object_name = path.stem
                    schema_name = path.parent.name or "public"
                    definitions.append(
                        ObjectDefinition(
                            schema_name=schema_name,
                            object_name=object_name,
                            object_kind=SchemaObjectKindV2.TABLE,
                            definition_text=content.strip(),
                            definition_language="sql",
                        )
                    )
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.DEFINITIONS, len(definitions), duration_ms)
        return DefinitionSnapshot(meta=meta, definitions=tuple(definitions))

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """Build lineage edges: source file -> table reference."""
        t0 = time.monotonic()
        scan_results = await self._scan_all_repos()

        edges: list[LineageEdge] = []
        for repo, tables in scan_results.items():
            for fqn, files in tables.items():
                parts = fqn.rsplit(".", 1)
                target = f"{parts[0]}.{parts[-1]}" if len(parts) > 1 else f"public.{fqn}"
                for file_path in sorted(files):
                    source_obj = f"github://{repo}/{file_path}"
                    edges.append(
                        LineageEdge(
                            source_object=source_obj,
                            target_object=target,
                            edge_kind=LineageEdgeKind.INFERRED,
                            confidence=0.8,
                        )
                    )

        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.LINEAGE, len(edges), duration_ms)
        return LineageSnapshot(meta=meta, edges=tuple(edges))
