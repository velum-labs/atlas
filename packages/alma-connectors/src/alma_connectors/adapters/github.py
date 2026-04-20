"""GitHub source adapter -- discovers table references in source code."""

from __future__ import annotations

import asyncio
import base64
import fnmatch
import logging
import os
import re
import shutil
import tarfile
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
from alma_sqlkit.lineage import extract_lineage as extract_sql_lineage
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

# dbt Jinja patterns.
_DBT_REF_RE = re.compile(r"""\{\{\s*ref\s*\(\s*['"]([^'"]+)['"]\s*\)\s*\}\}""")
_DBT_SOURCE_RE = re.compile(
    r"""\{\{\s*source\s*\(\s*['"]([^'"]+)['"]\s*,\s*['"]([^'"]+)['"]\s*\)\s*\}\}"""
)

# Python import patterns.
_PYTHON_IMPORT_RE = re.compile(
    r"""^(?:from\s+([\w.]+)\s+import|import\s+([\w.]+))""",
    re.MULTILINE,
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


def _git_base_url(api_base_url: str) -> str:
    """Derive the git clone base URL from the REST API base URL.

    - https://api.github.com        -> https://github.com
    - https://ghes.corp.com/api/v3  -> https://ghes.corp.com
    - https://ghes.corp.com/api/v3/ -> https://ghes.corp.com
    """
    stripped = api_base_url.rstrip("/")
    if stripped == "https://api.github.com":
        return "https://github.com"
    if stripped.endswith("/api/v3"):
        return stripped[: -len("/api/v3")]
    # Handle base URLs that contain /api/v3/ with a trailing path segment.
    idx = stripped.find("/api/v3/")
    if idx != -1:
        return stripped[:idx]
    return stripped


async def _clone_repo(
    repo: str,
    token: str,
    branch: str,
    dest: str,
    *,
    git_base: str = "https://github.com",
) -> None:
    """Shallow clone a GitHub repo into dest."""
    from urllib.parse import urlparse

    parsed = urlparse(git_base.rstrip("/"))
    clone_url = f"{parsed.scheme}://x-access-token:{token}@{parsed.netloc}/{repo}.git"
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


async def _download_repo_archive(
    repo: str,
    token: str,
    branch: str,
    dest: str,
    *,
    base_url: str = "https://api.github.com",
) -> None:
    """Download and extract a repo tarball via the GitHub REST API."""
    ref = branch or "HEAD"
    url = f"{base_url}/repos/{repo}/tarball/{ref}"
    async with httpx.AsyncClient(follow_redirects=True, timeout=120) as client:
        resp = await client.get(
            url,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
        )
        resp.raise_for_status()

    tar_path = os.path.join(dest, "_archive.tar.gz")
    with open(tar_path, "wb") as fh:
        fh.write(resp.content)

    with tarfile.open(tar_path, "r:gz") as tf:
        # GitHub tarballs have a single top-level directory; strip it.
        members = tf.getmembers()
        prefix = ""
        if members:
            prefix = members[0].name.split("/")[0] + "/"
        for member in members:
            if member.name == prefix.rstrip("/"):
                continue
            # Security: skip absolute paths and path traversal
            if member.name.startswith("/") or ".." in member.name.split("/"):
                continue
            member.path = member.name[len(prefix):] if member.name.startswith(prefix) else member.name
            tf.extract(member, dest, filter="data")

    os.remove(tar_path)


async def _scan_repo_via_git_data(
    repo: str,
    token: str,
    branch: str,
    *,
    base_url: str = "https://api.github.com",
    include_patterns: tuple[str, ...] = ("*.sql", "*.py"),
    exclude_patterns: tuple[str, ...] = ("**/node_modules/**", "**/.git/**", "**/venv/**"),
    max_file_size_bytes: int = 1_000_000,
) -> dict[str, set[str]]:
    """Scan a repo using the Git Data API (no clone or tarball).

    Uses /git/trees, /git/blobs, and /git/commits endpoints which are
    supported by emulate.dev GitHub fixtures.
    """
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    table_to_files: dict[str, set[str]] = {}

    async with httpx.AsyncClient(timeout=60) as client:
        # 1. Resolve head commit SHA.
        if branch:
            resp = await client.get(
                f"{base_url}/repos/{repo}/branches/{branch}",
                headers=headers,
            )
        else:
            # Get default branch first.
            resp = await client.get(
                f"{base_url}/repos/{repo}",
                headers=headers,
            )
            resp.raise_for_status()
            default_branch = resp.json()["default_branch"]
            resp = await client.get(
                f"{base_url}/repos/{repo}/branches/{default_branch}",
                headers=headers,
            )
        resp.raise_for_status()
        commit_sha = resp.json()["commit"]["sha"]

        # 2. Get commit -> tree SHA.
        resp = await client.get(
            f"{base_url}/repos/{repo}/git/commits/{commit_sha}",
            headers=headers,
        )
        resp.raise_for_status()
        tree_sha = resp.json()["tree"]["sha"]

        # 3. Get full recursive tree.
        resp = await client.get(
            f"{base_url}/repos/{repo}/git/trees/{tree_sha}",
            headers=headers,
            params={"recursive": "1"},
        )
        resp.raise_for_status()
        tree_entries = resp.json().get("tree", [])

        # 4. Filter and fetch blobs.
        for entry in tree_entries:
            if entry.get("type") != "blob":
                continue
            path = entry["path"]
            if not _matches_patterns(path, include_patterns, exclude_patterns):
                continue
            size = entry.get("size", 0)
            if size > max_file_size_bytes:
                continue

            blob_sha = entry["sha"]
            resp = await client.get(
                f"{base_url}/repos/{repo}/git/blobs/{blob_sha}",
                headers=headers,
            )
            resp.raise_for_status()
            blob_data = resp.json()

            encoding = blob_data.get("encoding", "base64")
            raw_content = blob_data.get("content", "")
            if encoding == "base64":
                try:
                    content = base64.b64decode(raw_content).decode("utf-8", errors="replace")
                except Exception:
                    continue
            elif encoding == "utf-8":
                content = raw_content
            else:
                continue

            table_names: list[str] = []
            suffix = Path(path).suffix

            if suffix == ".sql":
                refs = _extract_tables_from_sql_file(content)
                for ref in refs:
                    table_names.append(ref.canonical_name)
            elif suffix == ".py":
                table_names = _extract_tables_from_python_file(content)
            elif suffix in (".yml", ".yaml"):
                # dbt ref/source extraction.
                for model_name in _extract_dbt_refs(content):
                    table_names.append(model_name)
                for source_name, table_name in _extract_dbt_sources(content):
                    table_names.append(f"{source_name}.{table_name}")

            for name in table_names:
                table_to_files.setdefault(name, set()).add(path)

    return table_to_files


def _path_parts_contain(rel_path: str, exclude_dirs: frozenset[str]) -> bool:
    """Check if any path component is in the excluded directory set."""
    return bool(set(Path(rel_path).parts) & exclude_dirs)


# Pre-built set of common excluded directory names extracted from glob patterns.
_EXCLUDED_DIR_NAMES = frozenset({"node_modules", ".git", "venv", "__pycache__"})


def _matches_patterns(
    rel_path: str,
    include: tuple[str, ...],
    exclude: tuple[str, ...],
) -> bool:
    """Check if a relative path matches include patterns and not exclude patterns."""
    p = Path(rel_path)
    included = any(p.match(pat) for pat in include)
    if not included:
        return False
    # Check exclude: any path component in the excluded dirs set, or fnmatch.
    parts = set(p.parts)
    for pat in exclude:
        # Handle **/dirname/** patterns by checking path components.
        stripped = pat.replace("**/", "").replace("/**", "").strip("/")
        if stripped and not any(c in stripped for c in ("*", "?", "[")):
            if stripped in parts:
                return False
        elif fnmatch.fnmatch(rel_path, pat):
            return False
    return True


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
                tables.append(ref.canonical_name)

    return tables


def _extract_dbt_refs(content: str) -> list[str]:
    """Extract model names from dbt ref() calls."""
    return [m.group(1) for m in _DBT_REF_RE.finditer(content)]


def _extract_dbt_sources(content: str) -> list[tuple[str, str]]:
    """Extract (source_name, table_name) from dbt source() calls."""
    return [(m.group(1), m.group(2)) for m in _DBT_SOURCE_RE.finditer(content)]


def _extract_python_imports(content: str) -> list[str]:
    """Extract module names from Python import statements."""
    modules: list[str] = []
    for m in _PYTHON_IMPORT_RE.finditer(content):
        mod = m.group(1) or m.group(2)
        if mod:
            modules.append(mod)
    return modules


def _resolve_import_to_file(module: str, repo_py_files: set[str]) -> str | None:
    """Try to resolve a Python module name to a file path in the repo."""
    parts = module.replace(".", "/")
    for candidate in (f"{parts}.py", f"{parts}/__init__.py"):
        if candidate in repo_py_files:
            return candidate
    return None


def _lineage_from_sql_file(content: str, source_uri: str) -> list[LineageEdge]:
    """Extract lineage edges from a SQL file using sqlkit and dbt regex."""
    edges: list[LineageEdge] = []

    # dbt ref/source (works even on Jinja-templated SQL).
    for model_name in _extract_dbt_refs(content):
        edges.append(
            LineageEdge(
                source_object=source_uri,
                target_object=model_name,
                edge_kind=LineageEdgeKind.DECLARED,
                confidence=1.0,
                metadata={"dbt_type": "ref"},
            )
        )
    for source_name, table_name in _extract_dbt_sources(content):
        edges.append(
            LineageEdge(
                source_object=source_uri,
                target_object=f"{source_name}.{table_name}",
                edge_kind=LineageEdgeKind.DECLARED,
                confidence=1.0,
                metadata={"dbt_type": "source"},
            )
        )

    # SQL table references with read/write distinction via sqlkit.
    try:
        result = extract_sql_lineage(content, dialect="postgres")
        for ref in result.source_tables:
            edges.append(
                LineageEdge(
                    source_object=source_uri,
                    target_object=ref.canonical_name,
                    edge_kind=LineageEdgeKind.INFERRED_SQL,
                    confidence=0.85,
                    metadata={"direction": "reads"},
                )
            )
        if result.target_table:
            target = result.target_table
            if "." not in target:
                target = f"public.{target}"
            edges.append(
                LineageEdge(
                    source_object=source_uri,
                    target_object=target,
                    edge_kind=LineageEdgeKind.INFERRED_SQL,
                    confidence=0.85,
                    metadata={"direction": "writes"},
                )
            )
    except Exception:
        # Fallback to simple table extraction.
        refs = _extract_tables_from_sql_file(content)
        for ref in refs:
            edges.append(
                LineageEdge(
                    source_object=source_uri,
                    target_object=ref.canonical_name,
                    edge_kind=LineageEdgeKind.INFERRED_SQL,
                    confidence=0.8,
                )
            )

    return edges


def _lineage_from_python_file(
    content: str,
    source_uri: str,
    repo_name: str,
    py_files: set[str],
) -> list[LineageEdge]:
    """Extract lineage edges from a Python file."""
    edges: list[LineageEdge] = []
    seen_targets: set[str] = set()

    # SQLAlchemy __tablename__.
    for match in _SQLALCHEMY_TABLENAME.finditer(content):
        table = match.group(1)
        target = table if "." in table else f"public.{table}"
        if target not in seen_targets:
            seen_targets.add(target)
            edges.append(
                LineageEdge(
                    source_object=source_uri,
                    target_object=target,
                    edge_kind=LineageEdgeKind.INFERRED_SQL,
                    confidence=0.8,
                    metadata={"extraction": "sqlalchemy_model"},
                )
            )

    # pandas to_sql.
    for match in _PANDAS_TO_SQL.finditer(content):
        table = match.group(1)
        target = table if "." in table else f"public.{table}"
        if target not in seen_targets:
            seen_targets.add(target)
            edges.append(
                LineageEdge(
                    source_object=source_uri,
                    target_object=target,
                    edge_kind=LineageEdgeKind.INFERRED_SQL,
                    confidence=0.75,
                    metadata={"direction": "writes", "extraction": "pandas_to_sql"},
                )
            )

    # Embedded SQL strings parsed through sqlkit.
    for pattern in (_PANDAS_READ_SQL, _RAW_SQL_STRING):
        for match in pattern.finditer(content):
            sql_fragment = match.group(1).strip()
            if not sql_fragment:
                continue
            try:
                result = extract_sql_lineage(sql_fragment, dialect="postgres")
                for ref in result.source_tables:
                    if ref.canonical_name not in seen_targets:
                        seen_targets.add(ref.canonical_name)
                        edges.append(
                            LineageEdge(
                                source_object=source_uri,
                                target_object=ref.canonical_name,
                                edge_kind=LineageEdgeKind.INFERRED_SQL,
                                confidence=0.7,
                                metadata={"direction": "reads", "extraction": "python_embedded_sql"},
                            )
                        )
                if result.target_table:
                    t = result.target_table
                    tgt = t if "." in t else f"public.{t}"
                    if tgt not in seen_targets:
                        seen_targets.add(tgt)
                        edges.append(
                            LineageEdge(
                                source_object=source_uri,
                                target_object=tgt,
                                edge_kind=LineageEdgeKind.INFERRED_SQL,
                                confidence=0.7,
                                metadata={"direction": "writes", "extraction": "python_embedded_sql"},
                            )
                        )
            except Exception:
                refs = _extract_tables_from_sql_file(sql_fragment)
                for ref in refs:
                    if ref.canonical_name not in seen_targets:
                        seen_targets.add(ref.canonical_name)
                        edges.append(
                            LineageEdge(
                                source_object=source_uri,
                                target_object=ref.canonical_name,
                                edge_kind=LineageEdgeKind.INFERRED_SQL,
                                confidence=0.6,
                            )
                        )

    # Python script -> script import edges.
    for module in _extract_python_imports(content):
        resolved = _resolve_import_to_file(module, py_files)
        if resolved:
            target_uri = f"github://{repo_name}/{resolved}"
            if target_uri not in seen_targets:
                seen_targets.add(target_uri)
                edges.append(
                    LineageEdge(
                        source_object=source_uri,
                        target_object=target_uri,
                        edge_kind=LineageEdgeKind.HEURISTIC,
                        confidence=0.9,
                        metadata={"import_type": "python"},
                    )
                )

    return edges


def _scan_repo_lineage_edges(
    repo_dir: str,
    repo_name: str,
    include_patterns: tuple[str, ...],
    exclude_patterns: tuple[str, ...],
    max_file_size: int,
) -> list[LineageEdge]:
    """Walk a cloned repo and extract detailed lineage edges.

    Returns LineageEdge objects with source URIs prefixed by the repo name.
    """
    root = Path(repo_dir)
    edges: list[LineageEdge] = []

    # Collect Python file paths for import resolution.
    py_files: set[str] = set()
    for path in root.rglob("*.py"):
        rel = str(path.relative_to(root))
        if _matches_patterns(rel, include_patterns, exclude_patterns):
            py_files.add(rel)

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

        source_uri = f"github://{repo_name}/{rel}"

        if path.suffix == ".sql":
            edges.extend(_lineage_from_sql_file(content, source_uri))
        elif path.suffix == ".py":
            edges.extend(
                _lineage_from_python_file(content, source_uri, repo_name, py_files)
            )

    return edges


def stitch_cross_system_edges(
    github_edges: tuple[LineageEdge, ...],
    warehouse_tables: frozenset[str],
) -> tuple[LineageEdge, ...]:
    """Create cross-system edges where GitHub table references match warehouse tables.

    Matches on exact FQN and suffix match (e.g. schema.table matches
    project.schema.table in the warehouse).
    """
    if not warehouse_tables:
        return ()

    # Build lookups: normalized full name and two-part suffix.
    wh_by_name: dict[str, str] = {}
    wh_by_suffix: dict[str, str] = {}
    for fqn in warehouse_tables:
        norm = fqn.strip().lower()
        wh_by_name[norm] = fqn
        parts = norm.split(".")
        if len(parts) >= 2:
            suffix = ".".join(parts[-2:])
            wh_by_suffix.setdefault(suffix, fqn)

    stitched: list[LineageEdge] = []
    for edge in github_edges:
        target = edge.target_object.strip().lower()
        if target.startswith("github://"):
            continue

        matched_fqn: str | None = None
        match_type = "exact"

        if target in wh_by_name:
            matched_fqn = wh_by_name[target]
        else:
            parts = target.split(".")
            if len(parts) >= 2:
                suffix = ".".join(parts[-2:])
                if suffix in wh_by_suffix:
                    matched_fqn = wh_by_suffix[suffix]
                    match_type = "suffix"

        if matched_fqn and matched_fqn.strip().lower() != target:
            stitched.append(
                LineageEdge(
                    source_object=edge.source_object,
                    target_object=matched_fqn,
                    edge_kind=LineageEdgeKind.HEURISTIC,
                    confidence=0.9 if match_type == "exact" else 0.7,
                    metadata={
                        "cross_system": True,
                        "match_type": match_type,
                        "original_target": edge.target_object,
                    },
                )
            )

    return tuple(stitched)


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
                table_names.append(ref.canonical_name)
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
        scan_mode: str = "clone",
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
        self._git_base = _git_base_url(base_url)
        self._scan_mode = scan_mode

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
        """Clone or download and scan all configured repos.

        Returns {repo: {table_fqn: {file_paths}}}.
        """
        token = await self._resolve_token()
        results: dict[str, dict[str, set[str]]] = {}

        for repo in self._repos:
            try:
                if self._scan_mode == "git":
                    tables = await _scan_repo_via_git_data(
                        repo,
                        token,
                        self._branch,
                        base_url=self._base_url,
                        include_patterns=self._include_patterns,
                        exclude_patterns=self._exclude_patterns,
                        max_file_size_bytes=self._max_file_size_bytes,
                    )
                    results[repo] = tables
                else:
                    tmp_dir = tempfile.mkdtemp(prefix="atlas-github-")
                    try:
                        if self._scan_mode == "archive":
                            await _download_repo_archive(
                                repo, token, self._branch, tmp_dir, base_url=self._base_url,
                            )
                        else:
                            await _clone_repo(repo, token, self._branch, tmp_dir, git_base=self._git_base)
                        tables = _scan_repo_dir(
                            tmp_dir,
                            self._include_patterns,
                            self._exclude_patterns,
                            self._max_file_size_bytes,
                        )
                        results[repo] = tables
                    finally:
                        shutil.rmtree(tmp_dir, ignore_errors=True)
            except Exception:
                logger.exception("Failed to scan repo %s", repo)
                results[repo] = {}

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
                await _clone_repo(repo, token, self._branch, tmp_dir, git_base=self._git_base)
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

    async def _scan_all_repos_lineage(self) -> list[LineageEdge]:
        """Clone and scan all repos for detailed lineage edges."""
        token = await self._resolve_token()
        all_edges: list[LineageEdge] = []

        for repo in self._repos:
            tmp_dir = tempfile.mkdtemp(prefix="atlas-github-lineage-")
            try:
                await _clone_repo(repo, token, self._branch, tmp_dir, git_base=self._git_base)
                edges = _scan_repo_lineage_edges(
                    tmp_dir,
                    repo,
                    self._include_patterns,
                    self._exclude_patterns,
                    self._max_file_size_bytes,
                )
                all_edges.extend(edges)
            except Exception:
                logger.exception("Failed to scan repo %s for lineage", repo)
            finally:
                shutil.rmtree(tmp_dir, ignore_errors=True)

        return all_edges

    async def extract_lineage(
        self,
        adapter: PersistedSourceAdapter,
    ) -> LineageSnapshot:
        """Build lineage edges: source file -> table (reads/writes), dbt refs, python imports."""
        t0 = time.monotonic()
        edges = await self._scan_all_repos_lineage()
        duration_ms = (time.monotonic() - t0) * 1000
        meta = self._make_meta(adapter, AdapterCapability.LINEAGE, len(edges), duration_ms)
        return LineageSnapshot(meta=meta, edges=tuple(edges))
