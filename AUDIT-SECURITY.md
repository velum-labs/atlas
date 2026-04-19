# Security Audit — Atlas Monorepo

**Date:** 2026-04-07
**Auditor:** Claude Sonnet 4.6 (automated)
**Scope:** Full codebase — authentication, authorization, secrets, input validation, SQL injection, dependencies, API security, data exposure
**Overall Risk:** LOW

---

## Executive Summary

The Atlas codebase demonstrates mature security practices across most attack surfaces. Credential handling uses industry-standard Fernet encryption, SQL identifier injection is systematically prevented, YAML deserialization uses safe_load throughout, and config parsing is fail-closed with a whitelist approach. The hardening test suite (750+ lines) gives meaningful coverage of these properties.

Three findings warrant action before wider deployment:

1. **[MEDIUM]** `atlas.db` file permissions are not explicitly set — relies on umask
2. **[MEDIUM]** Webhook URLs not validated as http/https before use
3. **[MEDIUM]** No automated dependency vulnerability scanning in CI

---

## Findings

### F-01 — SQLite Database File Permissions [MEDIUM]

**File:** `packages/alma-atlas-store/src/alma_atlas_store/db.py`

The `secrets.key` and `secrets.json` files are explicitly chmod'd `0o600` after creation. The SQLite database at `~/.alma/atlas.db` is not. File creation relies on the process umask (typically `0o022` → `0o644`), leaving the database world-readable on single-user machines and group-readable in shared environments.

**Risk:** Any process running as the same user (or group on shared hosts) can read the database, which stores source metadata, scan results, and potentially derived schema information.

**Recommendation:** Add `os.chmod(path, 0o600)` after the `sqlite3.connect()` call, matching the pattern used for secrets files.

```python
# After: self._conn = sqlite3.connect(str(path), check_same_thread=False)
import os
os.chmod(path, 0o600)
```

---

### F-02 — Webhook URL Validation [MEDIUM]

**File:** `packages/alma-atlas/src/alma_atlas/config.py` (hook parsing), `packages/alma-atlas/src/alma_atlas/hooks/` (execution)

Webhook URLs are read from `atlas.yml` and passed directly to `httpx` as strings. There is no validation that they use `http://` or `https://`. A misconfigured or malicious `atlas.yml` could point hooks at `file://` or custom schemes that `httpx` may partially handle.

**Risk:** Low in practice (config is user-controlled), but important for defence-in-depth and to produce clear error messages rather than silent misbehaviour.

**Recommendation:** Validate hook URLs in the config parser:

```python
from urllib.parse import urlparse

def _validate_hook_url(url: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise ValueError(f"Hook URL must use http or https, got: {url!r}")
```

---

### F-03 — No Dependency Vulnerability Scanning in CI [MEDIUM]

**Files:** `pyproject.toml` (all packages), `.github/workflows/` (if present)

Dependencies use open-ended version ranges (e.g., `google-cloud-bigquery>=3.27`, `psycopg[binary]>=3.3.3`). A `uv.lock` is present, but there is no CI step that audits resolved packages against known CVE databases.

**Risk:** A transitive dependency could ship a high-severity CVE undetected until manual review.

**Recommendation:**

```yaml
# In CI workflow:
- name: Audit dependencies
  run: uv run pip-audit --requirement <(uv export --no-emit-workspace)
```

Or use `uv audit` if/when it becomes available. Run on every PR and weekly schedule.

---

### F-04 — ACP Agent Environment Variable Leakage [LOW]

**File:** `packages/alma-atlas/src/alma_atlas/agents/acp/session_runtime.py`

When `provider: acp` is configured, the agent subprocess inherits the full parent environment merged with any `env:` keys from `atlas.yml`. If the subprocess crashes or dumps its environment (e.g., via an unhandled exception that prints `os.environ`), all parent environment variables — including any secrets loaded by the shell profile — could appear in logs.

**Risk:** Low. The ACP subprocess is a trusted Anthropic agent binary. This is standard subprocess inheritance behaviour.

**Recommendation:** Document that sensitive shell exports (`AWS_SECRET_ACCESS_KEY`, etc.) should not be present in the environment when running `atlas learn`. No code change required unless Atlas gains an untrusted subprocess path.

---

### F-05 — Path Traversal in ACP File Operations [LOW]

**File:** `packages/alma-atlas/src/alma_atlas/agents/acp/client.py`

`write_text_file()` and `read_text_file()` accept arbitrary path strings from the ACP agent protocol. They call `Path(path).write_text()` / `Path(path).read_text()` without constraining paths to a working directory. A compromised or misbehaving agent binary could theoretically read/write arbitrary files reachable by the user.

**Risk:** Low today — the ACP binary is a pinned, trusted dependency. Risk increases if the agent command becomes configurable to arbitrary user-supplied binaries.

**Recommendation:** If the `command:` field in `atlas.yml` ever allows arbitrary executables, add path confinement:

```python
def _assert_within_workdir(self, path: str) -> Path:
    resolved = Path(path).resolve()
    workdir = self._workdir.resolve()
    if not str(resolved).startswith(str(workdir)):
        raise PermissionError(f"Path {path!r} is outside working directory")
    return resolved
```

---

### F-06 — Fernet Key Stored Unencrypted on Disk [LOW / INFORMATIONAL]

**File:** `packages/alma-atlas/src/alma_atlas/local_secrets.py`

The Fernet symmetric key is stored in plaintext at `~/.alma/secrets.key` (permissions `0o600`). The encrypted secrets blob is in `~/.alma/secrets.json`. Both files are correctly chmod'd. However, if the `~/.alma/` directory is synced (Dropbox, iCloud, git), both files travel together, making the encryption effectively transparent.

**Risk:** Low. The `0o600` chmod and filesystem ACL are the actual security boundary. This is standard practice for CLI-local credential stores (comparable to `~/.ssh/`).

**Recommendation:** Add a note to the user documentation and `.almignore` / `.gitignore` pattern:

```
# .gitignore (project level, home dir)
.alma/secrets.key
.alma/secrets.json
.alma/atlas.db
```

---

### F-07 — `check_same_thread=False` on SQLite Connection [LOW / INFORMATIONAL]

**File:** `packages/alma-atlas-store/src/alma_atlas_store/db.py`

The SQLite connection is opened with `check_same_thread=False` to support async coroutines that yield across await points but remain on the same OS thread. If a future refactor introduces thread-pool execution (e.g., `loop.run_in_executor`), the same connection object could be accessed from multiple OS threads concurrently, causing undefined behaviour.

**Risk:** Not exploitable today. A code comment already notes the constraint.

**Recommendation:** Keep the existing comment. Add a `threading.current_thread()` assertion in `__init__` if the connection creation thread should be enforced at call sites.

---

## Confirmed Strong Practices

| Area | Detail |
|------|--------|
| Credential encryption | Fernet (authenticated symmetric encryption), `cryptography` library |
| Secret file permissions | `os.chmod(path, 0o600)` for `.key` and `.json` |
| YAML deserialization | `yaml.safe_load()` throughout — no `yaml.load()` |
| JSON deserialization | `json.loads()` / `json.load()` — no pickle or eval |
| SQL identifiers | `quote_bq_identifier()` and `quote_sf_identifier()` with injection tests |
| SQLite queries | Parameterized via `?` placeholders throughout |
| Config parsing | Fail-closed whitelist: unknown top-level keys raise `ValueError` |
| Config repr | `redact_source_params()` hides `dsn`, `api_key`, `password`, etc. |
| Subprocess spawning | No `shell=True`; args passed as list |
| HTTP client | Bearer token auth, explicit timeouts, retry with backoff |
| Request IDs | UUID `X-Request-ID` header on every outbound request |
| Environment variable errors | Logs variable *name*, not value |
| Hardening tests | `test_hardening.py` 750+ lines; `test_sql_safety.py` covers injection |

---

## Recommended Actions (Priority Order)

| # | Priority | Action | File(s) |
|---|----------|--------|---------|
| 1 | Medium | Set `atlas.db` permissions to `0o600` after creation | `alma_atlas_store/db.py` |
| 2 | Medium | Validate webhook URLs as `http`/`https` in config parser | `alma_atlas/config.py` |
| 3 | Medium | Add `pip-audit` or `uv audit` step to CI workflow | `.github/workflows/` |
| 4 | Low | Document `.alma/` exclusion from cloud sync / git | User docs / `.gitignore` |
| 5 | Low | Document ACP env-var leakage caveat | `CLAUDE.md` or user docs |
| 6 | Low | Add path confinement to ACP file ops if untrusted executables become possible | `acp/client.py` |

---

*Generated by automated security review. Findings should be verified by a human engineer before acting on them.*
