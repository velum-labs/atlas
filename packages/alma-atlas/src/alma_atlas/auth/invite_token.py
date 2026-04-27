"""Invite-token authentication for Atlas Companion mode.

Per eng review:
- Issue 1A: Token storage at rest reuses LocalSecretStore (Fernet + chmod 600).
- Issue 3A: Tokens are validated on every MCP tool call (no caching) so
  revocation at the Alma endpoint takes effect on the next call. Trade-off:
  one HTTP round-trip per tool call. The user defended this against Codex's
  latency-tax challenge; if week-3 telemetry shows MCP tool-call p95 latency
  > 2s, revisit and add a 60s in-memory cache (option A from Tension 2).

Failure modes:
- Alma endpoint unreachable -> fail-closed with the agent-facing message
  "Atlas can't reach Velum right now, try again in a minute".
- Token revoked at Alma -> fail-closed with
  "Atlas access revoked, contact your Velum admin".
"""

from __future__ import annotations

import secrets
from dataclasses import dataclass

import httpx

from alma_atlas.local_secrets import LocalSecretStore

_INVITE_TOKEN_SECRET_ID = "companion_invite_token"
_INVITE_ENDPOINT_PATH = "/api/v1/companion/validate-token"
_REQUEST_TIMEOUT_SECONDS = 5.0
_MIN_TOKEN_LENGTH = 16


@dataclass(frozen=True)
class TokenValidation:
    """Result of validating an invite token against the Alma endpoint."""

    valid: bool
    reason: str | None = None  # internal reason code: revoked | unreachable | unknown | http_NNN

    @property
    def display_message(self) -> str:
        """Agent-facing error string for this failure mode.

        These strings are the design doc's required Week 0 prereq #5 "agent
        error UX strings". Keep them short, actionable, and non-technical so
        Cursor/Claude Desktop users see something useful.
        """
        if self.valid:
            return ""
        if self.reason == "revoked":
            return "Atlas access revoked, contact your Velum admin"
        if self.reason == "unreachable":
            return "Atlas can't reach Velum right now, try again in a minute"
        if self.reason == "unknown":
            return "Atlas access invalid, contact your Velum admin"
        if self.reason and self.reason.startswith("http_"):
            return "Atlas can't reach Velum right now, try again in a minute"
        return "Atlas access invalid, contact your Velum admin"


def generate_token() -> str:
    """Generate a new opaque random invite token (32-byte URL-safe).

    Issued by Velum to a named user (Javier, Diamela, named prospects). Stored
    server-side at the Alma deployment for validation; stored client-side in
    LocalSecretStore so the user only enters it once at install time.
    """
    return secrets.token_urlsafe(32)


def store_invite_token(secret_store: LocalSecretStore, token: str) -> None:
    """Persist an invite token at rest, encrypted via LocalSecretStore."""
    if not token or len(token) < _MIN_TOKEN_LENGTH:
        raise ValueError(f"Invite token must be at least {_MIN_TOKEN_LENGTH} characters")
    secret_store.store(_INVITE_TOKEN_SECRET_ID, token)


def retrieve_invite_token(secret_store: LocalSecretStore) -> str | None:
    """Read the persisted invite token, or None if not stored."""
    marker = secret_store.marker(_INVITE_TOKEN_SECRET_ID)
    try:
        decrypted = secret_store.resolve(marker)
    except ValueError:
        return None
    return decrypted if isinstance(decrypted, str) else None


def validate_token(token: str, alma_endpoint: str) -> TokenValidation:
    """Validate an invite token against the Alma deployment endpoint.

    Synchronous + per-call. Sends `{"token": token}` to
    `<alma_endpoint>/api/v1/companion/validate-token` and expects a JSON
    response of `{"valid": true | false}`.

    Args:
        token: The invite token to validate. Empty strings are rejected
            without making a network call.
        alma_endpoint: The base URL of the Alma deployment (e.g.,
            "https://app.alma.dev"). Trailing slashes are stripped.

    Returns:
        TokenValidation. `.valid` is True only on a 200 response with
        `{"valid": true}`. All other paths fail-closed.
    """
    if not token:
        return TokenValidation(valid=False, reason="unknown")

    url = alma_endpoint.rstrip("/") + _INVITE_ENDPOINT_PATH
    try:
        with httpx.Client(timeout=_REQUEST_TIMEOUT_SECONDS) as client:
            response = client.post(url, json={"token": token})
    except (httpx.RequestError, httpx.TimeoutException):
        return TokenValidation(valid=False, reason="unreachable")

    if response.status_code == 404:
        return TokenValidation(valid=False, reason="unknown")
    if response.status_code != 200:
        return TokenValidation(valid=False, reason=f"http_{response.status_code}")

    try:
        body = response.json()
    except ValueError:
        return TokenValidation(valid=False, reason="malformed_response")

    if isinstance(body, dict) and body.get("valid") is True:
        return TokenValidation(valid=True)
    return TokenValidation(valid=False, reason="revoked")
