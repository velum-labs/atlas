"""PostHog Cloud telemetry wrapper with mandatory + opt-in buckets.

Per design doc Premise 5: success metrics depend on telemetry that correlates
installs to Alma-pilot conversations. Two buckets, per the design's privacy split:

- Mandatory bucket: anonymous behavioral counts only. No PII. Always on.
  Powers aggregate metrics ("how often is companion_search_assets called").
  Schema enforced by ALLOWED_MANDATORY_PROPS allowlist below.

- Opt-in bucket: account-correlated. Includes alma_account_token. Powers
  install-to-conversation conversion math. OFF by default; user opts in via
  the `--telemetry` flag at install time (or by setting opt_in=True in cfg).

PostHog API failures are silent — they NEVER crash the host process. The
host can ship telemetry-disabled or with a misconfigured PostHog key without
any impact on Atlas's primary functionality.

Disable telemetry entirely by setting `ATLAS_TELEMETRY_OFF=1` in the env.
"""

from __future__ import annotations

import os
import platform
import uuid
from dataclasses import dataclass, field
from typing import Any

# PostHog SDK is optional at runtime — fall back to no-op if unavailable.
try:
    import posthog as _posthog  # type: ignore[import-untyped]

    _POSTHOG_AVAILABLE = True
except ImportError:  # pragma: no cover - defensive
    _posthog = None
    _POSTHOG_AVAILABLE = False


_POSTHOG_API_KEY_ENV = "ATLAS_POSTHOG_API_KEY"
_POSTHOG_HOST_ENV = "ATLAS_POSTHOG_HOST"
_TELEMETRY_OFF_ENV = "ATLAS_TELEMETRY_OFF"
_DEFAULT_POSTHOG_HOST = "https://us.i.posthog.com"

# Whitelist of property keys allowed in the mandatory (anonymous) bucket.
# Anything not in this set is dropped before sending. Prevents PII leakage
# via accidental property names.
ALLOWED_MANDATORY_PROPS = frozenset(
    {
        "tool_name",
        "mcp_session_duration_seconds",
        "connector_kind",
        "install_source",
        "atlas_version",
        "platform",
        "python_version",
    }
)


def _stable_install_id() -> str:
    """Generate a stable per-install anonymous ID with no PII.

    Hashes hostname + python version into a UUID5. Same machine + same
    Python = same id. No file-system writes; recomputed each process start.
    """
    base = f"{platform.node()}-{platform.python_version()}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, base))


@dataclass
class TelemetryConfig:
    """Runtime configuration for telemetry emission."""

    api_key: str | None = None
    host: str = _DEFAULT_POSTHOG_HOST
    opt_in: bool = False
    alma_account_token: str | None = None  # only meaningful when opt_in=True
    install_id: str = field(default_factory=_stable_install_id)
    disabled: bool = False  # full kill-switch (env ATLAS_TELEMETRY_OFF=1)


def telemetry_config_from_env() -> TelemetryConfig:
    """Build a TelemetryConfig from environment variables.

    Honors ATLAS_TELEMETRY_OFF as a hard kill-switch (anything truthy disables).
    """
    if os.environ.get(_TELEMETRY_OFF_ENV):
        return TelemetryConfig(disabled=True)
    return TelemetryConfig(
        api_key=os.environ.get(_POSTHOG_API_KEY_ENV),
        host=os.environ.get(_POSTHOG_HOST_ENV, _DEFAULT_POSTHOG_HOST),
    )


_client_cache: dict[tuple[str | None, str], Any] = {}


def _get_client(cfg: TelemetryConfig):
    """Get-or-create a PostHog client. All setup errors are swallowed."""
    if cfg.disabled or not _POSTHOG_AVAILABLE or not cfg.api_key:
        return None
    cache_key = (cfg.api_key, cfg.host)
    if cache_key in _client_cache:
        return _client_cache[cache_key]
    try:
        client = _posthog.Posthog(project_api_key=cfg.api_key, host=cfg.host, disabled=False)
    except Exception:  # pragma: no cover - defensive
        return None
    _client_cache[cache_key] = client
    return client


def reset_client_cache() -> None:
    """Clear the cached PostHog clients. For test isolation."""
    _client_cache.clear()


def mandatory_event(
    cfg: TelemetryConfig, event_name: str, properties: dict[str, Any] | None = None
) -> None:
    """Emit an anonymous behavioral event. Never blocks, never raises.

    Properties not in ALLOWED_MANDATORY_PROPS are silently dropped before
    sending.
    """
    client = _get_client(cfg)
    if client is None:
        return
    safe_props = _sanitize_mandatory_properties(properties or {})
    try:
        client.capture(
            distinct_id=cfg.install_id,
            event=event_name,
            properties=safe_props,
        )
    except Exception:  # pragma: no cover - defensive
        return


def opt_in_event(
    cfg: TelemetryConfig, event_name: str, properties: dict[str, Any] | None = None
) -> None:
    """Emit an account-correlated event. No-op unless cfg.opt_in is True.

    Sends the alma_account_token alongside the event so Velum can correlate
    installs to specific accounts. Only relevant for the conversion math
    described in design doc Premise 5.
    """
    if not cfg.opt_in:
        return
    client = _get_client(cfg)
    if client is None:
        return
    correlated_props = dict(properties or {})
    if cfg.alma_account_token:
        correlated_props["alma_account_token"] = cfg.alma_account_token
    try:
        client.capture(
            distinct_id=cfg.alma_account_token or cfg.install_id,
            event=event_name,
            properties=correlated_props,
        )
    except Exception:  # pragma: no cover - defensive
        return


def _sanitize_mandatory_properties(properties: dict[str, Any]) -> dict[str, Any]:
    """Drop any property whose key isn't in the mandatory allowlist."""
    return {k: v for k, v in properties.items() if k in ALLOWED_MANDATORY_PROPS}
