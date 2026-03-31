"""Post-scan hook execution for Atlas.

Supports two hook types:
- webhook: HTTP POST to a URL with the scan event as a JSON payload
- log: Print structured JSON to stdout (useful in CI pipelines)

Hooks are fired asynchronously; errors in one hook never block others.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import httpx

from alma_atlas.http_utils import async_request_with_retry

if TYPE_CHECKING:
    from alma_atlas.config import PostScanHook

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HookRuntimeConfig:
    """Operational settings for hook delivery."""

    webhook_timeout_seconds: float = 10.0
    max_retries: int = 1
    backoff_base_seconds: float = 1.0


DEFAULT_HOOK_RUNTIME_CONFIG = HookRuntimeConfig()


@dataclass
class HookEvent:
    """Payload sent to hooks."""

    event_type: str  # scan_complete | scan_error | drift_detected
    source_id: str
    timestamp: str  # ISO 8601
    data: dict = field(default_factory=dict)


@dataclass
class HookResult:
    """Result of firing a single hook."""

    hook_name: str
    success: bool
    error: str | None = None


def make_hook_event(event_type: str, source_id: str, **data: object) -> HookEvent:
    """Convenience constructor that stamps the current UTC time."""
    return HookEvent(
        event_type=event_type,
        source_id=source_id,
        timestamp=datetime.now(UTC).isoformat(),
        data=dict(data),
    )


def make_scan_result_event(
    *,
    source_id: str,
    asset_count: int,
    edge_count: int,
    error: str | None = None,
    warnings: list[str] | None = None,
) -> HookEvent:
    """Build the canonical hook event for one completed scan result."""

    data: dict[str, object] = {
        "asset_count": asset_count,
        "edge_count": edge_count,
    }
    if error:
        data["error"] = error
    if warnings:
        data["warnings"] = list(warnings)
    return make_hook_event(
        "scan_error" if error else "scan_complete",
        source_id,
        **data,
    )


def make_drift_detected_event(
    *,
    source_id: str,
    blocked: bool,
    asset_count: int,
) -> HookEvent:
    """Build the canonical hook event for drift detection."""

    return make_hook_event(
        "drift_detected",
        source_id,
        blocked=blocked,
        asset_count=asset_count,
    )


class HookExecutor:
    """Runs configured hooks, with error isolation per hook."""

    def __init__(
        self,
        hooks: list[PostScanHook],
        *,
        runtime: HookRuntimeConfig = DEFAULT_HOOK_RUNTIME_CONFIG,
    ) -> None:
        self._hooks = hooks
        self._runtime = runtime

    async def fire(self, event: HookEvent) -> list[HookResult]:
        """Fire all hooks matching *event_type*. Errors in one hook don't block others."""
        matching = [h for h in self._hooks if event.event_type in h.events]
        if not matching:
            return []

        tasks = [self._dispatch(hook, event) for hook in matching]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[HookResult] = []
        for hook, outcome in zip(matching, raw, strict=True):
            if isinstance(outcome, BaseException):
                logger.warning("Hook %r raised unexpected error: %s", hook.name, outcome)
                results.append(HookResult(hook_name=hook.name, success=False, error=str(outcome)))
            else:
                results.append(outcome)
        return results

    async def _dispatch(self, hook: PostScanHook, event: HookEvent) -> HookResult:
        if hook.type == "webhook":
            return await self._fire_webhook(hook, event)
        if hook.type == "log":
            return await self._fire_log(hook, event)
        logger.warning("Unknown hook type %r for hook %r — skipping", hook.type, hook.name)
        return HookResult(hook_name=hook.name, success=False, error=f"Unknown hook type: {hook.type!r}")

    async def _fire_webhook(self, hook: PostScanHook, event: HookEvent) -> HookResult:
        """HTTP POST with shared retry handling."""
        payload = json.dumps(
            {
                "event_type": event.event_type,
                "source_id": event.source_id,
                "timestamp": event.timestamp,
                "data": event.data,
            }
        ).encode()
        merged_headers = {"Content-Type": "application/json", **hook.headers}
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(self._runtime.webhook_timeout_seconds)) as client:
                await async_request_with_retry(
                    client,
                    method="POST",
                    url=str(hook.url),
                    headers=merged_headers,
                    json_body=json.loads(payload.decode("utf-8")),
                    logger=logger,
                    request_name=hook.name,
                    max_retries=self._runtime.max_retries,
                    backoff_base=self._runtime.backoff_base_seconds,
                    retry_status_codes=frozenset({500, 502, 503, 504}),
                )
            return HookResult(hook_name=hook.name, success=True)
        except Exception as exc:  # noqa: BLE001
            return HookResult(hook_name=hook.name, success=False, error=str(exc))

    async def _fire_log(self, hook: PostScanHook, event: HookEvent) -> HookResult:
        """Structured JSON to stdout."""
        payload = {
            "hook": hook.name,
            "event_type": event.event_type,
            "source_id": event.source_id,
            "timestamp": event.timestamp,
            "data": event.data,
        }
        print(json.dumps(payload), flush=True)  # noqa: T201
        return HookResult(hook_name=hook.name, success=True)
