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
import socket
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alma_atlas.config import PostScanHook

logger = logging.getLogger(__name__)

_WEBHOOK_TIMEOUT = 10  # seconds


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


class HookExecutor:
    """Runs configured hooks, with error isolation per hook."""

    def __init__(self, hooks: list[PostScanHook]) -> None:
        self._hooks = hooks

    async def fire(self, event: HookEvent) -> list[HookResult]:
        """Fire all hooks matching *event_type*. Errors in one hook don't block others."""
        matching = [h for h in self._hooks if event.event_type in h.events]
        if not matching:
            return []

        tasks = [self._dispatch(hook, event) for hook in matching]
        raw = await asyncio.gather(*tasks, return_exceptions=True)

        results: list[HookResult] = []
        for hook, outcome in zip(matching, raw):
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
        """HTTP POST with 10-second timeout and one retry on 5xx / timeout."""
        payload = json.dumps(
            {
                "event_type": event.event_type,
                "source_id": event.source_id,
                "timestamp": event.timestamp,
                "data": event.data,
            }
        ).encode()
        merged_headers = {"Content-Type": "application/json", **hook.headers}

        def _do_request() -> HookResult:
            req = urllib.request.Request(
                hook.url,  # type: ignore[arg-type]
                data=payload,
                headers=merged_headers,
                method="POST",
            )
            last_error: str = "retries exhausted"
            for attempt in range(2):
                try:
                    with urllib.request.urlopen(req, timeout=_WEBHOOK_TIMEOUT) as resp:
                        resp.read()  # consume body
                    return HookResult(hook_name=hook.name, success=True)
                except urllib.error.HTTPError as exc:
                    if exc.code >= 500 and attempt == 0:
                        last_error = f"HTTP {exc.code}"
                        logger.warning("Hook %r returned HTTP %d — retrying", hook.name, exc.code)
                        continue
                    return HookResult(hook_name=hook.name, success=False, error=f"HTTP {exc.code}")
                except urllib.error.URLError as exc:
                    is_timeout = isinstance(exc.reason, (socket.timeout, TimeoutError, OSError)) and (
                        "timed out" in str(exc.reason).lower()
                        or isinstance(exc.reason, (socket.timeout, TimeoutError))
                    )
                    if is_timeout and attempt == 0:
                        last_error = "timeout"
                        logger.warning("Hook %r timed out — retrying", hook.name)
                        continue
                    return HookResult(hook_name=hook.name, success=False, error=str(exc.reason))
                except TimeoutError:
                    if attempt == 0:
                        last_error = "timeout"
                        logger.warning("Hook %r timed out — retrying", hook.name)
                        continue
                    return HookResult(hook_name=hook.name, success=False, error="timed out")
                except Exception as exc:  # noqa: BLE001
                    return HookResult(hook_name=hook.name, success=False, error=str(exc))
            return HookResult(hook_name=hook.name, success=False, error=last_error)

        return await asyncio.to_thread(_do_request)

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
