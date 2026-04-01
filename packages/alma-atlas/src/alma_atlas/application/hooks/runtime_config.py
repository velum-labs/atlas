"""Runtime configuration helpers for hook execution."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a float, got {raw!r}") from exc
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {raw!r}")
    return value


def env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc
    if value < 0:
        raise ValueError(f"{name} must be >= 0, got {raw!r}")
    return value


@dataclass(frozen=True)
class HookRuntimeConfig:
    """Operational settings for hook delivery."""

    webhook_timeout_seconds: float = field(
        default_factory=lambda: env_float("ALMA_HOOK_WEBHOOK_TIMEOUT_SECONDS", 10.0)
    )
    max_retries: int = field(default_factory=lambda: env_int("ALMA_HOOK_MAX_RETRIES", 1))
    backoff_base_seconds: float = field(
        default_factory=lambda: env_float("ALMA_HOOK_BACKOFF_BASE_SECONDS", 1.0)
    )


DEFAULT_HOOK_RUNTIME_CONFIG = HookRuntimeConfig()
