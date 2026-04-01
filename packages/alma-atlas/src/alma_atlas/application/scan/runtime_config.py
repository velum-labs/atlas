"""Runtime configuration helpers for scan execution."""

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
    if value <= 0:
        raise ValueError(f"{name} must be > 0, got {raw!r}")
    return value


@dataclass(frozen=True)
class ScanRuntimeConfig:
    """Operational defaults for Atlas scan execution."""

    timeout_seconds: float = field(default_factory=lambda: env_float("ALMA_SCAN_TIMEOUT_SECONDS", 300.0))
    max_concurrent: int = field(default_factory=lambda: env_int("ALMA_SCAN_MAX_CONCURRENT", 4))


DEFAULT_SCAN_RUNTIME_CONFIG = ScanRuntimeConfig()
