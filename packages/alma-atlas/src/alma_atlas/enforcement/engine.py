"""Enforcement engine — decides what action to take based on DriftReport + contract mode.

Modes:
    shadow:  Log violations to the store but always report success.
             Useful for observing drift without blocking pipelines.
    warn:    Log violations and return them to the caller for notification.
             Pipeline is not blocked but violations are surfaced.
    enforce: Log violations and signal a non-zero / blocking result.
             Intended to halt pipelines when breaking changes are detected.
"""

from __future__ import annotations

import hashlib
import logging
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from alma_atlas.enforcement.drift import DriftReport, DriftViolation

if TYPE_CHECKING:
    from alma_atlas_store.db import Database

log = logging.getLogger(__name__)

_VALID_MODES = frozenset({"shadow", "warn", "enforce"})


def _deterministic_violation_id(asset_id: str, violation_type: str, details: dict) -> str:
    """Return a stable UUID derived from violation identity fields.

    The same drift event produces the same ID across runs, enabling
    idempotent inserts and cross-run deduplication.
    """
    column = details.get("column") or details.get("column_name") or ""
    contract_id = details.get("contract_id", "")
    key = f"{asset_id}:{violation_type}:{contract_id}:{column}"
    digest = hashlib.sha256(key.encode()).hexdigest()
    return str(uuid.UUID(digest[:32]))


@dataclass
class EnforcementResult:
    """Result returned by EnforcementEngine.enforce()."""

    mode: Literal["shadow", "warn", "enforce"]
    violations: list[DriftViolation] = field(default_factory=list)
    blocked: bool = False  # True only when mode=enforce and there are error-severity violations

    @property
    def has_violations(self) -> bool:
        return bool(self.violations)


class EnforcementEngine:
    """Apply enforcement policy to a DriftReport and persist violations."""

    def __init__(self, db: Database) -> None:
        self._db = db

    def enforce(
        self,
        report: DriftReport,
        mode: Literal["shadow", "warn", "enforce"],
    ) -> EnforcementResult:
        """Process a DriftReport according to the given enforcement mode.

        All violations are always written to the violations store so there is
        a persistent audit trail regardless of mode.

        Args:
            report: The DriftReport produced by DriftDetector.
            mode:   The enforcement mode for the contract ('shadow', 'warn',
                    or 'enforce').  Unknown modes are treated as 'shadow' with
                    a warning log so a malformed contract never crashes the
                    pipeline.

        Returns:
            EnforcementResult describing what was logged and whether the
            pipeline should be considered blocked.
        """
        from alma_atlas_store.violation_repository import Violation, ViolationRepository

        # Validate mode — unknown modes fall back to shadow (fail-safe).
        if mode not in _VALID_MODES:
            log.warning(
                "[enforcement] Unknown mode %r — treating as 'shadow' to avoid crash.",
                mode,
            )
            mode = "shadow"  # type: ignore[assignment]

        # Empty reports are fine — nothing to check.
        if not report.violations:
            log.debug("[enforcement/%s] No violations in report.", mode)
            return EnforcementResult(mode=mode, violations=[], blocked=False)

        log.info(
            "[enforcement/%s] Checking %d violation(s).",
            mode,
            len(report.violations),
        )

        repo = ViolationRepository(self._db)
        for v in report.violations:
            # Deterministic ID ensures same drift event deduplicates across runs.
            violation_id = _deterministic_violation_id(v.asset_id, v.violation_type, v.details)
            repo.insert(
                Violation(
                    id=violation_id,
                    asset_id=v.asset_id,
                    violation_type=v.violation_type,
                    severity=v.severity,
                    details=v.details,
                )
            )
            log.info(
                "[enforcement/%s] %s %s — %s",
                mode,
                v.asset_id,
                v.violation_type,
                v.details.get("message", ""),
            )

        log.info(
            "[enforcement/%s] Persisted %d violation(s). error=%d warning=%d info=%d",
            mode,
            len(report.violations),
            report.error_count,
            report.warning_count,
            len(report.violations) - report.error_count - report.warning_count,
        )

        if mode == "shadow":
            # Log only — never block.
            return EnforcementResult(mode=mode, violations=report.violations, blocked=False)

        if mode == "warn":
            # Surface violations but do not block.
            return EnforcementResult(mode=mode, violations=report.violations, blocked=False)

        # enforce: block when any error-severity violation is present.
        blocked = any(v.severity == "error" for v in report.violations)
        if blocked:
            log.warning(
                "[enforcement/enforce] BLOCKING — %d error violation(s) detected for asset(s): %s",
                report.error_count,
                ", ".join(sorted({v.asset_id for v in report.violations if v.severity == "error"})),
            )
        return EnforcementResult(mode=mode, violations=report.violations, blocked=blocked)
