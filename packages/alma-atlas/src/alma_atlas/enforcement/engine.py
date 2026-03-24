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

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Literal

from alma_atlas.enforcement.drift import DriftReport, DriftViolation

if TYPE_CHECKING:
    from alma_atlas_store.db import Database

log = logging.getLogger(__name__)


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

    def __init__(self, db: "Database") -> None:
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
            mode:   The enforcement mode for the contract.

        Returns:
            EnforcementResult describing what was logged and whether the
            pipeline should be considered blocked.
        """
        from alma_atlas_store.violation_repository import Violation, ViolationRepository

        repo = ViolationRepository(self._db)
        for v in report.violations:
            repo.insert(
                Violation(
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

        if mode == "shadow":
            # Log only — never block.
            return EnforcementResult(mode=mode, violations=report.violations, blocked=False)

        if mode == "warn":
            # Surface violations but do not block.
            return EnforcementResult(mode=mode, violations=report.violations, blocked=False)

        # enforce: block when any error-severity violation is present.
        blocked = any(v.severity == "error" for v in report.violations)
        return EnforcementResult(mode=mode, violations=report.violations, blocked=blocked)
