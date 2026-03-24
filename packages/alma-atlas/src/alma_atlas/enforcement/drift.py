"""Drift detection — compare schema snapshots to find contract violations.

DriftDetector compares a new SchemaSnapshot against the previously stored
snapshot for each asset and returns a DriftReport describing any deviations:

    - added_column:      a column present in new snapshot but not in previous
    - removed_column:    a column present in previous snapshot but dropped
    - type_changed:      a column whose data type changed between snapshots
    - table_dropped:     asset has a previous snapshot but no current snapshot
    - row_count_anomaly: row-count changed by more than 50% (requires row_count
                         metadata on the snapshot columns dict)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from alma_atlas_store.schema_repository import SchemaSnapshot

log = logging.getLogger(__name__)


@dataclass
class DriftViolation:
    """A single drift event detected for an asset."""

    asset_id: str
    violation_type: str  # added_column | removed_column | type_changed | table_dropped | row_count_anomaly
    severity: str        # info | warning | error
    details: dict


@dataclass
class DriftReport:
    """Aggregated result of a drift detection run across one or more assets."""

    violations: list[DriftViolation] = field(default_factory=list)

    @property
    def has_violations(self) -> bool:
        return bool(self.violations)

    @property
    def error_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for v in self.violations if v.severity == "warning")


class DriftDetector:
    """Compare new schema snapshots against stored ones to detect drift."""

    # Severity mapping per violation type
    _SEVERITY: dict[str, str] = {
        "added_column": "info",
        "removed_column": "warning",
        "type_changed": "error",
        "table_dropped": "error",
        "row_count_anomaly": "warning",
    }

    def detect(
        self,
        asset_id: str,
        previous: SchemaSnapshot | None,
        current: SchemaSnapshot | None,
        previous_row_count: int | None = None,
        current_row_count: int | None = None,
    ) -> DriftReport:
        """Detect drift between previous and current snapshots for an asset.

        Args:
            asset_id:           The asset being compared.
            previous:           Last known schema snapshot (None if first scan).
            current:            Newly captured schema snapshot (None if asset dropped).
            previous_row_count: Row count from previous scan (optional).
            current_row_count:  Row count from current scan (optional).

        Returns:
            A DriftReport listing all detected violations.
        """
        report = DriftReport()

        if previous is None:
            # First scan — nothing to compare against.
            return report

        if current is None:
            report.violations.append(
                DriftViolation(
                    asset_id=asset_id,
                    violation_type="table_dropped",
                    severity=self._SEVERITY["table_dropped"],
                    details={"message": f"Asset {asset_id!r} was present in the previous scan but is no longer discoverable."},
                )
            )
            return report

        prev_cols = {c.name.lower(): c for c in previous.columns}
        curr_cols = {c.name.lower(): c for c in current.columns}

        # Added columns (info only — additive change is usually safe)
        for name in curr_cols:
            if name not in prev_cols:
                report.violations.append(
                    DriftViolation(
                        asset_id=asset_id,
                        violation_type="added_column",
                        severity=self._SEVERITY["added_column"],
                        details={"column": name, "new_type": curr_cols[name].type,
                                 "message": f"Column {name!r} added to {asset_id!r}."},
                    )
                )

        # Removed columns (warning — downstream consumers may break)
        for name in prev_cols:
            if name not in curr_cols:
                report.violations.append(
                    DriftViolation(
                        asset_id=asset_id,
                        violation_type="removed_column",
                        severity=self._SEVERITY["removed_column"],
                        details={"column": name, "previous_type": prev_cols[name].type,
                                 "message": f"Column {name!r} removed from {asset_id!r}."},
                    )
                )

        # Type changes (error — may cause silent data corruption)
        for name in prev_cols:
            if name in curr_cols and prev_cols[name].type.lower() != curr_cols[name].type.lower():
                report.violations.append(
                    DriftViolation(
                        asset_id=asset_id,
                        violation_type="type_changed",
                        severity=self._SEVERITY["type_changed"],
                        details={
                            "column": name,
                            "previous_type": prev_cols[name].type,
                            "current_type": curr_cols[name].type,
                            "message": (
                                f"Column {name!r} on {asset_id!r} changed type "
                                f"from {prev_cols[name].type!r} to {curr_cols[name].type!r}."
                            ),
                        },
                    )
                )

        # Row count anomaly (>50% change)
        if previous_row_count is not None and current_row_count is not None and previous_row_count > 0:
            change_ratio = abs(current_row_count - previous_row_count) / previous_row_count
            if change_ratio > 0.5:
                direction = "increased" if current_row_count > previous_row_count else "decreased"
                report.violations.append(
                    DriftViolation(
                        asset_id=asset_id,
                        violation_type="row_count_anomaly",
                        severity=self._SEVERITY["row_count_anomaly"],
                        details={
                            "previous_row_count": previous_row_count,
                            "current_row_count": current_row_count,
                            "change_pct": round(change_ratio * 100, 1),
                            "message": (
                                f"Row count for {asset_id!r} {direction} by "
                                f"{round(change_ratio * 100, 1)}% "
                                f"({previous_row_count} → {current_row_count})."
                            ),
                        },
                    )
                )

        return report
