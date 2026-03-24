"""Tests for the enforcement engine — DriftDetector, EnforcementEngine, and violation storage."""

from __future__ import annotations

import pytest

from alma_atlas_store.db import Database
from alma_atlas_store.schema_repository import ColumnInfo, SchemaSnapshot

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _snap(asset_id: str, cols: list[tuple[str, str]]) -> SchemaSnapshot:
    """Build a minimal SchemaSnapshot from (name, type) pairs."""
    return SchemaSnapshot(
        asset_id=asset_id,
        columns=[ColumnInfo(name=n, type=t) for n, t in cols],
    )


@pytest.fixture
def db():
    with Database(":memory:") as database:
        yield database


# ---------------------------------------------------------------------------
# DriftDetector — column-level changes
# ---------------------------------------------------------------------------

class TestDriftDetector:
    from alma_atlas.enforcement.drift import DriftDetector

    def _detector(self):
        from alma_atlas.enforcement.drift import DriftDetector
        return DriftDetector()

    def test_no_violations_when_identical(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int"), ("name", "text")])
        curr = _snap("a::t", [("id", "int"), ("name", "text")])
        report = detector.detect("a::t", prev, curr)
        assert not report.has_violations

    def test_column_added(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        curr = _snap("a::t", [("id", "int"), ("email", "text")])
        report = detector.detect("a::t", prev, curr)
        assert len(report.violations) == 1
        v = report.violations[0]
        assert v.violation_type == "added_column"
        assert v.severity == "info"
        assert v.details["column"] == "email"

    def test_column_removed(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int"), ("email", "text")])
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, curr)
        assert len(report.violations) == 1
        v = report.violations[0]
        assert v.violation_type == "removed_column"
        assert v.severity == "warning"
        assert v.details["column"] == "email"

    def test_type_changed(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int"), ("amount", "float")])
        curr = _snap("a::t", [("id", "int"), ("amount", "text")])
        report = detector.detect("a::t", prev, curr)
        assert len(report.violations) == 1
        v = report.violations[0]
        assert v.violation_type == "type_changed"
        assert v.severity == "error"
        assert v.details["previous_type"] == "float"
        assert v.details["current_type"] == "text"

    def test_table_dropped(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, current=None)
        assert len(report.violations) == 1
        v = report.violations[0]
        assert v.violation_type == "table_dropped"
        assert v.severity == "error"

    def test_first_scan_no_violations(self):
        detector = self._detector()
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", previous=None, current=curr)
        assert not report.has_violations

    def test_multiple_changes_detected(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int"), ("name", "text"), ("score", "float")])
        curr = _snap("a::t", [("id", "int"), ("score", "int"), ("email", "text")])
        report = detector.detect("a::t", prev, curr)
        types = {v.violation_type for v in report.violations}
        assert "removed_column" in types   # name removed
        assert "type_changed" in types     # score: float → int
        assert "added_column" in types     # email added

    def test_case_insensitive_column_names(self):
        detector = self._detector()
        prev = _snap("a::t", [("ID", "int"), ("Name", "TEXT")])
        curr = _snap("a::t", [("id", "int"), ("name", "TEXT")])
        report = detector.detect("a::t", prev, curr)
        assert not report.has_violations


# ---------------------------------------------------------------------------
# DriftDetector — row count anomaly
# ---------------------------------------------------------------------------

class TestRowCountAnomaly:
    def _detector(self):
        from alma_atlas.enforcement.drift import DriftDetector
        return DriftDetector()

    def test_row_count_anomaly_decrease(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, curr, previous_row_count=1000, current_row_count=400)
        assert any(v.violation_type == "row_count_anomaly" for v in report.violations)

    def test_row_count_anomaly_increase(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, curr, previous_row_count=100, current_row_count=200)
        assert any(v.violation_type == "row_count_anomaly" for v in report.violations)

    def test_no_anomaly_within_threshold(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, curr, previous_row_count=100, current_row_count=140)
        assert not any(v.violation_type == "row_count_anomaly" for v in report.violations)

    def test_no_anomaly_when_counts_missing(self):
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, curr)
        assert not any(v.violation_type == "row_count_anomaly" for v in report.violations)

    def test_exact_50pct_change_is_not_anomaly(self):
        """Threshold is *strictly* greater than 50%."""
        detector = self._detector()
        prev = _snap("a::t", [("id", "int")])
        curr = _snap("a::t", [("id", "int")])
        report = detector.detect("a::t", prev, curr, previous_row_count=100, current_row_count=150)
        assert not any(v.violation_type == "row_count_anomaly" for v in report.violations)


# ---------------------------------------------------------------------------
# EnforcementEngine — mode behaviour
# ---------------------------------------------------------------------------

class TestEnforcementEngine:
    def _make_report_with_error(self):
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        v = DriftViolation(
            asset_id="a::t",
            violation_type="type_changed",
            severity="error",
            details={"message": "type changed"},
        )
        return DriftReport(violations=[v])

    def _make_report_with_warning(self):
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        v = DriftViolation(
            asset_id="a::t",
            violation_type="removed_column",
            severity="warning",
            details={"message": "column removed"},
        )
        return DriftReport(violations=[v])

    def test_shadow_logs_only_not_blocked(self, db):
        from alma_atlas.enforcement.engine import EnforcementEngine
        engine = EnforcementEngine(db)
        report = self._make_report_with_error()
        result = engine.enforce(report, "shadow")
        assert not result.blocked
        assert result.has_violations
        assert result.mode == "shadow"

    def test_warn_returns_violations_not_blocked(self, db):
        from alma_atlas.enforcement.engine import EnforcementEngine
        engine = EnforcementEngine(db)
        report = self._make_report_with_warning()
        result = engine.enforce(report, "warn")
        assert not result.blocked
        assert result.has_violations
        assert result.mode == "warn"

    def test_enforce_blocks_on_error_severity(self, db):
        from alma_atlas.enforcement.engine import EnforcementEngine
        engine = EnforcementEngine(db)
        report = self._make_report_with_error()
        result = engine.enforce(report, "enforce")
        assert result.blocked
        assert result.mode == "enforce"

    def test_enforce_does_not_block_on_warning_only(self, db):
        from alma_atlas.enforcement.engine import EnforcementEngine
        engine = EnforcementEngine(db)
        report = self._make_report_with_warning()
        result = engine.enforce(report, "enforce")
        assert not result.blocked

    def test_enforce_no_violations_not_blocked(self, db):
        from alma_atlas.enforcement.drift import DriftReport
        from alma_atlas.enforcement.engine import EnforcementEngine
        engine = EnforcementEngine(db)
        result = engine.enforce(DriftReport(), "enforce")
        assert not result.blocked
        assert not result.has_violations


# ---------------------------------------------------------------------------
# Violations stored in DB
# ---------------------------------------------------------------------------

class TestViolationStorage:
    def test_violations_persisted_by_engine(self, db):
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        from alma_atlas.enforcement.engine import EnforcementEngine
        from alma_atlas_store.violation_repository import ViolationRepository

        engine = EnforcementEngine(db)
        report = DriftReport(violations=[
            DriftViolation("a::t", "type_changed", "error", {"message": "x"}),
            DriftViolation("a::t", "removed_column", "warning", {"message": "y"}),
        ])
        engine.enforce(report, "shadow")

        repo = ViolationRepository(db)
        stored = repo.list_for_asset("a::t")
        assert len(stored) == 2
        types = {v.violation_type for v in stored}
        assert types == {"type_changed", "removed_column"}

    def test_violations_stored_regardless_of_mode(self, db):
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        from alma_atlas.enforcement.engine import EnforcementEngine
        from alma_atlas_store.violation_repository import ViolationRepository

        engine = EnforcementEngine(db)
        v = DriftViolation("b::t", "added_column", "info", {"message": "added"})
        report = DriftReport(violations=[v])

        for mode in ("shadow", "warn", "enforce"):
            engine.enforce(report, mode)  # type: ignore[arg-type]

        repo = ViolationRepository(db)
        stored = repo.list_for_asset("b::t")
        assert len(stored) == 3  # one per mode call

    def test_list_recent_returns_unresolved(self, db):
        from alma_atlas.enforcement.drift import DriftReport, DriftViolation
        from alma_atlas.enforcement.engine import EnforcementEngine
        from alma_atlas_store.violation_repository import ViolationRepository

        engine = EnforcementEngine(db)
        report = DriftReport(violations=[
            DriftViolation("c::t", "type_changed", "error", {"message": "t"}),
        ])
        engine.enforce(report, "enforce")

        repo = ViolationRepository(db)
        recent = repo.list_recent(limit=10)
        assert any(v.asset_id == "c::t" for v in recent)
        assert all(v.resolved_at is None for v in recent)
