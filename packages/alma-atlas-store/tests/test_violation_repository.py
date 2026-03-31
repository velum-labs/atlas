"""Tests for ViolationRepository."""

from __future__ import annotations

from alma_atlas_store.violation_repository import ViolationRepository
from alma_ports.violation import Violation


def test_reinserting_resolved_violation_reopens_it(db):
    repo = ViolationRepository(db)
    violation = Violation(
        id="violation-1",
        asset_id="asset-1",
        violation_type="type_mismatch",
        severity="error",
        details={"column": "id", "message": "bad type"},
    )

    repo.insert(violation)
    repo.resolve("violation-1")
    repo.insert(violation)

    stored = repo.list_for_asset("asset-1")
    assert len(stored) == 1
    assert stored[0].resolved_at is None
