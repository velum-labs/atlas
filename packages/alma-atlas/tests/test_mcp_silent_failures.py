"""Tests for the silent sqlglot parse failure in _handle_verify.

_handle_verify had ``except Exception: pass`` around ``sqlglot.parse_one``.
On failure agg_columns stayed empty, surrogate-key checks never ran, and the
caller received ``{"valid": true, "warnings": []}``.

Patching notes: ``parse_one`` is patched at ``sqlglot.parse_one`` (global)
because _handle_verify uses a lazy function-body import.
``extract_tables_from_sql`` is patched at its definition site
(``alma_sqlkit.table_refs``) so execution reaches the parse_one block.
If either import is hoisted to module level the patch targets must change.

sqlglot>=25 raises ``SqlglotError`` instead of returning ``None`` for empty
input (e.g. ``";"``), so the else-branch always receives an ``exp.Expression``.
"""

from __future__ import annotations

import json
import logging
import types
from pathlib import Path
from unittest.mock import patch

import sqlglot.errors

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp import tools as _tools_mod
from alma_atlas.mcp.tools import _handle_verify
from alma_atlas_store.annotation_repository import AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database
from alma_ports.annotation import AnnotationRecord

# Substring contracts for the warning message — update if the string changes.
_WARNING_FRAGMENT_1 = "aggregate-column detection failed"
_WARNING_FRAGMENT_2 = "sqlglot parse error"
_WARNING_FRAGMENT_3 = "surrogate-key annotation checks did not run"

# Derived from the module so a rename is caught at import time.
_LOGGER_NAME = _tools_mod.__name__

# See module docstring for why the definition-site target is correct here.
_PATCH_EXTRACT = "alma_sqlkit.table_refs.extract_tables_from_sql"


def _seed_asset(db_path: Path, table: str = "public.orders") -> None:
    with Database(db_path) as db:
        AssetRepository(db).upsert(Asset(id=f"pg::{table}", source="pg:test", kind="table", name=table))


def _seed_surrogate_key(
    db_path: Path,
    table: str = "public.orders",
    column: str = "order_id",
) -> None:
    with Database(db_path) as db:
        AssetRepository(db).upsert(Asset(id=f"pg::{table}", source="pg:test", kind="table", name=table))
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id=f"pg::{table}",
                properties={"column_notes": {column: "surrogate key — do not aggregate"}},
            )
        )


class TestHandleVerifySqlglotParseFailure:
    """Covers the failure branch of the sqlglot.parse_one try/except/else block."""

    def test_parse_failure_logs_warning(self, cfg_with_db: AtlasConfig, caplog):
        """A SqlglotError from parse_one must produce a WARNING log entry."""
        with (
            caplog.at_level(logging.WARNING, logger=_LOGGER_NAME),
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("parse boom")),
        ):
            _handle_verify(cfg_with_db, {"sql": "SELECT SUM(x) FROM t"})

        assert any(r.levelno == logging.WARNING for r in caplog.records)

    def test_parse_failure_warning_contains_exception_string(self, cfg_with_db: AtlasConfig, caplog):
        """The WARNING must interpolate the exception message (sentinel verifies %s binding)."""
        sentinel = "SENTINEL_EXC_9f3a"

        with (
            caplog.at_level(logging.WARNING, logger=_LOGGER_NAME),
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError(sentinel)),
        ):
            _handle_verify(cfg_with_db, {"sql": "SELECT SUM(x) FROM t"})

        assert any(sentinel in r.getMessage() for r in caplog.records)

    def test_parse_failure_surfaces_as_warning_in_result(self, cfg_with_db: AtlasConfig):
        """Parse failure must appear in warnings, making valid:false."""
        with (
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("boom")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT SUM(x) FROM t"})

        data = json.loads(result_texts[0].text)
        assert data["valid"] is False
        assert any(_WARNING_FRAGMENT_1 in w for w in data["warnings"])

    def test_parse_failure_warning_contains_cause_fragment(self, cfg_with_db: AtlasConfig):
        """The warning string must name the cause: sqlglot parse error."""
        with (
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("bad sql")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT 1"})

        data = json.loads(result_texts[0].text)
        assert any(_WARNING_FRAGMENT_2 in w for w in data["warnings"])

    def test_parse_failure_warning_names_skipped_check(self, cfg_with_db: AtlasConfig):
        """The warning must state which check did not run."""
        with (
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("any")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT SUM(x) FROM t"})

        data = json.loads(result_texts[0].text)
        assert any(_WARNING_FRAGMENT_3 in w for w in data["warnings"])

    def test_parse_failure_warning_appears_exactly_once(self, cfg_with_db: AtlasConfig):
        """One parse failure must produce exactly one warning entry."""
        with (
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("boom")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT SUM(a) FROM t"})

        data = json.loads(result_texts[0].text)
        count = sum(1 for w in data["warnings"] if _WARNING_FRAGMENT_1 in w)
        assert count == 1, f"Expected exactly one warning entry, found {count}: {data['warnings']}"

    def test_parse_failure_result_is_valid_json(self, cfg_with_db: AtlasConfig):
        """Result must be valid JSON with valid/warnings/suggestions keys."""
        with (
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("any")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT 1"})

        assert len(result_texts) == 1
        data = json.loads(result_texts[0].text)
        assert "valid" in data
        assert "warnings" in data
        assert "suggestions" in data

    def test_token_error_subclass_also_caught(self, cfg_with_db: AtlasConfig):
        """SqlglotError base class covers TokenError — not just ParseError."""
        with (
            patch(_PATCH_EXTRACT, return_value=[]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.TokenError("bad token")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT @bad FROM t"})

        data = json.loads(result_texts[0].text)
        assert data["valid"] is False
        assert any(_WARNING_FRAGMENT_1 in w for w in data["warnings"])

    def test_semicolon_only_input_raises_and_warning_emitted(self, cfg_with_db: AtlasConfig):
        """In sqlglot>=25, parse_one raises ParseError for ';' — must be caught and surfaced."""
        # This test does NOT patch parse_one; it exercises the real sqlglot behaviour.
        with patch(_PATCH_EXTRACT, return_value=[]):
            result_texts = _handle_verify(cfg_with_db, {"sql": ";"})

        data = json.loads(result_texts[0].text)
        # parse_one raises SqlglotError for ";", which is caught and emits a warning.
        assert data["valid"] is False
        assert any(_WARNING_FRAGMENT_1 in w for w in data["warnings"])


class TestHandleVerifyDegradedModeScenario:
    """Demonstrates the concrete failure mode: parse error → valid:false, not silent valid:true.

    test_degraded_mode_makes_result_invalid uses real DB state for the seeded
    surrogate key, but the annotation loop is never reached when parse_one
    raises — the test asserts on the detection-failure warning, not annotation
    behaviour.  test_degraded_mode_vs_working_parse uses DB state for both
    arms: the working-parse arm exercises the full surrogate-key annotation
    path; the failing-parse arm shows the distinct detection-failure warning.

    Both tests fail when the pre-fix ``except Exception: pass`` is restored:
    the old code appended nothing to warnings and returned valid:true.
    """

    def test_degraded_mode_makes_result_invalid(self, cfg_with_db: AtlasConfig):
        """On parse failure, valid must be false and the warning must be present."""
        _seed_surrogate_key(cfg_with_db.db_path, table="public.orders", column="order_id")
        fake_ref = types.SimpleNamespace(canonical_name="public.orders")

        with (
            patch(_PATCH_EXTRACT, return_value=[fake_ref]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("parse boom")),
        ):
            result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT SUM(order_id) FROM public.orders"})

        data = json.loads(result_texts[0].text)
        assert data["valid"] is False
        assert any(_WARNING_FRAGMENT_1 in w for w in data["warnings"])

    def test_degraded_mode_vs_working_parse(self, cfg_with_db: AtlasConfig):
        """Working parse surfaces surrogate-key warning; failing parse surfaces detection-failure warning."""
        _seed_surrogate_key(cfg_with_db.db_path, table="public.orders", column="order_id")
        fake_ref = types.SimpleNamespace(canonical_name="public.orders")

        sql = "SELECT SUM(order_id) FROM public.orders"

        with patch(_PATCH_EXTRACT, return_value=[fake_ref]):
            normal_texts = _handle_verify(cfg_with_db, {"sql": sql})
        normal = json.loads(normal_texts[0].text)

        with (
            patch(_PATCH_EXTRACT, return_value=[fake_ref]),
            patch("sqlglot.parse_one", side_effect=sqlglot.errors.ParseError("boom")),
        ):
            degraded_texts = _handle_verify(cfg_with_db, {"sql": sql})
        degraded = json.loads(degraded_texts[0].text)

        # Working parse: surrogate-key warning, no detection-failure warning
        assert normal["valid"] is False
        assert any("order_id" in w for w in normal["warnings"])
        assert not any(_WARNING_FRAGMENT_1 in w for w in normal["warnings"])

        # Failing parse: detection-failure warning, no surrogate-key warning
        assert degraded["valid"] is False
        assert any(_WARNING_FRAGMENT_1 in w for w in degraded["warnings"])
        assert not any("order_id" in w for w in degraded["warnings"])


class TestHandleVerifySqlglotHappyPath:
    """Guards against regressions on the normal (no parse failure) path."""

    def test_no_detection_failure_warning_when_parse_succeeds(self, cfg_with_db: AtlasConfig):
        """Normal parse path must not produce the detection-failure warning."""
        _seed_surrogate_key(cfg_with_db.db_path)

        result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT SUM(amount) FROM public.orders"})
        data = json.loads(result_texts[0].text)

        assert not any(_WARNING_FRAGMENT_1 in w for w in data["warnings"])

    def test_surrogate_key_warning_still_fires(self, cfg_with_db: AtlasConfig):
        """Surrogate-key check must still work when sqlglot succeeds."""
        _seed_surrogate_key(cfg_with_db.db_path, column="order_id")

        result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT SUM(order_id) FROM public.orders"})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is False
        assert any("order_id" in w for w in data["warnings"])

    def test_clean_query_returns_valid_true_no_warnings(self, cfg_with_db: AtlasConfig):
        """A query with no issues returns valid:true, empty warnings, empty suggestions."""
        _seed_asset(cfg_with_db.db_path)

        result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT * FROM public.orders"})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is True
        assert data["warnings"] == []
        assert data["suggestions"] == []

    def test_avg_on_non_surrogate_column_no_warning(self, cfg_with_db: AtlasConfig):
        """AVG on a non-surrogate column produces no warning."""
        _seed_surrogate_key(cfg_with_db.db_path, column="order_id")

        result_texts = _handle_verify(cfg_with_db, {"sql": "SELECT AVG(amount) FROM public.orders"})
        data = json.loads(result_texts[0].text)

        assert data["valid"] is True
        assert data["warnings"] == []
