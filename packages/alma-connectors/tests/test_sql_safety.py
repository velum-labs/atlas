"""Tests for SQL identifier quoting utilities in alma_ports.sql_safety."""

from __future__ import annotations

import pytest

from alma_ports.sql_safety import quote_bq_identifier, quote_sf_identifier

# ---------------------------------------------------------------------------
# quote_bq_identifier
# ---------------------------------------------------------------------------


class TestQuoteBqIdentifier:
    def test_normal_name(self):
        assert quote_bq_identifier("my_project") == "`my_project`"

    def test_name_with_hyphens(self):
        assert quote_bq_identifier("my-project-123") == "`my-project-123`"

    def test_region_composite(self):
        assert quote_bq_identifier("region-us-central1") == "`region-us-central1`"

    def test_escapes_internal_backtick(self):
        assert quote_bq_identifier("tab`le") == "`tab\\`le`"

    def test_escapes_multiple_backticks(self):
        assert quote_bq_identifier("`evil`") == "`\\`evil\\``"

    def test_unicode_name(self):
        assert quote_bq_identifier("テーブル") == "`テーブル`"

    def test_emoji_name(self):
        assert quote_bq_identifier("my💥table") == "`my💥table`"

    def test_semicolon_injection(self):
        # Semicolon is safe inside backtick-quoted identifier
        result = quote_bq_identifier("'; DROP TABLE users --")
        assert result == "`'; DROP TABLE users --`"

    def test_newline_in_name(self):
        result = quote_bq_identifier("evil\nname")
        assert result == "`evil\nname`"

    def test_null_byte(self):
        result = quote_bq_identifier("evil\x00name")
        assert result == "`evil\x00name`"

    def test_double_quote_in_name(self):
        result = quote_bq_identifier('"evil"')
        assert result == '`"evil"`'

    def test_comment_marker(self):
        result = quote_bq_identifier("name--comment")
        assert result == "`name--comment`"

    def test_block_comment(self):
        result = quote_bq_identifier("name/*comment*/")
        assert result == "`name/*comment*/`"

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            quote_bq_identifier("")


# ---------------------------------------------------------------------------
# quote_sf_identifier
# ---------------------------------------------------------------------------


class TestQuoteSfIdentifier:
    def test_normal_name(self):
        assert quote_sf_identifier("my_database") == '"my_database"'

    def test_name_with_spaces(self):
        assert quote_sf_identifier("my database") == '"my database"'

    def test_escapes_double_quote(self):
        assert quote_sf_identifier('"evil"') == '"""evil"""'

    def test_escapes_single_double_quote(self):
        assert quote_sf_identifier('say "hello"') == '"say ""hello"""'

    def test_semicolon_injection(self):
        result = quote_sf_identifier("'; DROP TABLE users --")
        assert result == """\"'; DROP TABLE users --\""""

    def test_newline_in_name(self):
        result = quote_sf_identifier("evil\nname")
        assert result == '"evil\nname"'

    def test_null_byte(self):
        result = quote_sf_identifier("evil\x00name")
        assert result == '"evil\x00name"'

    def test_unicode_name(self):
        assert quote_sf_identifier("テーブル") == '"テーブル"'

    def test_emoji_name(self):
        assert quote_sf_identifier("my💥table") == '"my💥table"'

    def test_comment_marker(self):
        result = quote_sf_identifier("name--comment")
        assert result == '"name--comment"'

    def test_block_comment(self):
        result = quote_sf_identifier("name/*comment*/")
        assert result == '"name/*comment*/"'

    def test_backtick_in_name(self):
        result = quote_sf_identifier("name`here")
        assert result == '"name`here"'

    def test_empty_raises(self):
        with pytest.raises(ValueError, match="must not be empty"):
            quote_sf_identifier("")
