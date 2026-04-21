"""Tests for SQLiteAdapter.extract_profiles column profiling."""

from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

import pytest

from alma_connectors import PersistedSourceAdapter, SourceAdapterKind, SourceAdapterStatus, SQLiteAdapterConfig
from alma_connectors.adapters.sqlite import SQLiteAdapter
from alma_ports.profiling import ColumnProfile


def _make_persisted(db_path: Path) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id="00000000-0000-0000-0000-000000000222",
        key="sqlite-profile-test",
        display_name="SQLite Profile Test",
        kind=SourceAdapterKind.SQLITE,
        target_id="sqlite:profile-test",
        status=SourceAdapterStatus.READY,
        config=SQLiteAdapterConfig(path=str(db_path)),
    )


def _create_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "profile_test.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE products (
            id      INTEGER PRIMARY KEY,
            name    TEXT NOT NULL,
            price   REAL,
            status  TEXT,
            created_date TEXT
        )
        """
    )
    conn.executemany(
        "INSERT INTO products (name, price, status, created_date) VALUES (?, ?, ?, ?)",
        [
            ("Widget", 9.99, "active", "2024-01-01"),
            ("Gadget", 19.99, "active", "2024-01-02"),
            ("Doohickey", 4.99, "inactive", "2024-01-03"),
            ("Thingamajig", None, "active", "2024-01-04"),
            ("Whatsit", 9.99, None, "2024-01-05"),
        ],
    )
    conn.commit()
    conn.close()
    return db_path


def test_extract_profiles_returns_column_profiles(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))

    assert len(profiles) > 0
    assert all(isinstance(p, ColumnProfile) for p in profiles)


def test_extract_profiles_asset_id_format(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))

    assert all(p.asset_id == "sqlite-profile-test/products" for p in profiles)


def test_extract_profiles_covers_all_columns(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    col_names = {p.column_name for p in profiles}

    assert col_names == {"id", "name", "price", "status", "created_date"}


def test_extract_profiles_distinct_count(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    assert by_col["status"].distinct_count == 2  # "active", "inactive" (NULL not counted)
    assert by_col["id"].distinct_count == 5


def test_extract_profiles_null_count(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    # price has 1 NULL (Thingamajig), status has 1 NULL (Whatsit)
    assert by_col["price"].null_count == 1
    assert by_col["status"].null_count == 1
    assert by_col["name"].null_count == 0


def test_extract_profiles_null_fraction(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    # 1 null out of 5 rows = 0.2
    assert abs(by_col["price"].null_fraction - 0.2) < 1e-9


def test_extract_profiles_min_max(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    assert by_col["price"].min_value == "4.99"
    assert by_col["price"].max_value == "19.99"


def test_extract_profiles_top_values_low_cardinality(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    # status has 2 distinct values (low cardinality), should have top_values
    assert len(by_col["status"].top_values) > 0
    top_vals = {tv["value"] for tv in by_col["status"].top_values}
    assert "active" in top_vals


def test_extract_profiles_no_top_values_high_cardinality(tmp_path: Path) -> None:
    """Columns with distinct_count > 200 should have empty top_values."""
    db_path = tmp_path / "hc.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, val TEXT)")
    conn.executemany(
        "INSERT INTO t (val) VALUES (?)",
        [(f"value_{i}",) for i in range(300)],
    )
    conn.commit()
    conn.close()

    adapter = SQLiteAdapter(db_path=db_path)
    persisted = PersistedSourceAdapter(
        id="00000000-0000-0000-0000-000000000333",
        key="sqlite-hc-test",
        display_name="High Cardinality Test",
        kind=SourceAdapterKind.SQLITE,
        target_id="sqlite:hc-test",
        status=SourceAdapterStatus.READY,
        config=SQLiteAdapterConfig(path=str(db_path)),
    )

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    assert by_col["val"].top_values == []


def test_extract_profiles_sample_values_for_date_columns(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))
    by_col = {p.column_name: p for p in profiles}

    # created_date has "date" in type name -> should have sample_values
    assert len(by_col["created_date"].sample_values) > 0
    assert all(isinstance(v, str) for v in by_col["created_date"].sample_values)


def test_extract_profiles_profiled_at_is_set(tmp_path: Path) -> None:
    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    profiles = asyncio.run(adapter.extract_profiles(persisted))

    assert all(p.profiled_at is not None for p in profiles)


def test_extract_profiles_skips_large_tables(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Tables with row_count exceeding the limit should be skipped entirely."""
    import alma_connectors.adapters.sqlite as sqlite_mod

    db_path = _create_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    # Lower the limit below the 5 rows in the test DB so all tables are skipped
    monkeypatch.setattr(sqlite_mod, "_PROFILE_ROW_LIMIT", 4)

    profiles = asyncio.run(adapter.extract_profiles(persisted))

    assert profiles == []
