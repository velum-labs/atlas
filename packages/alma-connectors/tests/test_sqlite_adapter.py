from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path

from alma_connectors import PersistedSourceAdapter, SQLiteAdapterConfig, SourceAdapterKind, SourceAdapterStatus
from alma_connectors.adapters.sqlite import SQLiteAdapter
from alma_connectors.source_adapter_v2 import AdapterCapability, ExtractionScope
from alma_connectors.source_adapter_v2 import SchemaObjectKind as V2Kind


def _create_sqlite_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "sample.sqlite"
    connection = sqlite3.connect(str(db_path))
    connection.execute("PRAGMA foreign_keys = ON")
    connection.execute(
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            nickname,
            age INTEGER
        )
        """
    )
    connection.execute(
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            user_id INTEGER REFERENCES users(id),
            amount REAL
        )
        """
    )
    connection.executemany(
        "INSERT INTO users (name, nickname, age) VALUES (?, ?, ?)",
        [("Alice", "ally", 30), ("Bob", None, 25)],
    )
    connection.executemany(
        "INSERT INTO orders (id, user_id, amount) VALUES (?, ?, ?)",
        [(1, 1, 99.9), (2, 2, 49.5)],
    )
    connection.execute("CREATE VIEW user_names AS SELECT id, name FROM users")
    connection.commit()
    connection.close()
    return db_path


def _make_persisted(db_path: Path) -> PersistedSourceAdapter:
    return PersistedSourceAdapter(
        id="00000000-0000-0000-0000-000000000111",
        key="sqlite-sample",
        display_name="SQLite Sample",
        kind=SourceAdapterKind.SQLITE,
        target_id="sqlite:sample",
        status=SourceAdapterStatus.READY,
        config=SQLiteAdapterConfig(path=str(db_path)),
    )


def test_probe_valid_db(tmp_path: Path) -> None:
    db_path = _create_sqlite_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    results = asyncio.run(adapter.probe(persisted))

    by_capability = {result.capability: result for result in results}
    assert by_capability[AdapterCapability.DISCOVER].available is True
    assert by_capability[AdapterCapability.SCHEMA].available is True
    assert by_capability[AdapterCapability.DISCOVER].scope == ExtractionScope.DATABASE
    assert by_capability[AdapterCapability.DISCOVER].scope_context is not None
    assert by_capability[AdapterCapability.DISCOVER].scope_context.identifiers["path"] == str(db_path.resolve())


def test_probe_missing_file(tmp_path: Path) -> None:
    db_path = tmp_path / "missing.sqlite"
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    results = asyncio.run(adapter.probe(persisted))

    assert all(result.available is False for result in results)
    assert results[0].message == f"File not found: {db_path.resolve()}"


def test_probe_invalid_file(tmp_path: Path) -> None:
    db_path = tmp_path / "invalid.sqlite"
    db_path.write_text("not a sqlite database", encoding="utf-8")
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    results = asyncio.run(adapter.probe(persisted))

    assert all(result.available is False for result in results)
    assert results[0].message is not None
    assert results[0].message.startswith("Not a valid SQLite database:")


def test_discover_returns_tables_and_views(tmp_path: Path) -> None:
    db_path = _create_sqlite_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    snapshot = asyncio.run(adapter.discover(persisted))

    discovered = {container.display_name: container.container_type for container in snapshot.containers}
    assert discovered == {
        "orders": "table",
        "user_names": "view",
        "users": "table",
    }
    assert "sqlite_sequence" not in discovered


def test_extract_schema_returns_expected_objects(tmp_path: Path) -> None:
    db_path = _create_sqlite_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    snapshot = asyncio.run(adapter.extract_schema(persisted))

    objects_by_name = {obj.object_name: obj for obj in snapshot.objects}
    assert set(objects_by_name) == {"orders", "user_names", "users"}
    assert all(object_.schema_name == "_default" for object_ in snapshot.objects)
    assert objects_by_name["users"].kind == V2Kind.TABLE
    assert objects_by_name["user_names"].kind == V2Kind.VIEW
    assert objects_by_name["users"].row_count == 2
    assert objects_by_name["orders"].row_count == 2

    users_columns = {column.name: column for column in objects_by_name["users"].columns}
    assert users_columns["name"].data_type == "TEXT"
    assert users_columns["nickname"].data_type == "ANY"
    assert users_columns["name"].is_nullable is False


def test_extract_schema_emits_foreign_key_dependencies(tmp_path: Path) -> None:
    db_path = _create_sqlite_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    snapshot = asyncio.run(adapter.extract_schema(persisted))

    dependencies = {
        (
            dependency.source_schema,
            dependency.source_object,
            dependency.target_schema,
            dependency.target_object,
        )
        for dependency in snapshot.dependencies
    }
    assert dependencies == {("_default", "orders", "_default", "users")}


def test_extract_schema_excludes_system_tables(tmp_path: Path) -> None:
    db_path = _create_sqlite_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    snapshot = asyncio.run(adapter.extract_schema(persisted))

    assert all(not obj.object_name.startswith("sqlite_") for obj in snapshot.objects)


def test_execute_query_is_read_only(tmp_path: Path) -> None:
    db_path = _create_sqlite_db(tmp_path)
    adapter = SQLiteAdapter(db_path=db_path)
    persisted = _make_persisted(db_path)

    select_result = asyncio.run(
        adapter.execute_query(
            persisted,
            "SELECT id, name FROM users ORDER BY id",
        )
    )
    assert select_result.success is True
    assert select_result.row_count == 2
    assert tuple(row["name"] for row in select_result.rows) == ("Alice", "Bob")

    write_result = asyncio.run(
        adapter.execute_query(
            persisted,
            "INSERT INTO users (name, nickname, age) VALUES ('Cara', 'c', 22)",
        )
    )
    assert write_result.success is False
    assert write_result.error_message is not None
    assert "readonly" in write_result.error_message.lower() or "read-only" in write_result.error_message.lower()
