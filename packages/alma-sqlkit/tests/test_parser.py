from __future__ import annotations

from alma_sqlkit import SQLParser


def test_parser_supports_in_subquery_predicate() -> None:
    parser = SQLParser()

    ra = parser.parse(
        """
        SELECT id
        FROM orders
        WHERE customer_id IN (SELECT customer_id FROM orders)
        """
    )

    assert "IN" in ra.fingerprint()


def test_parser_supports_exists_predicate() -> None:
    parser = SQLParser()

    ra = parser.parse(
        """
        SELECT id
        FROM orders
        WHERE EXISTS (SELECT 1 FROM orders o2 WHERE o2.id = orders.id)
        """
    )

    assert "EXISTS" in ra.fingerprint()


def test_parser_preserves_nested_order_and_limit() -> None:
    parser = SQLParser()

    ra = parser.parse(
        """
        SELECT sq.id
        FROM (
            SELECT id
            FROM orders
            ORDER BY id
            LIMIT 5
        ) AS sq
        ORDER BY sq.id
        LIMIT 1
        """
    )

    fingerprint = ra.fingerprint()
    assert "LIMIT 5" in fingerprint
    assert "LIMIT 1" in fingerprint
