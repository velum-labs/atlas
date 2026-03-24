"""ConflictResolver — merge strategy for assets, edges, and contracts.

Policy:
    assets / edges:  last-write-wins (higher last_seen / updated_at beats older)
    contracts:       server-wins (team admin controls enforcement policy)
"""

from __future__ import annotations


class ConflictResolver:
    """Resolves conflicts between local and remote records during sync."""

    @staticmethod
    def resolve_asset(local: dict, remote: dict) -> dict:
        """Return the version with the later last_seen timestamp (last-write-wins)."""
        local_ts = local.get("last_seen") or ""
        remote_ts = remote.get("last_seen") or ""
        return remote if remote_ts >= local_ts else local

    @staticmethod
    def resolve_edge(local: dict, remote: dict) -> dict:
        """Return the version with the later last_seen timestamp (last-write-wins)."""
        local_ts = local.get("last_seen") or ""
        remote_ts = remote.get("last_seen") or ""
        return remote if remote_ts >= local_ts else local

    @staticmethod
    def resolve_contract(local: dict, remote: dict) -> dict:  # noqa: ARG004
        """Server always wins for contracts — team admin controls enforcement policy."""
        return remote
