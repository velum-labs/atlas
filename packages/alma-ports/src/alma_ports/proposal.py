"""Proposal and rewrite rule storage protocols."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from alma_ports.connection import ConnectionT

type EvidenceBundleJSON = dict[str, Any]
type RewriteRulesJSON = list[dict[str, Any]]


@runtime_checkable
class ProposalReader(Protocol):
    """Read-only access to proposals and rewrite rules."""

    def get_proposal(self, id: str | UUID) -> dict[str, Any] | None: ...

    def get_proposals(
        self,
        *,
        status: str | None = None,
        proposal_type: str | None = None,
        contract_id: str | UUID | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]: ...

    def get_proposal_comments(self, proposal_id: str | UUID) -> list[dict[str, Any]]: ...

    def get_rewrite_rules(
        self, contract_id: str | UUID | None = None, *, enabled_only: bool = False
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class ProposalWriter(Protocol):
    """Write access to proposals and rewrite rules."""

    def insert_proposal(
        self,
        *,
        id: UUID | None = None,
        proposal_type: str,
        contract_id: str | UUID,
        title: str,
        description: str | None = None,
        status: str = "open",
        migration_sql: str | None = None,
        view_sql: str | None = None,
        rewrite_rules: RewriteRulesJSON | None = None,
        evidence_bundle: EvidenceBundleJSON | None = None,
        created_by: str | None = None,
    ) -> UUID: ...

    def update_proposal(self, id: str | UUID, updates: dict[str, Any]) -> dict[str, Any] | None: ...

    def delete_proposal(self, id: str | UUID) -> bool: ...

    def insert_proposal_comment(
        self, proposal_id: str | UUID, author: str, content: str
    ) -> UUID: ...

    def insert_rewrite_rule(
        self,
        *,
        contract_id: str | UUID,
        query_pattern: str,
        rewritten_sql: str,
        pattern_type: str = "signature_hash",
        enabled: bool = False,
        priority: int = 0,
    ) -> UUID: ...

    def update_rewrite_rule(
        self, id: str | UUID, updates: dict[str, Any]
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class ProposalTransactor(Protocol):
    """Transactional proposal operations for merge workflows."""

    def get_connection(self) -> Any: ...

    def get_proposal_for_update(
        self, id: str | UUID, conn: ConnectionT
    ) -> dict[str, Any] | None: ...

    def update_proposal_in_tx(
        self, id: str | UUID, updates: dict[str, Any], conn: ConnectionT
    ) -> None: ...

    def execute_sql_in_tx(
        self, sql: str, conn: ConnectionT, *, allow_unsafe: bool = False
    ) -> None: ...

    def merge_proposal_in_transaction(
        self,
        proposal_id: str,
        updates: dict[str, Any],
        migration_sql: str | None,
        *,
        allow_unsafe: bool = False,
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class ProposalRepository(ProposalReader, ProposalWriter, ProposalTransactor, Protocol):
    """Full proposal storage for modules that need reads, writes, and transactions."""

    ...
