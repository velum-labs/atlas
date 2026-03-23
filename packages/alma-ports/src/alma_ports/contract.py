"""Contract storage protocols."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

type ContractSchemaJSON = dict[str, Any]


@runtime_checkable
class ContractReader(Protocol):
    """Read-only access to contracts and shadow configuration."""

    def get_contracts(
        self, *, status: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[dict[str, Any]]: ...

    def get_contract(self, id: str | UUID) -> dict[str, Any] | None: ...

    def get_shadow_config(self, contract_id: str | UUID) -> dict[str, Any] | None: ...

    def get_shadow_summary(self, contract_id: str | UUID) -> dict[str, Any] | None: ...

    def get_shadow_results(
        self, contract_id: str | UUID, *, limit: int = 1000, since: datetime | None = None
    ) -> list[dict[str, Any]]: ...


@runtime_checkable
class ContractWriter(Protocol):
    """Write access to contracts and shadow configuration."""

    def insert_contract(
        self,
        *,
        id: UUID | None = None,
        name: str,
        version: int,
        schema_json: ContractSchemaJSON,
        view_sql: str,
        support_score: float | None = None,
        coverage_weight: float | None = None,
        status: str = "proposed",
    ) -> UUID: ...

    def update_contract_fields(
        self, id: str | UUID, updates: dict[str, Any]
    ) -> dict[str, Any] | None: ...

    def delete_contract(self, id: str | UUID) -> bool: ...

    def insert_shadow_config(
        self,
        *,
        contract_id: str | UUID,
        enabled: bool = False,
        sample_rate: float = 0.1,
        min_tests_required: int = 100,
        min_pass_rate: float = 0.99,
        max_performance_regression: float = 1.5,
    ) -> dict[str, Any] | None: ...

    def update_shadow_config(
        self,
        contract_id: str | UUID,
        *,
        enabled: bool | None = None,
        sample_rate: float | None = None,
        min_tests_required: int | None = None,
        min_pass_rate: float | None = None,
        max_performance_regression: float | None = None,
    ) -> dict[str, Any] | None: ...


@runtime_checkable
class ContractRepository(ContractReader, ContractWriter, Protocol):
    """Full contract storage for modules that need both reads and writes."""

    ...
