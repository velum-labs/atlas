"""Instance setting storage protocols."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class InstanceSettingReader(Protocol):
    """Read-only access to instance settings."""

    def get_instance_setting(self, *, key: str) -> dict[str, object] | None: ...


@runtime_checkable
class InstanceSettingWriter(Protocol):
    """Write access to instance settings."""

    def upsert_instance_setting(
        self,
        *,
        key: str,
        value_encrypted: bytes,
    ) -> dict[str, object] | None: ...

    def delete_instance_setting(self, *, key: str) -> bool: ...


@runtime_checkable
class InstanceSettingRepository(InstanceSettingReader, InstanceSettingWriter, Protocol):
    """Full instance setting storage."""

    ...
