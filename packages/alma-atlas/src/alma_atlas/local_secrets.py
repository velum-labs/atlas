"""Local encrypted secret persistence for Atlas config state."""

from __future__ import annotations

import contextlib
import json
import os
from pathlib import Path
from typing import Any, cast

from cryptography.fernet import Fernet

from alma_connectors.credentials import decrypt_credential, encrypt_credential

_SECRET_MARKER_KEY = "__alma_secret_id__"


class LocalSecretStore:
    """Persist local config secrets encrypted at rest under the config dir."""

    def __init__(self, config_dir: Path) -> None:
        self._config_dir = config_dir
        self._key_file = config_dir / "secrets.key"
        self._secrets_file = config_dir / "secrets.json"

    @staticmethod
    def is_marker(value: object) -> bool:
        if not isinstance(value, dict):
            return False
        marker = cast(dict[str, Any], value).get(_SECRET_MARKER_KEY)
        return isinstance(marker, str)

    def marker(self, secret_id: str) -> dict[str, str]:
        return {_SECRET_MARKER_KEY: secret_id}

    def resolve(self, value: object) -> object:
        if not self.is_marker(value):
            return value
        secret_id = cast(dict[str, str], value)[_SECRET_MARKER_KEY]
        payload = self._load_payload()
        encoded = payload.get(secret_id)
        if not isinstance(encoded, str):
            raise ValueError(f"missing stored secret payload for {secret_id!r}")
        return decrypt_credential(encoded.encode("utf-8"), key=self._load_key())

    def store(self, secret_id: str, plaintext: str) -> dict[str, str]:
        if not plaintext:
            raise ValueError("local secret plaintext must be non-empty")
        payload = self._load_payload()
        payload[secret_id] = encrypt_credential(plaintext, key=self._load_or_create_key()).decode("utf-8")
        self._save_payload(payload)
        return self.marker(secret_id)

    def _load_or_create_key(self) -> str:
        if self._key_file.exists():
            return self._key_file.read_text(encoding="utf-8").strip()
        self._config_dir.mkdir(parents=True, exist_ok=True)
        key = Fernet.generate_key().decode("utf-8")
        self._key_file.write_text(key, encoding="utf-8")
        with contextlib.suppress(OSError):
            os.chmod(self._key_file, 0o600)
        return key

    def _load_key(self) -> str:
        if not self._key_file.exists():
            raise ValueError("no local Atlas secret key found")
        return self._key_file.read_text(encoding="utf-8").strip()

    def _load_payload(self) -> dict[str, str]:
        if not self._secrets_file.exists():
            return {}
        raw = json.loads(self._secrets_file.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise ValueError("local Atlas secrets store must be a JSON object")
        return {str(key): str(value) for key, value in raw.items()}

    def _save_payload(self, payload: dict[str, str]) -> None:
        self._config_dir.mkdir(parents=True, exist_ok=True)
        self._secrets_file.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        with contextlib.suppress(OSError):
            os.chmod(self._secrets_file, 0o600)
