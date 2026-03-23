"""Fernet-based credential encryption for source adapter secrets."""

from __future__ import annotations

from cryptography.fernet import Fernet


def encrypt_credential(plaintext: str, *, key: str) -> bytes:
    """Encrypt a plaintext credential string using Fernet symmetric encryption."""
    if not key:
        raise ValueError("encryption_key is required for credential encryption")
    if not plaintext:
        raise ValueError("plaintext credential must be non-empty")
    fernet = Fernet(key.encode("utf-8"))
    return fernet.encrypt(plaintext.encode("utf-8"))


def decrypt_credential(ciphertext: bytes, *, key: str) -> str:
    """Decrypt a Fernet-encrypted credential."""
    if not key:
        raise ValueError("encryption_key is required for credential decryption")
    if not ciphertext:
        raise ValueError("ciphertext must be non-empty")
    fernet = Fernet(key.encode("utf-8"))
    return fernet.decrypt(ciphertext).decode("utf-8")
