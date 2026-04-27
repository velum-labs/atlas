"""Tests for invite-token storage, generation, and validate-on-call against the Alma endpoint."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from alma_atlas.auth.invite_token import (
    TokenValidation,
    generate_token,
    retrieve_invite_token,
    store_invite_token,
    validate_token,
)
from alma_atlas.local_secrets import LocalSecretStore

# ---------------------------------------------------------------------------
# generate_token
# ---------------------------------------------------------------------------


def test_generate_token_produces_long_url_safe_string() -> None:
    token = generate_token()
    assert len(token) >= 32  # token_urlsafe(32) -> ~43 chars after base64
    # URL-safe base64 alphabet
    allowed = set(
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    )
    assert all(c in allowed for c in token)


def test_generate_token_unique_each_call() -> None:
    tokens = {generate_token() for _ in range(50)}
    assert len(tokens) == 50  # vanishingly small chance of collision


# ---------------------------------------------------------------------------
# store / retrieve roundtrip via LocalSecretStore
# ---------------------------------------------------------------------------


def test_store_and_retrieve_roundtrip(tmp_path: Path) -> None:
    store = LocalSecretStore(tmp_path)
    token = generate_token()
    store_invite_token(store, token)
    retrieved = retrieve_invite_token(store)
    assert retrieved == token


def test_retrieve_returns_none_when_no_token_stored(tmp_path: Path) -> None:
    store = LocalSecretStore(tmp_path)
    assert retrieve_invite_token(store) is None


def test_store_rejects_short_token(tmp_path: Path) -> None:
    store = LocalSecretStore(tmp_path)
    with pytest.raises(ValueError, match="at least 16"):
        store_invite_token(store, "tooshort")


def test_store_rejects_empty_token(tmp_path: Path) -> None:
    store = LocalSecretStore(tmp_path)
    with pytest.raises(ValueError):
        store_invite_token(store, "")


# ---------------------------------------------------------------------------
# validate_token (HTTP semantics)
# ---------------------------------------------------------------------------


def _mock_response(status_code: int, json_body: dict | None = None, raise_on_json: bool = False):
    response = MagicMock()
    response.status_code = status_code
    if raise_on_json:
        response.json.side_effect = ValueError("not json")
    else:
        response.json.return_value = json_body or {}
    return response


def test_validate_token_returns_valid_on_200_with_valid_true() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.return_value = _mock_response(200, {"valid": True})
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is True
    assert result.display_message == ""


def test_validate_token_revoked_on_200_with_valid_false() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.return_value = _mock_response(200, {"valid": False})
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is False
    assert result.reason == "revoked"
    assert "revoked" in result.display_message
    assert "Velum admin" in result.display_message


def test_validate_token_unknown_on_404() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.return_value = _mock_response(404)
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is False
    assert result.reason == "unknown"


def test_validate_token_fails_closed_on_network_error() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.side_effect = httpx.ConnectError("connection refused")
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is False
    assert result.reason == "unreachable"
    assert "try again in a minute" in result.display_message


def test_validate_token_fails_closed_on_timeout() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.side_effect = httpx.TimeoutException("timed out")
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is False
    assert result.reason == "unreachable"


def test_validate_token_fails_closed_on_5xx() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.return_value = _mock_response(503)
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is False
    assert result.reason == "http_503"
    # 5xx shows the same friendly "try again" message as unreachable
    assert "try again in a minute" in result.display_message


def test_validate_token_fails_closed_on_malformed_json() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.return_value = _mock_response(200, raise_on_json=True)
        mock_client_cls.return_value = mock_client

        result = validate_token("a" * 32, "https://alma.example.com")

    assert result.valid is False
    assert result.reason == "malformed_response"


def test_validate_token_rejects_empty_token_without_network_call() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        result = validate_token("", "https://alma.example.com")
    assert result.valid is False
    assert result.reason == "unknown"
    mock_client_cls.assert_not_called()


def test_validate_token_strips_trailing_slash_from_endpoint() -> None:
    with patch("alma_atlas.auth.invite_token.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client.__enter__.return_value = mock_client
        mock_client.post.return_value = _mock_response(200, {"valid": True})
        mock_client_cls.return_value = mock_client

        validate_token("a" * 32, "https://alma.example.com/")

    args, _kwargs = mock_client.post.call_args
    url = args[0]
    assert url == "https://alma.example.com/api/v1/companion/validate-token"


# ---------------------------------------------------------------------------
# TokenValidation display messages (exhaustive, since these are user-facing strings)
# ---------------------------------------------------------------------------


def test_token_validation_display_messages() -> None:
    assert TokenValidation(valid=True).display_message == ""
    assert "revoked" in TokenValidation(valid=False, reason="revoked").display_message
    assert "try again" in TokenValidation(valid=False, reason="unreachable").display_message
    assert "invalid" in TokenValidation(valid=False, reason="unknown").display_message
    assert "try again" in TokenValidation(valid=False, reason="http_500").display_message
    # Unrecognized reason still produces a sensible default
    assert TokenValidation(valid=False, reason="weird_state").display_message
