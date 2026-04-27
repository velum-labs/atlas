"""Tests for the PostHog telemetry wrapper.

Verifies the mandatory/opt-in bucket split, PII sanitization, env-var
configuration, kill-switch behavior, and that PostHog API errors never crash
the host process.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from alma_atlas.telemetry import (
    ALLOWED_MANDATORY_PROPS,
    TelemetryConfig,
    _sanitize_mandatory_properties,
    mandatory_event,
    opt_in_event,
    reset_client_cache,
    telemetry_config_from_env,
)


@pytest.fixture(autouse=True)
def _clear_client_cache():
    """Each test gets a fresh client cache so patches take effect."""
    reset_client_cache()
    yield
    reset_client_cache()


# ---------------------------------------------------------------------------
# telemetry_config_from_env
# ---------------------------------------------------------------------------


def test_env_off_kill_switch_disables_telemetry(monkeypatch):
    monkeypatch.setenv("ATLAS_TELEMETRY_OFF", "1")
    cfg = telemetry_config_from_env()
    assert cfg.disabled is True


def test_env_off_truthy_string_disables(monkeypatch):
    monkeypatch.setenv("ATLAS_TELEMETRY_OFF", "yes")
    cfg = telemetry_config_from_env()
    assert cfg.disabled is True


def test_env_loads_api_key_and_host(monkeypatch):
    monkeypatch.delenv("ATLAS_TELEMETRY_OFF", raising=False)
    monkeypatch.setenv("ATLAS_POSTHOG_API_KEY", "phc_test_key")
    monkeypatch.setenv("ATLAS_POSTHOG_HOST", "https://eu.posthog.com")
    cfg = telemetry_config_from_env()
    assert cfg.disabled is False
    assert cfg.api_key == "phc_test_key"
    assert cfg.host == "https://eu.posthog.com"


def test_env_defaults_to_us_host_when_unset(monkeypatch):
    monkeypatch.delenv("ATLAS_TELEMETRY_OFF", raising=False)
    monkeypatch.delenv("ATLAS_POSTHOG_HOST", raising=False)
    cfg = telemetry_config_from_env()
    assert cfg.host == "https://us.i.posthog.com"


# ---------------------------------------------------------------------------
# install_id stability
# ---------------------------------------------------------------------------


def test_install_id_is_stable_across_configs():
    cfg_a = TelemetryConfig()
    cfg_b = TelemetryConfig()
    assert cfg_a.install_id == cfg_b.install_id


def test_install_id_is_a_uuid_string():
    cfg = TelemetryConfig()
    # UUID5 stringified is 36 chars with hyphens at canonical positions
    assert len(cfg.install_id) == 36
    assert cfg.install_id.count("-") == 4


# ---------------------------------------------------------------------------
# mandatory_event
# ---------------------------------------------------------------------------


def test_mandatory_event_sends_to_posthog_with_install_id():
    mock_client = MagicMock()
    cfg = TelemetryConfig(api_key="phc_test")
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        mandatory_event(cfg, "tool_call", {"tool_name": "companion_search_assets"})

    mock_client.capture.assert_called_once()
    _args, kwargs = mock_client.capture.call_args
    assert kwargs["distinct_id"] == cfg.install_id
    assert kwargs["event"] == "tool_call"
    assert kwargs["properties"] == {"tool_name": "companion_search_assets"}


def test_mandatory_event_drops_disallowed_props():
    mock_client = MagicMock()
    cfg = TelemetryConfig(api_key="phc_test")
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        mandatory_event(
            cfg,
            "tool_call",
            {
                "tool_name": "companion_search_assets",
                "user_email": "javier@fintual.com",  # disallowed - PII
                "warehouse_creds": "s3cret",  # disallowed - PII
            },
        )

    _args, kwargs = mock_client.capture.call_args
    assert "user_email" not in kwargs["properties"]
    assert "warehouse_creds" not in kwargs["properties"]
    assert kwargs["properties"]["tool_name"] == "companion_search_assets"


def test_mandatory_event_no_op_when_disabled():
    mock_client = MagicMock()
    cfg = TelemetryConfig(api_key="phc_test", disabled=True)
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        mandatory_event(cfg, "tool_call", {"tool_name": "x"})

    mock_client.capture.assert_not_called()


def test_mandatory_event_no_op_when_no_api_key():
    mock_client = MagicMock()
    cfg = TelemetryConfig(api_key=None)
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        mandatory_event(cfg, "tool_call", {"tool_name": "x"})

    mock_client.capture.assert_not_called()


def test_mandatory_event_swallows_posthog_exceptions():
    mock_client = MagicMock()
    mock_client.capture.side_effect = RuntimeError("PostHog broken")
    cfg = TelemetryConfig(api_key="phc_test")
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        # Must not raise
        mandatory_event(cfg, "tool_call", {"tool_name": "x"})


# ---------------------------------------------------------------------------
# opt_in_event
# ---------------------------------------------------------------------------


def test_opt_in_event_no_op_when_opt_in_false():
    mock_client = MagicMock()
    cfg = TelemetryConfig(api_key="phc_test", opt_in=False)
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        opt_in_event(cfg, "install_to_conversation", {"x": "y"})

    mock_client.capture.assert_not_called()


def test_opt_in_event_sends_with_account_token_when_opt_in_true():
    mock_client = MagicMock()
    cfg = TelemetryConfig(
        api_key="phc_test",
        opt_in=True,
        alma_account_token="acct_fintual_xyz",
    )
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        opt_in_event(cfg, "install", {"channel": "concierge_invite"})

    _args, kwargs = mock_client.capture.call_args
    assert kwargs["distinct_id"] == "acct_fintual_xyz"
    assert kwargs["properties"]["alma_account_token"] == "acct_fintual_xyz"
    assert kwargs["properties"]["channel"] == "concierge_invite"


def test_opt_in_event_falls_back_to_install_id_without_account_token():
    mock_client = MagicMock()
    cfg = TelemetryConfig(api_key="phc_test", opt_in=True)
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        opt_in_event(cfg, "install", {})

    _args, kwargs = mock_client.capture.call_args
    assert kwargs["distinct_id"] == cfg.install_id


def test_opt_in_event_swallows_posthog_exceptions():
    mock_client = MagicMock()
    mock_client.capture.side_effect = RuntimeError("PostHog broken")
    cfg = TelemetryConfig(api_key="phc_test", opt_in=True)
    with patch("alma_atlas.telemetry._posthog") as mock_posthog:
        mock_posthog.Posthog.return_value = mock_client
        opt_in_event(cfg, "install", {})  # must not raise


# ---------------------------------------------------------------------------
# Property allowlist
# ---------------------------------------------------------------------------


def test_allowed_mandatory_props_documents_full_schema():
    expected = {
        "tool_name",
        "mcp_session_duration_seconds",
        "connector_kind",
        "install_source",
        "atlas_version",
        "platform",
        "python_version",
    }
    assert set(ALLOWED_MANDATORY_PROPS) == expected


def test_sanitize_drops_unlisted_keys():
    out = _sanitize_mandatory_properties(
        {"tool_name": "x", "secret_thing": "y", "platform": "darwin"}
    )
    assert out == {"tool_name": "x", "platform": "darwin"}


def test_sanitize_passes_empty_input_through():
    assert _sanitize_mandatory_properties({}) == {}
