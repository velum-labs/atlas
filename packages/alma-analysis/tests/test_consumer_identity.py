"""Tests for the unified consumer identification pipeline."""

from __future__ import annotations

import pytest

from alma_analysis.consumer_identity import (
    ConsumerIdentity,
    _normalize_consumer_name,
    identify_bq_consumer,
    identify_pg_consumer,
)

# ---------------------------------------------------------------------------
# _normalize_consumer_name
# ---------------------------------------------------------------------------


class TestNormalizeConsumerName:
    def test_strips_version_suffix(self) -> None:
        assert _normalize_consumer_name("backoffice-v2") == "backoffice"

    def test_strips_version_with_minor(self) -> None:
        assert _normalize_consumer_name("backoffice-v1.3") == "backoffice"

    def test_strips_numeric_version(self) -> None:
        assert _normalize_consumer_name("api-2.5") == "api"

    def test_strips_colon_env_production(self) -> None:
        assert _normalize_consumer_name("backoffice:production") == "backoffice"

    def test_strips_colon_env_staging(self) -> None:
        assert _normalize_consumer_name("api-service:staging") == "api-service"

    def test_strips_dash_env_prod(self) -> None:
        assert _normalize_consumer_name("backoffice-prod") == "backoffice"

    def test_strips_dash_env_dev(self) -> None:
        assert _normalize_consumer_name("backoffice-dev") == "backoffice"

    def test_strips_chained_version_and_env(self) -> None:
        # e.g. backoffice-v2:production → backoffice-v2 → backoffice
        assert _normalize_consumer_name("backoffice-v2:production") == "backoffice"

    def test_plain_name_unchanged(self) -> None:
        assert _normalize_consumer_name("backoffice") == "backoffice"

    def test_lowercases(self) -> None:
        assert _normalize_consumer_name("BackOffice") == "backoffice"

    def test_strips_whitespace(self) -> None:
        assert _normalize_consumer_name("  backoffice  ") == "backoffice"

    def test_case_insensitive_env(self) -> None:
        assert _normalize_consumer_name("backoffice-PROD") == "backoffice"

    def test_does_not_strip_mid_word(self) -> None:
        # "production" only stripped as a suffix, not inside the name.
        assert _normalize_consumer_name("production-service") == "production-service"

    def test_empty_string_preserved(self) -> None:
        # Degenerate: empty after normalization falls back to stripped original.
        assert _normalize_consumer_name("  ") == ""


# ---------------------------------------------------------------------------
# identify_pg_consumer
# ---------------------------------------------------------------------------


class TestIdentifyPgConsumer:
    def test_app_is_primary(self) -> None:
        identity = identify_pg_consumer(app="backoffice", user="svc", client="10.0.0.1")
        assert identity.name == "backoffice"
        assert identity.type == "application"
        assert identity.source_label == "backoffice"
        assert identity.confidence == 0.9

    def test_app_is_normalised(self) -> None:
        identity = identify_pg_consumer(app="backoffice-v2", user="svc", client="10.0.0.1")
        assert identity.name == "backoffice"
        assert identity.source_label == "backoffice-v2"

    def test_app_with_env_suffix_normalised(self) -> None:
        identity = identify_pg_consumer(app="backoffice:production", user=None, client=None)
        assert identity.name == "backoffice"

    def test_user_fallback_when_app_empty(self) -> None:
        identity = identify_pg_consumer(app="", user="analyst", client="10.0.0.1")
        assert identity.name == "user:analyst"
        assert identity.type == "user"
        assert identity.confidence == 0.7

    def test_user_fallback_when_app_none(self) -> None:
        identity = identify_pg_consumer(app=None, user="svc_role", client=None)
        assert identity.name == "user:svc_role"

    def test_client_fallback_when_app_and_user_empty(self) -> None:
        identity = identify_pg_consumer(app="", user="", client="10.0.0.1")
        assert identity.name == "client:10.0.0.1"
        assert identity.type == "client"
        assert identity.confidence == 0.3

    def test_unknown_when_all_empty(self) -> None:
        identity = identify_pg_consumer(app=None, user=None, client=None)
        assert identity.name == "unknown"
        assert identity.type == "unknown"
        assert identity.confidence == 0.0

    def test_app_whitespace_treated_as_empty(self) -> None:
        identity = identify_pg_consumer(app="   ", user="svc", client=None)
        assert identity.name == "user:svc"

    def test_database_user_preserved_in_metadata(self) -> None:
        identity = identify_pg_consumer(app="backoffice", user="svc", client=None)
        assert identity.metadata.get("database_user") == "svc"

    def test_database_user_none_in_metadata_when_empty(self) -> None:
        identity = identify_pg_consumer(app="backoffice", user="", client=None)
        assert identity.metadata.get("database_user") is None

    def test_returns_consumer_identity_type(self) -> None:
        assert isinstance(identify_pg_consumer(app="svc", user=None, client=None), ConsumerIdentity)


# ---------------------------------------------------------------------------
# identify_bq_consumer
# ---------------------------------------------------------------------------


class TestIdentifyBqConsumer:
    def test_dag_and_task_airflow(self) -> None:
        labels = {"dag_id": "load_users", "task_id": "copy_users"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="job-1")
        assert identity.name == "airflow:load_users:copy_users"
        assert identity.type == "airflow"
        assert identity.confidence == 0.95

    def test_dag_only_airflow(self) -> None:
        labels = {"dag_id": "load_users"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="job-1")
        assert identity.name == "airflow:load_users"
        assert identity.type == "airflow"

    def test_airflow_legacy_label_keys(self) -> None:
        labels = {"airflow_dag": "ingest_orders", "airflow_task": "transform"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="job-2")
        assert identity.name == "airflow:ingest_orders:transform"

    def test_service_label_application(self) -> None:
        labels = {"service": "reporting-api"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="job-3")
        assert identity.name == "reporting-api"
        assert identity.type == "application"
        assert identity.confidence == 0.8

    def test_service_label_normalised(self) -> None:
        labels = {"service": "reporting-api-v2"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="job-4")
        assert identity.name == "reporting-api"
        assert identity.source_label == "reporting-api-v2"

    def test_app_label(self) -> None:
        labels = {"app": "backoffice"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="job-5")
        assert identity.name == "backoffice"

    def test_user_email_fallback(self) -> None:
        identity = identify_bq_consumer(labels={}, user_email="analyst@example.com", job_id="job-6")
        assert identity.name == "user:analyst@example.com"
        assert identity.type == "user"
        assert identity.confidence == 0.7

    def test_job_id_fallback(self) -> None:
        identity = identify_bq_consumer(labels={}, user_email=None, job_id="bqjob_xyz")
        assert identity.name == "job:bqjob_xyz"
        assert identity.type == "unknown"
        assert identity.confidence == 0.4

    def test_dag_metadata_stored(self) -> None:
        labels = {"dag_id": "etl", "task_id": "load"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="j")
        assert identity.metadata["dag_id"] == "etl"
        assert identity.metadata["task_id"] == "load"

    def test_airflow_priority_over_service_label(self) -> None:
        labels = {"dag_id": "etl", "service": "reporting"}
        identity = identify_bq_consumer(labels=labels, user_email=None, job_id="j")
        assert identity.type == "airflow"

    def test_returns_consumer_identity_type(self) -> None:
        assert isinstance(
            identify_bq_consumer(labels={}, user_email=None, job_id="j"), ConsumerIdentity
        )


# ---------------------------------------------------------------------------
# Integration: deduplication via normalised consumer keys
# ---------------------------------------------------------------------------


class TestConsumerDeduplication:
    """Verify that variant names collapse to the same canonical key."""

    @pytest.mark.parametrize(
        "variant",
        [
            "backoffice",
            "backoffice-v2",
            "backoffice-v3",
            "backoffice:production",
            "backoffice-prod",
            "backoffice:staging",
        ],
    )
    def test_pg_variants_map_to_same_canonical_key(self, variant: str) -> None:
        identity = identify_pg_consumer(app=variant, user=None, client=None)
        assert identity.name == "backoffice", f"{variant!r} → {identity.name!r}"
