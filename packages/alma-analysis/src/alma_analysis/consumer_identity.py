"""Unified consumer identification pipeline for alma-analysis adapters.

Provides a single ``ConsumerIdentity`` type and adapter-specific identification
functions so that BigQuery and PostgreSQL adapters produce identical output shapes.

Consumer name normalisation
---------------------------
The same logical consumer can appear under slightly different labels across
runs (e.g. ``backoffice``, ``backoffice-v2``, ``backoffice:production``).
``_normalize_consumer_name`` strips common version and environment suffixes so
that all variants collapse to the same canonical key before storage.  The raw
original label is preserved in ``source_label`` for diagnostics.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any

# Version suffixes: -v2, -v3, -v1.0, -2.5 …
_RE_VERSION = re.compile(r"-v?\d+(?:\.\d+)*$", re.IGNORECASE)

# Environment suffixes joined after a colon or dash: :production, -prod, etc.
_ENV_TOKENS = {
    "production",
    "prod",
    "staging",
    "stage",
    "dev",
    "development",
    "local",
    "test",
    "testing",
    "sandbox",
}
_RE_ENV_COLON = re.compile(
    r":(" + "|".join(_ENV_TOKENS) + r")$",
    re.IGNORECASE,
)
_RE_ENV_DASH = re.compile(
    r"-(" + "|".join(_ENV_TOKENS) + r")$",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ConsumerIdentity:
    """Normalised consumer identity produced by an adapter.

    Attributes:
        name: Canonical consumer key used for storage and deduplication.
            Version/environment suffixes are stripped so that variants such as
            ``backoffice``, ``backoffice-v2``, and ``backoffice:production``
            all map to the same key.
        type: Consumer classification — ``"airflow"``, ``"user"``,
            ``"application"``, ``"client"``, or ``"unknown"``.
        source_label: The original raw label before normalisation.  Preserved
            for diagnostics and metadata.
        confidence: Identification confidence in [0.0, 1.0].
        metadata: Adapter-specific extras (dag_id, task_id, client_addr, …).
    """

    name: str
    type: str
    source_label: str
    confidence: float
    metadata: dict[str, Any] = field(default_factory=dict)


def _normalize_consumer_name(raw: str) -> str:
    """Strip version and environment suffixes from a consumer label.

    Applies repeatedly until stable so that ``backoffice-v2:production``
    collapses to ``backoffice`` in two passes.
    """
    normalized = raw.strip().lower()
    while True:
        candidate = _RE_ENV_COLON.sub("", normalized)
        candidate = _RE_ENV_DASH.sub("", candidate)
        candidate = _RE_VERSION.sub("", candidate)
        candidate = candidate.strip()
        if candidate == normalized:
            break
        normalized = candidate
    return normalized or raw.strip().lower()


def identify_pg_consumer(
    *,
    app: str | None,
    user: str | None,
    client: str | None,
) -> ConsumerIdentity:
    """Identify a PostgreSQL consumer from log-prefix fields.

    Priority order:
    1. ``application_name`` (``app``) — highest confidence; set explicitly by
       the connecting library and uniquely identifies the service.
    2. ``user`` — database role; useful when ``app`` is blank.
    3. ``client`` — source IP; last resort for service identification when only
       network context is available.

    Args:
        app: The ``application_name`` from the PG log prefix (``%a``).
        user: The database user from the PG log prefix (``%u``).
        client: The client host/IP from the PG log prefix (``%h``).

    Returns:
        A ``ConsumerIdentity`` with the canonical name and confidence score.
    """
    app_clean = (app or "").strip()
    user_clean = (user or "").strip()
    client_clean = (client or "").strip()

    if app_clean:
        canonical = _normalize_consumer_name(app_clean)
        return ConsumerIdentity(
            name=canonical,
            type="application",
            source_label=app_clean,
            confidence=0.9,
            metadata={"database_user": user_clean or None},
        )

    if user_clean:
        return ConsumerIdentity(
            name=f"user:{user_clean}",
            type="user",
            source_label=user_clean,
            confidence=0.7,
            metadata={},
        )

    if client_clean:
        return ConsumerIdentity(
            name=f"client:{client_clean}",
            type="client",
            source_label=client_clean,
            confidence=0.3,
            metadata={},
        )

    return ConsumerIdentity(
        name="unknown",
        type="unknown",
        source_label="",
        confidence=0.0,
        metadata={},
    )


def identify_bq_consumer(
    *,
    labels: dict[str, str],
    user_email: str | None,
    job_id: str,
) -> ConsumerIdentity:
    """Identify a BigQuery consumer from job metadata.

    Priority order:
    1. Labels — Airflow DAG/task labels have highest fidelity; other label
       keys (e.g. ``service``, ``app``) also give strong service identity.
    2. ``user_email`` — the authenticated Google identity running the job.
    3. ``job_id`` prefix — last resort; preserves traceability but carries low
       confidence because job IDs are per-execution, not per-service.

    Args:
        labels: Normalised BigQuery job labels (key → value).
        user_email: The email of the user who submitted the job.
        job_id: The BigQuery job ID (used as fallback ``name``).

    Returns:
        A ``ConsumerIdentity`` with the canonical name and confidence score.
    """
    dag_id = labels.get("dag_id") or labels.get("airflow_dag")
    task_id = labels.get("task_id") or labels.get("airflow_task")

    if dag_id:
        if task_id:
            raw_key = f"airflow:{dag_id}:{task_id}"
        else:
            raw_key = f"airflow:{dag_id}"
        return ConsumerIdentity(
            name=raw_key,
            type="airflow",
            source_label=raw_key,
            confidence=0.95,
            metadata={"dag_id": dag_id, "task_id": task_id},
        )

    # Try non-Airflow service labels.
    service_label = labels.get("service") or labels.get("app") or labels.get("application")
    if service_label:
        canonical = _normalize_consumer_name(service_label)
        return ConsumerIdentity(
            name=canonical,
            type="application",
            source_label=service_label,
            confidence=0.8,
            metadata={"labels": dict(labels)},
        )

    if user_email:
        email_clean = user_email.strip()
        return ConsumerIdentity(
            name=f"user:{email_clean}",
            type="user",
            source_label=email_clean,
            confidence=0.7,
            metadata={},
        )

    return ConsumerIdentity(
        name=f"job:{job_id}",
        type="unknown",
        source_label=job_id,
        confidence=0.4,
        metadata={},
    )
