# alma-connectors

`alma-connectors` contains the canonical adapter model and all source-system integrations for Atlas.

## What lives here

- persisted adapter kinds and config dataclasses in `src/alma_connectors/source_adapter.py`
- capability-oriented extraction protocol in `src/alma_connectors/source_adapter_v2.py`
- runtime adapter construction in `src/alma_connectors/source_adapter_runtime.py`
- persisted-row serialization/deserialization in `src/alma_connectors/source_adapter_service.py`
- concrete adapters in `src/alma_connectors/adapters/`

## Supported connectors

- BigQuery
- PostgreSQL
- Snowflake
- dbt
- Airflow
- Looker
- Fivetran
- Metabase

## Architecture notes

- `SourceAdapterKind` is the single canonical adapter kind enum for both persisted and runtime models.
- Community connectors now participate in the same canonical persisted config model as the warehouse adapters.
- `source_adapter_runtime.py` is the single source of truth for turning a persisted adapter into a live runtime adapter instance.
