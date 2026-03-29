"""Tests for atlas_get_annotations MCP handler."""

from __future__ import annotations

import json
from pathlib import Path

from alma_atlas.config import AtlasConfig
from alma_atlas.mcp.tools import _handle_get_annotations
from alma_atlas_store.annotation_repository import AnnotationRecord, AnnotationRepository
from alma_atlas_store.asset_repository import Asset, AssetRepository
from alma_atlas_store.db import Database


def _make_cfg(tmp_path: Path) -> AtlasConfig:
    db_path = tmp_path / "atlas.db"
    cfg = AtlasConfig(config_dir=tmp_path / "alma", db_path=db_path)
    with Database(db_path):
        pass
    return cfg


def test_get_annotations_empty(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_get_annotations(cfg, {"limit": 10})
    payload = json.loads(result[0].text)
    assert payload["annotations"] == []


def test_get_annotations_for_asset(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    with Database(cfg.db_path) as db:
        # Need an asset present for FK join in list_unannotated (not used here, but good hygiene)
        AssetRepository(db).upsert(Asset(id="pg::public.orders", source="pg:test", kind="table", name="public.orders"))
        AnnotationRepository(db).upsert(
            AnnotationRecord(
                asset_id="pg::public.orders",
                ownership="data",
                granularity="one row per order",
                join_keys=["order_id"],
                freshness_guarantee="hourly",
                business_logic_summary="orders table",
                sensitivity="financial",
                annotated_by="agent:mock",
            )
        )

    result = _handle_get_annotations(cfg, {"asset_id": "pg::public.orders"})
    payload = json.loads(result[0].text)
    assert payload["asset_id"] == "pg::public.orders"
    assert payload["ownership"] == "data"
    assert payload["join_keys"] == ["order_id"]


def test_get_annotations_missing_asset(tmp_path: Path) -> None:
    cfg = _make_cfg(tmp_path)
    result = _handle_get_annotations(cfg, {"asset_id": "missing"})
    assert "no annotation" in result[0].text.lower()
