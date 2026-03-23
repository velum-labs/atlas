"""Tests for ContractRepository."""

from __future__ import annotations

import pytest

from alma_atlas_store.contract_repository import Contract


@pytest.fixture(autouse=True)
def seed_asset(asset_repo, sample_asset):
    asset_repo.upsert(sample_asset)


def test_upsert_creates_contract(contract_repo, sample_contract):
    contract_repo.upsert(sample_contract)
    result = contract_repo.get(sample_contract.id)
    assert result is not None
    assert result.id == sample_contract.id
    assert result.version == sample_contract.version
    assert result.status == sample_contract.status


def test_upsert_updates_existing_contract(contract_repo, sample_contract):
    contract_repo.upsert(sample_contract)
    updated = Contract(
        id=sample_contract.id,
        asset_id=sample_contract.asset_id,
        version="2.0.0",
        spec={"columns": []},
        status="deprecated",
    )
    contract_repo.upsert(updated)
    result = contract_repo.get(sample_contract.id)
    assert result.version == "2.0.0"
    assert result.status == "deprecated"


def test_get_returns_none_for_missing(contract_repo):
    assert contract_repo.get("nonexistent") is None


def test_spec_json_roundtrip(contract_repo, sample_contract):
    contract_repo.upsert(sample_contract)
    result = contract_repo.get(sample_contract.id)
    assert result.spec == sample_contract.spec


def test_list_for_asset_returns_contracts(contract_repo, sample_contract, sample_asset):
    contract_repo.upsert(sample_contract)
    results = contract_repo.list_for_asset(sample_asset.id)
    assert any(c.id == sample_contract.id for c in results)


def test_list_for_asset_returns_empty_for_other(contract_repo, sample_contract, sample_asset_b, asset_repo):
    asset_repo.upsert(sample_asset_b)
    contract_repo.upsert(sample_contract)
    results = contract_repo.list_for_asset(sample_asset_b.id)
    assert results == []


def test_list_all_returns_all(contract_repo, sample_asset):
    c1 = Contract(id="c1", asset_id=sample_asset.id, version="1.0", spec={}, status="draft")
    c2 = Contract(id="c2", asset_id=sample_asset.id, version="2.0", spec={}, status="active")
    contract_repo.upsert(c1)
    contract_repo.upsert(c2)
    all_c = contract_repo.list_all()
    ids = {c.id for c in all_c}
    assert {"c1", "c2"}.issubset(ids)


def test_list_all_empty(contract_repo):
    assert contract_repo.list_all() == []


def test_timestamps_set(contract_repo, sample_contract):
    contract_repo.upsert(sample_contract)
    result = contract_repo.get(sample_contract.id)
    assert result.created_at is not None
    assert result.updated_at is not None


@pytest.mark.parametrize("status", ["draft", "active", "deprecated"])
def test_contract_statuses(contract_repo, sample_asset, status):
    c = Contract(id=f"c_{status}", asset_id=sample_asset.id, version="1.0", spec={}, status=status)
    contract_repo.upsert(c)
    result = contract_repo.get(f"c_{status}")
    assert result.status == status


def test_multiple_contracts_for_same_asset(contract_repo, sample_asset):
    c1 = Contract(id="v1", asset_id=sample_asset.id, version="1.0", spec={"v": 1}, status="deprecated")
    c2 = Contract(id="v2", asset_id=sample_asset.id, version="2.0", spec={"v": 2}, status="active")
    contract_repo.upsert(c1)
    contract_repo.upsert(c2)
    results = contract_repo.list_for_asset(sample_asset.id)
    assert len(results) == 2
