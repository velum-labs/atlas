"""Regression: tool catalogs for the default surface and the Companion surface are stable.

Two registration profiles:

- Default `register(server, cfg)` exposes the 20 atlas_* tools.
- `register(server, cfg, modules=COMPANION_CATEGORY_MODULES)` exposes the 3
  companion_* tools.

This test pins both lists. If a category module forgets to include a tool in
its `specs()` / `handlers()` helpers, OR if a new tool is added without being
added here, the test fails fast.
"""

from __future__ import annotations

from alma_atlas.mcp.tools import (
    ATLAS_CATEGORY_MODULES,
    COMPANION_CATEGORY_MODULES,
    _tool_handlers,
    _tool_specs,
)

# Default registration: the full 20-tool atlas_* surface.
EXPECTED_ATLAS_TOOL_NAMES = frozenset({
    "atlas_search",
    "atlas_get_asset",
    "atlas_get_annotations",
    "atlas_lineage",
    "atlas_status",
    "atlas_get_schema",
    "atlas_impact",
    "atlas_get_query_patterns",
    "atlas_suggest_tables",
    "atlas_check_contract",
    "atlas_list_violations",
    "atlas_team_sync",
    "atlas_explain_column",
    "atlas_profile_column",
    "atlas_describe_relationship",
    "atlas_find_term",
    "atlas_verify",
    "atlas_define_term",
    "atlas_context",
    "atlas_ask",
})

# Companion mode: the curated 3-tool surface.
EXPECTED_COMPANION_TOOL_NAMES = frozenset({
    "companion_search_assets",
    "companion_get_schema_and_owner",
    "companion_explain_lineage_and_contract",
})


# ---------------------------------------------------------------------------
# Default (atlas_* tools only)
# ---------------------------------------------------------------------------


def test_default_specs_match_expected_atlas_surface():
    spec_names = {spec.name for spec in _tool_specs()}
    missing = EXPECTED_ATLAS_TOOL_NAMES - spec_names
    extra = spec_names - EXPECTED_ATLAS_TOOL_NAMES
    assert not missing, f"Tools missing from default _tool_specs(): {sorted(missing)}"
    assert not extra, (
        f"Tools added to default _tool_specs() but not expected: {sorted(extra)}. "
        "If intentional, update EXPECTED_ATLAS_TOOL_NAMES."
    )


def test_default_handlers_match_expected_atlas_surface():
    handlers = _tool_handlers()
    missing = EXPECTED_ATLAS_TOOL_NAMES - handlers.keys()
    extra = handlers.keys() - EXPECTED_ATLAS_TOOL_NAMES
    assert not missing, f"Tools missing from default _tool_handlers(): {sorted(missing)}"
    assert not extra, (
        f"Tools added to default _tool_handlers() but not expected: {sorted(extra)}."
    )


def test_default_specs_and_handlers_agree():
    spec_names = {spec.name for spec in _tool_specs()}
    handler_names = set(_tool_handlers().keys())
    assert spec_names == handler_names


def test_default_does_not_include_companion_tools():
    """Companion tools must not leak into the default surface."""
    spec_names = {spec.name for spec in _tool_specs()}
    overlap = spec_names & EXPECTED_COMPANION_TOOL_NAMES
    assert overlap == set(), (
        f"Companion tools leaked into default surface: {sorted(overlap)}. "
        "Companion tools must only be registered when modules=COMPANION_CATEGORY_MODULES."
    )


# ---------------------------------------------------------------------------
# Companion mode (companion_* tools only)
# ---------------------------------------------------------------------------


def test_companion_specs_match_expected_companion_surface():
    spec_names = {spec.name for spec in _tool_specs(modules=COMPANION_CATEGORY_MODULES)}
    assert spec_names == EXPECTED_COMPANION_TOOL_NAMES


def test_companion_handlers_match_expected_companion_surface():
    handler_names = set(_tool_handlers(modules=COMPANION_CATEGORY_MODULES).keys())
    assert handler_names == EXPECTED_COMPANION_TOOL_NAMES


def test_companion_does_not_include_atlas_tools():
    """Atlas tools must not appear when only the Companion modules are registered."""
    spec_names = {spec.name for spec in _tool_specs(modules=COMPANION_CATEGORY_MODULES)}
    overlap = spec_names & EXPECTED_ATLAS_TOOL_NAMES
    assert overlap == set(), (
        f"Atlas tools leaked into Companion surface: {sorted(overlap)}."
    )


# ---------------------------------------------------------------------------
# Cross-surface guarantees
# ---------------------------------------------------------------------------


def test_no_overlap_between_atlas_and_companion_tool_names():
    overlap = EXPECTED_ATLAS_TOOL_NAMES & EXPECTED_COMPANION_TOOL_NAMES
    assert overlap == set(), (
        f"Atlas and Companion surfaces share tool names: {sorted(overlap)}. "
        "Tool names across surfaces must be globally unique."
    )


def test_no_duplicate_tool_names_across_all_categories():
    """No two category modules may register the same tool name (would be a silent override)."""
    seen: dict[str, str] = {}
    for module in ATLAS_CATEGORY_MODULES + COMPANION_CATEGORY_MODULES:
        for spec in module.specs():
            assert spec.name not in seen, (
                f"Tool {spec.name!r} registered in both {seen[spec.name]} and {module.__name__}"
            )
            seen[spec.name] = module.__name__
