"""Regression test: every Atlas MCP tool name registered before the tools.py split is still registered.

After splitting `mcp/tools.py` into category modules (tools_search, tools_schema,
tools_lineage, tools_contracts, tools_meta, tools_agent), the public registration
surface must remain identical. This test asserts the full canonical tool list
is exposed by `_tool_specs()` and `_tool_handlers()` aggregations.

If a handler is moved to a category module but not re-exported by `tools.py`,
or if a category module forgets to include a tool in its `specs()` / `handlers()`
helpers, this test fails fast.
"""

from __future__ import annotations

from alma_atlas.mcp.tools import _tool_handlers, _tool_specs

# Canonical list of tool names that must be registered. This is the contract
# external clients (Cursor, Claude Desktop, MCP-curious devs) depend on.
EXPECTED_TOOL_NAMES = frozenset({
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


def test_all_expected_tools_have_specs():
    """Every expected tool name must appear in the aggregated spec catalog."""
    spec_names = {spec.name for spec in _tool_specs()}
    missing = EXPECTED_TOOL_NAMES - spec_names
    extra = spec_names - EXPECTED_TOOL_NAMES
    assert not missing, f"Tools missing from _tool_specs(): {sorted(missing)}"
    assert not extra, (
        f"Tools added to _tool_specs() but not in EXPECTED_TOOL_NAMES: {sorted(extra)}. "
        "If this is intentional, update EXPECTED_TOOL_NAMES."
    )


def test_all_expected_tools_have_handlers():
    """Every expected tool name must have a callable in the aggregated handler dispatch."""
    handlers = _tool_handlers()
    missing = EXPECTED_TOOL_NAMES - handlers.keys()
    extra = handlers.keys() - EXPECTED_TOOL_NAMES
    assert not missing, f"Tools missing from _tool_handlers(): {sorted(missing)}"
    assert not extra, (
        f"Tools added to _tool_handlers() but not in EXPECTED_TOOL_NAMES: {sorted(extra)}. "
        "If this is intentional, update EXPECTED_TOOL_NAMES."
    )


def test_specs_and_handlers_cover_the_same_tools():
    """The spec catalog and the handler dispatch must agree on the tool list."""
    spec_names = {spec.name for spec in _tool_specs()}
    handler_names = set(_tool_handlers().keys())
    assert spec_names == handler_names, (
        f"Spec/handler mismatch. "
        f"In specs but not handlers: {sorted(spec_names - handler_names)}. "
        f"In handlers but not specs: {sorted(handler_names - spec_names)}."
    )


def test_no_duplicate_tool_names_across_categories():
    """No two category modules may register the same tool name (would be a silent override)."""
    from alma_atlas.mcp import (
        tools_agent,
        tools_contracts,
        tools_lineage,
        tools_meta,
        tools_schema,
        tools_search,
    )

    seen: dict[str, str] = {}
    for module in (tools_search, tools_schema, tools_lineage, tools_contracts, tools_meta, tools_agent):
        for spec in module.specs():
            assert spec.name not in seen, (
                f"Tool {spec.name!r} registered in both {seen[spec.name]} and {module.__name__}"
            )
            seen[spec.name] = module.__name__
