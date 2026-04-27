"""Regression test: every Atlas MCP tool dispatches to the correct handler implementation.

After splitting `mcp/tools.py` into category modules, the dispatch table built by
`_tool_handlers()` must point at the same handler functions exposed by each
category module. If a category module accidentally exports the wrong handler
(e.g., a typo, or two tools sharing a function), this test catches it.
"""

from __future__ import annotations

import inspect

from alma_atlas.mcp import tools
from alma_atlas.mcp.tools_agent import _handle_ask, _handle_context
from alma_atlas.mcp.tools_contracts import (
    _dispatch_verify,
    _handle_check_contract,
    _handle_define_term,
    _handle_list_violations,
    _handle_verify,
    _handle_verify_deep,
)
from alma_atlas.mcp.tools_lineage import (
    _handle_describe_relationship,
    _handle_impact,
    _handle_lineage,
)
from alma_atlas.mcp.tools_meta import (
    _handle_get_query_patterns,
    _handle_status,
    _handle_team_sync,
)
from alma_atlas.mcp.tools_schema import (
    _handle_explain_column,
    _handle_get_annotations,
    _handle_get_asset,
    _handle_get_schema,
    _handle_profile_column,
)
from alma_atlas.mcp.tools_search import (
    _handle_find_term,
    _handle_search,
    _handle_suggest_tables,
)


def test_dispatch_routes_each_tool_to_its_implementation():
    """The dispatch dict must route each tool name to the correct underlying function."""
    handlers = tools._tool_handlers()

    expected_routes = {
        "atlas_search": _handle_search,
        "atlas_suggest_tables": _handle_suggest_tables,
        "atlas_find_term": _handle_find_term,
        "atlas_get_asset": _handle_get_asset,
        "atlas_get_annotations": _handle_get_annotations,
        "atlas_get_schema": _handle_get_schema,
        "atlas_explain_column": _handle_explain_column,
        "atlas_profile_column": _handle_profile_column,
        "atlas_lineage": _handle_lineage,
        "atlas_impact": _handle_impact,
        "atlas_describe_relationship": _handle_describe_relationship,
        "atlas_check_contract": _handle_check_contract,
        "atlas_list_violations": _handle_list_violations,
        "atlas_verify": _dispatch_verify,
        "atlas_define_term": _handle_define_term,
        "atlas_get_query_patterns": _handle_get_query_patterns,
        "atlas_context": _handle_context,
        "atlas_ask": _handle_ask,
    }
    # atlas_status and atlas_team_sync are wrapped in lambdas (their handlers
    # take only `cfg`, not `(cfg, arguments)`) so we verify by name instead.
    lambda_wrapped = {"atlas_status", "atlas_team_sync"}

    for tool_name, expected_fn in expected_routes.items():
        actual = handlers.get(tool_name)
        assert actual is expected_fn, (
            f"Dispatch for {tool_name!r} routes to {actual!r}, expected {expected_fn!r}. "
            "A category module likely exported the wrong handler."
        )

    for tool_name in lambda_wrapped:
        actual = handlers.get(tool_name)
        assert actual is not None, f"Dispatch for {tool_name!r} is missing"
        assert callable(actual), f"Dispatch for {tool_name!r} is not callable"


def test_handler_signatures_unchanged():
    """Handlers must keep their (cfg, arguments) or (cfg) signatures so the dispatcher works."""
    standard_handlers = [
        _handle_search,
        _handle_get_asset,
        _handle_get_annotations,
        _handle_lineage,
        _handle_get_schema,
        _handle_impact,
        _handle_get_query_patterns,
        _handle_suggest_tables,
        _handle_check_contract,
        _handle_list_violations,
        _handle_explain_column,
        _handle_profile_column,
        _handle_describe_relationship,
        _handle_find_term,
        _handle_verify,
        _handle_define_term,
        _handle_context,
        _handle_ask,
        _handle_verify_deep,
        _dispatch_verify,
    ]
    for handler in standard_handlers:
        sig = inspect.signature(handler)
        params = list(sig.parameters.keys())
        assert len(params) == 2, (
            f"{handler.__name__} should take 2 args (cfg, arguments), got {params}"
        )

    # cfg-only handlers (wrapped in lambdas at the dispatch table)
    cfg_only_handlers = [_handle_status, _handle_team_sync]
    for handler in cfg_only_handlers:
        sig = inspect.signature(handler)
        params = list(sig.parameters.keys())
        assert len(params) == 1, (
            f"{handler.__name__} should take 1 arg (cfg), got {params}"
        )
