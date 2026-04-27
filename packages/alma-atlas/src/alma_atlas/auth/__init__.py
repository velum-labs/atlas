"""Authentication primitives for Atlas Companion mode.

Companion mode (started via `alma-atlas serve --alma-token <invite>`) gates
MCP tool calls behind an invite-token validated against an Alma deployment
endpoint. The invite_token submodule owns token storage (encrypted at rest
via LocalSecretStore) and per-call validation.
"""
