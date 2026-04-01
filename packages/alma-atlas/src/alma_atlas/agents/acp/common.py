"""Shared ACP runtime constants and utilities."""

from __future__ import annotations

from importlib import metadata

from acp.schema import Implementation, TerminalExitStatus

MAX_RETRIES = 3
DEFAULT_OUTPUT_BYTE_LIMIT = 1024 * 1024
_ATLAS_CLIENT_NAME = "alma-atlas"
_ATLAS_CLIENT_TITLE = "Alma Atlas"


def atlas_version() -> str:
    try:
        return metadata.version("alma-atlas")
    except metadata.PackageNotFoundError:
        return "0.1.0"


def default_client_info() -> Implementation:
    return Implementation(
        name=_ATLAS_CLIENT_NAME,
        title=_ATLAS_CLIENT_TITLE,
        version=atlas_version(),
    )


def truncate_output(text: str, output_byte_limit: int) -> tuple[str, bool]:
    encoded = text.encode("utf-8", errors="replace")
    if len(encoded) <= output_byte_limit:
        return text, False

    trimmed = encoded[-output_byte_limit:]
    while trimmed:
        try:
            return trimmed.decode("utf-8"), True
        except UnicodeDecodeError:
            trimmed = trimmed[1:]
    return "", True


def exit_status_from_returncode(returncode: int | None) -> TerminalExitStatus | None:
    if returncode is None:
        return None
    if returncode < 0:
        return TerminalExitStatus(exit_code=None, signal=str(-returncode))
    return TerminalExitStatus(exit_code=returncode, signal=None)
