"""TeamAuth — validates team_api_key and attaches it to outgoing requests."""

from __future__ import annotations


class TeamAuth:
    """Holds a team API key and generates auth headers for HTTP requests."""

    def __init__(self, api_key: str) -> None:
        if not api_key:
            raise ValueError("team_api_key must not be empty")
        self._api_key = api_key

    def headers(self) -> dict[str, str]:
        """Return HTTP headers that authenticate this client with the team server."""
        return {"Authorization": f"Bearer {self._api_key}"}
