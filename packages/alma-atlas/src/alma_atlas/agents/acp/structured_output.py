"""Structured-output prompting helpers for ACP-backed workflows."""

from __future__ import annotations

import json
import logging
import tempfile
import uuid
from pathlib import Path

from acp import text_block
from pydantic import BaseModel

from .client import SimpleClient
from .common import MAX_RETRIES

logger = logging.getLogger(__name__)

async def analyze_structured_output[T: BaseModel](
    *,
    conn,
    client: SimpleClient,
    session_id: str,
    system_prompt: str,
    user_prompt: str,
    response_schema: type[T],
) -> T:
    """Run one structured-output exchange over ACP and validate the result."""
    tmp_path = Path(tempfile.gettempdir()) / f"atlas-learn-{uuid.uuid4().hex}.json"
    schema_json = json.dumps(response_schema.model_json_schema(), indent=2)

    initial_prompt = (
        f"{system_prompt}\n\n"
        f"{user_prompt}\n\n"
        f"Write your results as valid JSON to: {tmp_path}\n\n"
        f"The JSON must conform to this schema:\n{schema_json}\n\n"
        "Write ONLY valid JSON to the file. No markdown, no comments, no wrapping."
    )

    last_error: Exception | None = None

    for attempt in range(MAX_RETRIES):
        if attempt == 0:
            prompt_text = initial_prompt
        else:
            prompt_text = (
                f"The file you wrote to {tmp_path} has validation errors:\n"
                f"{last_error}\n\n"
                "Fix the file so it passes validation. "
                "Write the corrected JSON to the same path."
            )

        client.clear_written_file(str(tmp_path))

        logger.debug(
            "ACP structured output: sending prompt (attempt %d/%d)",
            attempt + 1,
            MAX_RETRIES,
        )
        await conn.prompt(
            session_id=session_id,
            prompt=[text_block(prompt_text)],
        )

        content = client.get_written_file(str(tmp_path))
        if content is None and tmp_path.exists():
            content = tmp_path.read_text()

        if content is None:
            last_error = ValueError(f"Agent did not write output to {tmp_path}")
            logger.warning(
                "ACP structured output: attempt %d: agent did not write output file",
                attempt + 1,
            )
            continue

        try:
            result = response_schema.model_validate_json(content)
            client.clear_written_file(str(tmp_path))
            tmp_path.unlink(missing_ok=True)
            logger.debug(
                "ACP structured output: attempt %d: validation succeeded",
                attempt + 1,
            )
            return result
        except Exception as exc:
            last_error = exc
            logger.warning(
                "ACP structured output: attempt %d: validation failed: %s",
                attempt + 1,
                exc,
            )

    client.clear_written_file(str(tmp_path))
    tmp_path.unlink(missing_ok=True)
    raise ValueError(
        f"ACPProvider: failed to get valid response after {MAX_RETRIES} attempts. "
        f"Last error: {last_error}"
    ) from last_error
