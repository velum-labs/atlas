"""Shared HTTP retry helpers for Atlas application services."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

DEFAULT_RETRY_STATUS_CODES = frozenset({429, 500, 502, 503, 504})


async def async_request_with_retry(
    client: httpx.AsyncClient,
    *,
    method: str,
    url: str,
    headers: dict[str, str],
    logger: logging.Logger,
    request_name: str,
    json_body: dict[str, Any] | None = None,
    params: dict[str, Any] | None = None,
    max_retries: int = 3,
    backoff_base: float = 1.0,
    retry_status_codes: frozenset[int] = DEFAULT_RETRY_STATUS_CODES,
) -> httpx.Response:
    """Perform one HTTP request with retry/backoff handling."""
    last_exc: Exception | None = None
    method_name = method.upper()
    for attempt in range(max_retries + 1):
        try:
            request_method = getattr(client, method_name.lower(), None)
            request_kwargs: dict[str, Any] = {"headers": headers}
            if json_body is not None:
                request_kwargs["json"] = json_body
            if params:
                request_kwargs["params"] = params
            if callable(request_method):
                response = await request_method(url, **request_kwargs)
            else:
                response = await client.request(
                    method=method_name,
                    url=url,
                    **request_kwargs,
                )
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            if attempt < max_retries:
                wait = backoff_base * (2**attempt)
                logger.debug(
                    "[http] %s %s network error (attempt %d/%d), retrying in %.1fs: %s",
                    method_name,
                    request_name,
                    attempt + 1,
                    max_retries,
                    wait,
                    exc,
                )
                await asyncio.sleep(wait)
                continue
            raise

        if response.status_code == 429:
            retry_after = float(response.headers.get("Retry-After", backoff_base * (2**attempt)))
            logger.warning(
                "[http] %s %s rate limited (attempt %d/%d), retrying in %.1fs",
                method_name,
                request_name,
                attempt + 1,
                max_retries,
                retry_after,
            )
            await asyncio.sleep(retry_after)
            continue

        if response.status_code in retry_status_codes and attempt < max_retries:
            wait = backoff_base * (2**attempt)
            logger.warning(
                "[http] %s %s HTTP %d (attempt %d/%d), retrying in %.1fs",
                method_name,
                request_name,
                response.status_code,
                attempt + 1,
                max_retries,
                wait,
            )
            await asyncio.sleep(wait)
            continue

        response.raise_for_status()
        return response

    if last_exc is not None:
        raise last_exc
    request = httpx.Request(method_name, url)
    response = httpx.Response(599, request=request)
    raise httpx.HTTPStatusError(
        message=f"Max retries exceeded for {method_name} {request_name}",
        request=request,
        response=response,
    )
