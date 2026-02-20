"""Cliente HTTP assíncrono com retry e backoff exponencial."""

import asyncio
from typing import Any, Mapping

import aiohttp

from .config import MAX_RETRIES, TIMEOUT_SECS

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=TIMEOUT_SECS)


async def http_get_json(
    session: aiohttp.ClientSession,
    url: str,
    params: Mapping[str, Any],
) -> dict[str, Any]:
    """Executa GET JSON com retry para erros transitórios."""
    if MAX_RETRIES <= 0:
        raise ValueError("MAX_RETRIES deve ser maior que zero.")

    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT,
            ) as response:
                if response.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep((2 ** attempt) + 0.1 * attempt)
                    continue

                response.raise_for_status()
                return await response.json()
        except (aiohttp.ClientError, asyncio.TimeoutError):
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep((2 ** attempt) + 0.2 * attempt)
    raise RuntimeError("Falha inesperada ao executar requisição HTTP.")
