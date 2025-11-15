# -*- coding: utf-8 -*-
from typing import Any, Dict
import asyncio
import aiohttp

from .config import MAX_RETRIES, TIMEOUT_SECS


async def http_get_json(session: aiohttp.ClientSession, url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, params=params, timeout=TIMEOUT_SECS) as r:
                if r.status in (429, 500, 502, 503, 504):
                    await asyncio.sleep((2 ** attempt) + 0.1 * attempt)
                    continue
                r.raise_for_status()
                return await r.json()
        except Exception:
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep((2 ** attempt) + 0.2 * attempt)
    return {}
