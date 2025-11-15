# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import asyncio
from datetime import date
from typing import Any, Dict, List, Optional, Sequence, Union

import aiohttp

from .config import (
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_PART_SIZE_MB,
    DEFAULT_RPS,
    DEFAULT_MAX_VIDEOS,
    TIMEOUT_SECS,
)
from .utils import utc_iso_now
from .io_ndjson import NDJSONRotatingWriter
from .rate_limiter import RateLimiter
from .youtube_api import channels_list, search_list_videos_of_channel, videos_list_details

ChannelRef = Union[str, Sequence[str]]


async def _ingest_single_channel(
    session: aiohttp.ClientSession,
    limiter: RateLimiter,
    *,
    api_key: str,
    channel_id: str,
    out_dir_channels: str,
    out_dir_videos: str,
    part_size_mb: int,
    max_videos: int,
):
    w_channels = NDJSONRotatingWriter(out_dir_channels, part_size_mb)
    w_videos = NDJSONRotatingWriter(out_dir_videos, part_size_mb)

    # 1) channels.list
    await limiter.acquire()
    ch_info = await channels_list(session, api_key, channel_id=channel_id)
    for item in ch_info.get("items", []):
        w_channels.write_line({
            "_type": "youtube_channel",
            "ingestion_ts": utc_iso_now(),
            "source": "youtube_api_v3",
            "entity": "channels",
            "channelId": item.get("id"),
            "payload": item,
        })

    # 2) search.list (vídeos do canal)
    await limiter.acquire()
    video_ids = await search_list_videos_of_channel(
        session, api_key, channel_id, max_videos=max_videos, order="date"
    )

    if not video_ids:
        w_channels.write_line({
            "_type": "info",
            "ingestion_ts": utc_iso_now(),
            "source": "youtube_api_v3",
            "entity": "channels",
            "channelId": channel_id,
            "message": "no_videos_found_via_search_list",
        })
        w_channels.close()
        w_videos.close()
        return

    # 3) videos.list (detalhes)
    await limiter.acquire()
    details = await videos_list_details(session, api_key, video_ids)
    returned = set()
    for it in details:
        vid = it.get("id")
        returned.add(vid)
        w_videos.write_line({
            "_type": "youtube_video",
            "ingestion_ts": utc_iso_now(),
            "source": "youtube_api_v3",
            "entity": "videos",
            "channelId": channel_id,
            "videoId": vid,
            "payload": it,
        })

    # 4) auditoria: não retornados
    missing = [vid for vid in video_ids if vid not in returned]
    for vid in missing:
        w_videos.write_line({
            "_type": "not_found",
            "ingestion_ts": utc_iso_now(),
            "source": "youtube_api_v3",
            "entity": "videos",
            "channelId": channel_id,
            "videoId": vid,
        })

    w_channels.close()
    w_videos.close()


async def ingest_from_channel_reference(
    api_key: str,
    *,
    output_root: str = DEFAULT_OUTPUT_ROOT,
    ingestion_date: Optional[str] = None,
    rps: int = DEFAULT_RPS,
    part_size_mb: int = DEFAULT_PART_SIZE_MB,
    channel_id: Optional[ChannelRef] = None,
    for_username: Optional[str] = None,    # reservado (legacy)
    # reservado (descoberta por search de canal)
    handle_or_query: Optional[str] = None,
    max_videos: int = DEFAULT_MAX_VIDEOS,
):
    """
    Fluxo: channels.list → search.list (vídeos) → videos.list → grava NDJSON particionado.

    OBS: atualmente `channel_id` é obrigatório (pode ser str ou lista[str]).
         Descoberta por `for_username`/`handle_or_query` pode ser adicionada depois.
    """
    if not channel_id:
        raise RuntimeError(
            "Forneça ao menos `channel_id` (str ou lista de str).")

    ingestion_date = ingestion_date or date.today().isoformat()

    out_dir_channels = os.path.join(
        output_root, "youtube", "channels", f"ingestion_date={ingestion_date}")
    out_dir_videos = os.path.join(
        output_root, "youtube", "videos",   f"ingestion_date={ingestion_date}")

    limiter = RateLimiter(rps=rps)
    timeout = aiohttp.ClientTimeout(total=None, connect=TIMEOUT_SECS)
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)

    # Normaliza para lista
    channel_ids: List[str] = list(channel_id) if isinstance(
        channel_id, (list, tuple, set)) else [channel_id]

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [
            _ingest_single_channel(
                session, limiter,
                api_key=api_key,
                channel_id=cid,
                out_dir_channels=out_dir_channels,
                out_dir_videos=out_dir_videos,
                part_size_mb=part_size_mb,
                max_videos=max_videos,
            )
            for cid in channel_ids
        ]
        # Executa com concorrência controlada pelo RateLimiter (HTTP e API lidam com backoff)
        await asyncio.gather(*tasks)
