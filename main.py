#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import io
import gzip
import json
import time
import asyncio
from datetime import date
from typing import List, Dict, Any, Optional

import aiohttp

# ===================== Config =====================
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3"
TIMEOUT_SECS = 30
MAX_RETRIES = 5
DEFAULT_RPS = 8
BATCH_SIZE_IDS = 50          # videos.list aceita até 50
DEFAULT_PART_SIZE_MB = 32
DEFAULT_OUTPUT_ROOT = "./raw"
# quantos vídeos coletar por canal (via search.list)
DEFAULT_MAX_VIDEOS = 50

# ===================== Utilidades =====================


def utc_iso_now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def atomic_write_bytes(path_final: str, data: bytes):
    tmp = f"{path_final}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path_final)


class NDJSONRotatingWriter:
    """NDJSON .gz com rotação aproximada por tamanho."""

    def __init__(self, out_dir: str, part_size_mb: int = DEFAULT_PART_SIZE_MB):
        self.out_dir = out_dir
        ensure_dir(out_dir)
        self.target_bytes = part_size_mb * 1024 * 1024
        self.buf = io.BytesIO()
        self.file_index = 0
        self.count = 0

    def _rotate(self):
        if self.buf.tell() == 0:
            return
        compressed = gzip.compress(self.buf.getvalue())
        path = os.path.join(
            self.out_dir, f"part-{self.file_index:05d}.ndjson.gz")
        atomic_write_bytes(path, compressed)
        self.file_index += 1
        self.buf = io.BytesIO()

    def write_line(self, obj: Dict[str, Any]):
        line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
        self.buf.write(line)
        self.count += 1
        if self.buf.tell() >= self.target_bytes:
            self._rotate()

    def close(self):
        self._rotate()


class RateLimiter:
    """Limite simples: até RPS por segundo."""

    def __init__(self, rps: int):
        self.rps = max(1, rps)
        self.sem = asyncio.Semaphore(self.rps)
        self.last_reset = time.monotonic()

    async def acquire(self):
        now = time.monotonic()
        if now - self.last_reset >= 1.0:
            self.sem = asyncio.Semaphore(self.rps)
            self.last_reset = now
        await self.sem.acquire()

# ===================== HTTP com retries =====================


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

# ===================== YouTube: endpoints permitidos =====================


async def channels_list(session: aiohttp.ClientSession, api_key: str, *,
                        channel_id: Optional[str] = None,
                        for_username: Optional[str] = None,
                        parts: str = "brandingSettings, id, snippet, statistics") -> Dict[str, Any]:
    """youtube.channels().list"""
    url = f"{YOUTUBE_API_URL}/channels"
    params: Dict[str, Any] = {"part": parts, "key": api_key}
    if channel_id:
        params["id"] = channel_id
    if for_username:
        params["forUsername"] = for_username
    return await http_get_json(session, url, params)


async def search_list_channels(session: aiohttp.ClientSession, api_key: str, query: str, max_results: int = 5) -> Optional[str]:
    """Usa youtube.search().list (type=channel) para descobrir channelId a partir de handle/nome."""
    url = f"{YOUTUBE_API_URL}/search"
    params = {
        "part": "snippet",
        "q": query,
        "type": "channel",
        "maxResults": max_results,
        "key": api_key,
    }
    data = await http_get_json(session, url, params)
    for it in data.get("items", []):
        cid = it.get("snippet", {}).get("channelId")
        if cid:
            return cid
    return None


async def search_list_videos_of_channel(session: aiohttp.ClientSession, api_key: str, channel_id: str,
                                        max_videos: int = DEFAULT_MAX_VIDEOS, order: str = "date") -> List[str]:
    """youtube.search().list para listar vídeos de um canal (sem playlist)."""
    url = f"{YOUTUBE_API_URL}/search"
    video_ids: List[str] = []
    page_token = None
    while len(video_ids) < max_videos:
        params = {
            "part": "id",
            "channelId": channel_id,
            "type": "video",
            "order": order,               # "date" = mais recentes
            "maxResults": min(50, max_videos - len(video_ids)),
            "key": api_key,
        }
        if page_token:
            params["pageToken"] = page_token
        data = await http_get_json(session, url, params)
        for it in data.get("items", []):
            if it.get("id", {}).get("kind") == "youtube#video":
                vid = it.get("id", {}).get("videoId")
                if vid:
                    video_ids.append(vid)
        page_token = data.get("nextPageToken")
        if not page_token:
            break
    return video_ids


async def videos_list_details(session: aiohttp.ClientSession, api_key: str, video_ids: List[str],
                              parts: str = "contentDetails,id,liveStreamingDetails,localizations,recordingDetails,snippet,statistics,status,topicDetails") -> List[Dict[str, Any]]:
    """youtube.videos().list em lote (até 50 IDs por chamada)."""
    if not video_ids:
        return []
    url = f"{YOUTUBE_API_URL}/videos"
    out: List[Dict[str, Any]] = []
    for i in range(0, len(video_ids), BATCH_SIZE_IDS):
        chunk = video_ids[i:i + BATCH_SIZE_IDS]
        params = {"id": ",".join(chunk), "part": parts, "key": api_key}
        data = await http_get_json(session, url, params)
        out.extend(data.get("items", []))
    return out

# ===================== Pipeline =====================


async def ingest_from_channel_reference(
    api_key: str,
    output_root: str = DEFAULT_OUTPUT_ROOT,
    ingestion_date: Optional[str] = None,
    rps: int = DEFAULT_RPS,
    part_size_mb: int = DEFAULT_PART_SIZE_MB,
    # formas de referenciar o canal (escolha UMA):
    channel_id: Optional[str] = None,
    for_username: Optional[str] = None,
    # ex.: "@portaldojosé" ou "Portal do José"
    handle_or_query: Optional[str] = None,
    max_videos: int = DEFAULT_MAX_VIDEOS,
):
    """Fluxo: channels.list → search.list (vídeos) → videos.list → grava NDJSON particionado."""
    ingestion_date = ingestion_date or date.today().isoformat()

    out_dir_channels = os.path.join(
        output_root, "youtube", "channels", f"ingestion_date={ingestion_date}")
    out_dir_videos = os.path.join(
        output_root, "youtube", "videos",   f"ingestion_date={ingestion_date}")
    w_channels = NDJSONRotatingWriter(out_dir_channels, part_size_mb)
    w_videos = NDJSONRotatingWriter(out_dir_videos, part_size_mb)

    limiter = RateLimiter(rps=rps)
    timeout = aiohttp.ClientTimeout(total=None, connect=TIMEOUT_SECS)
    connector = aiohttp.TCPConnector(limit=60, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        # 1) Resolver e obter informações do canal via channels.list
        #    (usa channel_id/for_username; se não houver, tenta achar via search.list de canais com o handle/termo)
        resolved_channel_id = channel_id
        if not resolved_channel_id:
            if for_username:
                await limiter.acquire()
                ch = await channels_list(session, api_key, for_username=for_username)
                items = ch.get("items", [])
                if items:
                    resolved_channel_id = items[0]["id"]
            if not resolved_channel_id and handle_or_query:
                # tentar encontrar o canal com search.list (type=channel) usando o handle/termo
                await limiter.acquire()
                maybe = await search_list_channels(session, api_key, handle_or_query, max_results=3)
                if maybe:
                    resolved_channel_id = maybe

        if not resolved_channel_id:
            raise RuntimeError(
                "Não foi possível resolver o channelId (forneça channel_id, for_username ou handle_or_query).")

        # channels.list final (pegar info principal do canal)
        await limiter.acquire()
        ch_info = await channels_list(session, api_key, channel_id=resolved_channel_id)
        # registrar na raw/channels
        for item in ch_info.get("items", []):
            w_channels.write_line({
                "_type": "youtube_channel",
                "ingestion_ts": utc_iso_now(),
                "source": "youtube_api_v3",
                "entity": "channels",
                "channelId": item.get("id"),
                "payload": item
            })

        # 2) search.list (vídeos do canal)
        await limiter.acquire()
        video_ids = await search_list_videos_of_channel(session, api_key, resolved_channel_id, max_videos=max_videos, order="date")

        if not video_ids:
            w_channels.write_line({
                "_type": "info",
                "ingestion_ts": utc_iso_now(),
                "source": "youtube_api_v3",
                "entity": "channels",
                "channelId": resolved_channel_id,
                "message": "no_videos_found_via_search_list"
            })
            w_channels.close()
            w_videos.close()
            return

        # 3) videos.list (detalhes) em lotes
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
                "channelId": resolved_channel_id,
                "videoId": vid,
                "payload": it
            })

        # marcar não retornados (auditoria)
        missing = [vid for vid in video_ids if vid not in returned]
        for vid in missing:
            w_videos.write_line({
                "_type": "not_found",
                "ingestion_ts": utc_iso_now(),
                "source": "youtube_api_v3",
                "entity": "videos",
                "channelId": resolved_channel_id,
                "videoId": vid
            })

    w_channels.close()
    w_videos.close()

# ===================== Execução direta (sem CLI) =====================
if __name__ == "__main__":
    # ↳ edite aqui para seu teste local (sem precisar de terminal/flags)
    API_KEY = os.getenv("YOUTUBE_API_KEY")

    # Exemplo 1: já tenho o channel_id
    TEST_CHANNEL_ID = ["UCELku_rf-FHbWhLIwsKLGGA",      # Portal do José
                       "UCVHFbqXqoYvEWM1Ddxl0QDg"]      # Amazon

    # Exemplo 2 (opcional): handle/termo (caso não tenha o id)
    # pode ser "@..." ou "Portal do José"
    TEST_HANDLE_OR_QUERY = "@portaldojosé"

    asyncio.run(
        ingest_from_channel_reference(
            api_key=API_KEY,
            output_root=DEFAULT_OUTPUT_ROOT,
            ingestion_date=None,          # usa data de hoje
            rps=6,
            part_size_mb=DEFAULT_PART_SIZE_MB,
            channel_id=TEST_CHANNEL_ID,   # prioridade: usa este se existir
            for_username=None,            # legacy (usuários antigos)
            handle_or_query=TEST_HANDLE_OR_QUERY,
            max_videos=50,                # quantos vídeos buscar via search.list
        )
    )
