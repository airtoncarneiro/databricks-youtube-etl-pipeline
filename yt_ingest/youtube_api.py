"""Wrappers assíncronos para endpoints da YouTube Data API v3."""

from typing import Any, Dict, List, Optional
import aiohttp

from .config import YOUTUBE_API_URL, BATCH_SIZE_IDS
from .http_client import http_get_json


async def channels_list(
    session: aiohttp.ClientSession,
    api_key: str,
    *,
    channel_id: Optional[str] = None,
    for_username: Optional[str] = None,
    parts: str = "brandingSettings,id,snippet,statistics",
) -> Dict[str, Any]:
    """Consulta metadados de canais via endpoint `channels.list`."""
    url = f"{YOUTUBE_API_URL}/channels"
    params: Dict[str, Any] = {"part": parts, "key": api_key}
    if channel_id:
        params["id"] = channel_id
    if for_username:
        params["forUsername"] = for_username
    return await http_get_json(session, url, params)


async def search_list_videos_of_channel(
    session: aiohttp.ClientSession,
    api_key: str,
    channel_id: str,
    *,
    max_videos: int,
    order: str = "date",
) -> List[str]:
    """Lista IDs de vídeos de um canal via `search.list`."""
    url = f"{YOUTUBE_API_URL}/search"
    video_ids: List[str] = []
    page_token: Optional[str] = None

    while len(video_ids) < max_videos:
        params: Dict[str, Any] = {
            "part": "id",
            "channelId": channel_id,
            "type": "video",
            "order": order,
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


async def videos_list_details(
    session: aiohttp.ClientSession,
    api_key: str,
    video_ids: List[str],
    *,
    parts: str = (
        "contentDetails,id,liveStreamingDetails,localizations,"
        "recordingDetails,snippet,statistics,status,topicDetails"
    ),
) -> List[Dict[str, Any]]:
    """Busca detalhes de vídeos em lotes via endpoint `videos.list`."""
    if not video_ids:
        return []

    url = f"{YOUTUBE_API_URL}/videos"
    out: List[Dict[str, Any]] = []
    for i in range(0, len(video_ids), BATCH_SIZE_IDS):
        chunk = video_ids[i : i + BATCH_SIZE_IDS]
        params = {"id": ",".join(chunk), "part": parts, "key": api_key}
        data = await http_get_json(session, url, params)
        out.extend(data.get("items", []))
    return out
