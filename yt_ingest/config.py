"""Constantes de configuração padrão da ingestão do YouTube."""

YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3"
TIMEOUT_SECS = 30
MAX_RETRIES = 5
DEFAULT_RPS = 8
BATCH_SIZE_IDS = 50              # videos.list aceita até 50
DEFAULT_PART_SIZE_MB = 32
DEFAULT_OUTPUT_ROOT = "./raw"
# quantos vídeos coletar por canal (via search.list)
DEFAULT_MAX_VIDEOS = 50
