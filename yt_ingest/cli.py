"""Interface de linha de comando para executar a ingestão."""

import os
import asyncio
import argparse

from .config import (
    DEFAULT_OUTPUT_ROOT, DEFAULT_RPS, DEFAULT_PART_SIZE_MB, DEFAULT_MAX_VIDEOS
)
from .pipeline import ingest_from_channel_reference


def _comma_or_space_list(value: str) -> list[str]:
    """Converte string separada por vírgula/espaço em lista de canais."""
    value = value.strip()
    if not value:
        return []
    if "," in value:
        return [v.strip() for v in value.split(",") if v.strip()]
    return [v.strip() for v in value.split() if v.strip()]


def build_parser() -> argparse.ArgumentParser:
    """Monta o parser de argumentos da CLI."""
    p = argparse.ArgumentParser(
        description="Ingestão de dados do YouTube para NDJSON particionado (.gz)")
    p.add_argument("--api-key", default=os.getenv("YOUTUBE_API_KEY"),
                   help="YouTube Data API v3 Key (ou env YOUTUBE_API_KEY)")
    p.add_argument(
        "--channel-id", help="ID(s) de canal, separado(s) por vírgula ou espaço")
    p.add_argument("--output-root", default=DEFAULT_OUTPUT_ROOT)
    p.add_argument("--ingestion-date", default=None,
                   help="AAAA-MM-DD (default: hoje)")
    p.add_argument("--rps", type=int, default=DEFAULT_RPS)
    p.add_argument("--part-size-mb", type=int, default=DEFAULT_PART_SIZE_MB)
    p.add_argument("--max-videos", type=int, default=DEFAULT_MAX_VIDEOS)
    return p


def main() -> None:
    """Ponto de entrada da CLI e disparo da ingestão assíncrona."""
    parser = build_parser()
    args = parser.parse_args()

    if not args.api_key:
        parser.error(
            "É necessário fornecer --api-key ou definir YOUTUBE_API_KEY.")

    channels = _comma_or_space_list(args.channel_id) if args.channel_id else []
    if not channels:
        parser.error("Forneça ao menos um --channel-id.")

    asyncio.run(
        ingest_from_channel_reference(
            api_key=args.api_key,
            output_root=args.output_root,
            ingestion_date=args.ingestion_date,
            rps=args.rps,
            part_size_mb=args.part_size_mb,
            channel_id=channels,
            max_videos=args.max_videos,
        )
    )


if __name__ == "__main__":
    main()
