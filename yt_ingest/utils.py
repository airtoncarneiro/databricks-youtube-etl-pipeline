"""Funções utilitárias para tempo, diretórios e escrita atômica."""

import os
import time


def utc_iso_now() -> str:
    """Retorna o timestamp UTC atual no formato ISO-8601 simples."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def ensure_dir(path: str) -> None:
    """Garante que o diretório exista, criando-o quando necessário."""
    os.makedirs(path, exist_ok=True)


def atomic_write_bytes(path_final: str, data: bytes) -> None:
    """Escreve bytes em arquivo de forma atômica usando arquivo temporário."""
    tmp = f"{path_final}.tmp"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path_final)
