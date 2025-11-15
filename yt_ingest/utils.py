# -*- coding: utf-8 -*-
import os
import time


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
