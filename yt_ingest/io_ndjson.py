# -*- coding: utf-8 -*-
import io
import os
import gzip
import json
from typing import Any, Dict

from .utils import ensure_dir, atomic_write_bytes


class NDJSONRotatingWriter:
    """NDJSON .gz com rotação aproximada por tamanho."""

    def __init__(self, out_dir: str, part_size_mb: int):
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
