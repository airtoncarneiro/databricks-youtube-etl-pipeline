# -*- coding: utf-8 -*-
import asyncio
import time


class RateLimiter:
    """Limite simples: até RPS por segundo (reset por janela deslizante de ~1s)."""

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
