"""
LiteLLM CustomLogger that injects Anthropic-style SSE ping frames
during idle periods of streaming responses.

Design doc: docs/superpowers/specs/2026-05-09-ping-injector-design.md
"""
import asyncio
import os
import time

from litellm.integrations.custom_logger import CustomLogger


class PingInjector(CustomLogger):
    def __init__(self):
        super().__init__()
        self.interval = float(os.environ.get("PING_INTERVAL_SECONDS", "30"))

    async def async_post_call_streaming_iterator_hook(
        self, user_api_key_dict, response, request_data
    ):
        q: asyncio.Queue = asyncio.Queue()

        async def pump():
            try:
                async for chunk in response:
                    await q.put(("item", chunk))
            except Exception as exc:
                await q.put(("error", exc))
            else:
                await q.put(("done", None))

        async def tick():
            while True:
                await asyncio.sleep(self.interval)
                await q.put(("ping", {"type": "ping"}))

        pump_t = asyncio.create_task(pump())
        tick_t = asyncio.create_task(tick())
        last_yield = time.monotonic()
        try:
            while True:
                kind, v = await q.get()
                if kind == "done":
                    return
                if kind == "error":
                    raise v
                if kind == "ping":
                    if time.monotonic() - last_yield < self.interval:
                        continue
                yield v
                last_yield = time.monotonic()
        finally:
            tick_t.cancel()
            pump_t.cancel()
