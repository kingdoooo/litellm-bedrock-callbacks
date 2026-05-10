"""
Test-only CustomLogger that delays the FIRST chunk of a streaming response
by CHUNK_DELAY_SECONDS seconds. All subsequent chunks are forwarded
immediately. Used to simulate a slow Bedrock TTFB (time-to-first-byte) so
that downstream callbacks (e.g. PingInjector) and clients (CC) can be
tested against a deterministic long idle gap.

Default CHUNK_DELAY_SECONDS=0 → pass-through, no-op.

Callback order in config.yaml MUST be:
    - callbacks.chunk_delayer.instance
    - callbacks.ping_injector.instance
so that PingInjector sees the delayed upstream.
"""
import asyncio
import os

from litellm.integrations.custom_logger import CustomLogger


class ChunkDelayer(CustomLogger):
    def __init__(self):
        super().__init__()
        self.delay = float(os.environ.get("CHUNK_DELAY_SECONDS", "0"))

    async def async_post_call_streaming_iterator_hook(
        self, user_api_key_dict, response, request_data
    ):
        if self.delay <= 0:
            async for chunk in response:
                yield chunk
            return

        first = True
        async for chunk in response:
            if first:
                await asyncio.sleep(self.delay)
                first = False
            yield chunk


instance = ChunkDelayer()
