"""
LiteLLM CustomLogger that injects Anthropic-style SSE ping frames
during idle periods of streaming responses.

Design doc: docs/superpowers/specs/2026-05-09-ping-injector-design.md

The callback must be referenced in config.yaml as
`callbacks.ping_injector.instance` — LiteLLM's `get_instance_fn` resolves
the dotted path via `getattr`, so the target must be a pre-built instance,
not the class.

We yield a pre-formatted SSE string (not a dict) so the SSE serializer
preserves the `event: ping` line — serializing a dict would drop it.
"""
import asyncio
import os
import time

from litellm.integrations.custom_logger import CustomLogger

from callbacks._route import (
    ANTHROPIC_MESSAGES,
    RESPONSES,
    classify_endpoint,
)

# Anthropic Messages API / Chat Completions — heartbeat as named event
_PING_FRAME = 'event: ping\ndata: {"type":"ping"}\n\n'
# OpenAI Responses API (Codex CLI) — SSE comment per WHATWG spec.
# eventsource_stream consumes comments before dispatch, so Codex's
# event-type match arms never see it. Bytes still flow on the wire,
# which resets Codex's idle-timeout watchdog.
_COMMENT_FRAME = ':\n\n'


class PingInjector(CustomLogger):
    def __init__(self):
        super().__init__()
        self.interval = float(os.environ.get("PING_INTERVAL_SECONDS", "30"))

    async def async_post_call_streaming_iterator_hook(
        self, user_api_key_dict, response, request_data
    ):
        endpoint = classify_endpoint(request_data)
        if endpoint == ANTHROPIC_MESSAGES:
            frame = _PING_FRAME
        elif endpoint == RESPONSES:
            frame = _COMMENT_FRAME
        else:
            # OpenAI-compatible Chat Completions and any other route:
            # do not inject. Their clients (e.g. line-based JSON.parse
            # consumers) cannot tolerate `event: ping` lines, and SSE
            # comments would not save them either. Pass through clean.
            async for chunk in response:
                yield chunk
            return

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
                await q.put(("ping", frame))

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
            for t in (tick_t, pump_t):
                try:
                    await t
                except BaseException:
                    pass


instance = PingInjector()
