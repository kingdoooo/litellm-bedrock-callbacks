# Ping Injector — Per-Client Routing

Status: design approved 2026-05-16

## Problem

`callbacks/ping_injector.py` injects an Anthropic-style named SSE event
(`event: ping\ndata: {"type":"ping"}\n\n`) on any streaming request whose
body carries `messages`. OpenAI-compatible Chat Completions clients also
send `messages` and parse SSE `data:` lines as JSON — they choke on the
`event: ping` line:

```
JSON parsing failed: Text: event: ping {"type":"ping"}.
Error message: Unexpected token 'e', "event: pin"... is not valid JSON
```

Existing discriminator `callbacks/_route.is_responses_api` only knows
two routes (Responses-API vs everything-else), so Anthropic Messages
and OpenAI Chat Completions are conflated.

## Goal

Three-way endpoint routing inside streaming-iterator callbacks:

| Endpoint | Client | Behavior |
|---|---|---|
| `/v1/messages` | Claude Code | inject `event: ping\ndata: {"type":"ping"}\n\n` |
| `/v1/responses` | Codex CLI | inject SSE comment `:\n\n` |
| `/v1/chat/completions` | OpenAI-compatible | **pass-through, no injection** |
| anything else | embeddings, etc. | pass-through |

Pass-through for Chat Completions is the user-confirmed choice. Rationale:
OpenAI's own `/v1/chat/completions` stream ships no heartbeat, OpenAI-
compatible clients are written against that contract, and the ping
injector exists to work around Claude Code's idle-timeout — it should not
leak into a generic OpenAI path. The reporting client uses line-based
`JSON.parse`, so SSE comments would not save it either.

## Discriminator

LiteLLM stamps `request_data["proxy_server_request"]["url"]` with the
full incoming URL in `litellm_pre_call_utils.py:1028-1034` before
streaming hooks run. Using the URL is more reliable than inspecting the
body shape (Anthropic Messages and Chat Completions both carry
`messages`). Path is matched by **suffix** so custom mounts like
`/anthropic/v1/messages` still classify correctly.

## Changes

### `callbacks/_route.py`

Replace the `is_responses_api(request_data) -> bool` predicate with a
classifier that returns one of four labels:

```python
ANTHROPIC_MESSAGES = "anthropic_messages"
CHAT_COMPLETIONS = "chat_completions"
RESPONSES = "responses"
OTHER = "other"

def classify_endpoint(request_data: dict) -> str:
    url = (request_data.get("proxy_server_request") or {}).get("url") or ""
    if url.endswith("/v1/responses"):
        return RESPONSES
    if url.endswith("/v1/messages"):
        return ANTHROPIC_MESSAGES
    if url.endswith("/v1/chat/completions"):
        return CHAT_COMPLETIONS
    return OTHER
```

The old `is_responses_api` is removed; nothing outside this repo imports
it.

### `callbacks/ping_injector.py`

Replace the binary frame-selection at line 40 with a three-way switch
that returns early for the non-injection cases:

```python
from callbacks._route import (
    classify_endpoint, ANTHROPIC_MESSAGES, RESPONSES,
)

endpoint = classify_endpoint(request_data)
if endpoint == ANTHROPIC_MESSAGES:
    frame = _PING_FRAME
elif endpoint == RESPONSES:
    frame = _COMMENT_FRAME
else:
    # Chat Completions and any other endpoint: stream untouched.
    async for chunk in response:
        yield chunk
    return
```

Rest of the pump/tick logic is unchanged.

### `callbacks/chunk_delayer.py`

Same classifier: only delay Anthropic Messages traffic. The current code
explicitly skips Responses-API; the new behavior also skips Chat
Completions (and anything else) so the test harness never perturbs
unrelated client streams.

```python
from callbacks._route import classify_endpoint, ANTHROPIC_MESSAGES

if classify_endpoint(request_data) != ANTHROPIC_MESSAGES:
    async for chunk in response:
        yield chunk
    return
```

## Out of scope

- No env switch to opt Chat Completions back into ping injection (YAGNI).
- No changes to `codex_sanitizer.py` — its `call_type`-based dispatch is
  independent and correct.
- No new automated test fixtures; container-level smoke covers it.

## Verification

With `PING_INTERVAL_SECONDS=2` and `CHUNK_DELAY_SECONDS=5` set, run a
streaming request against each endpoint and inspect the raw SSE bytes:

1. `/v1/messages` (Anthropic Messages) — stream contains
   `event: ping\ndata: {"type":"ping"}` lines.
2. `/v1/responses` (Codex) — stream contains `:\n\n` comment lines.
3. `/v1/chat/completions` (OpenAI-compatible) — stream contains
   **neither** marker; only upstream chunks.
4. Re-run the original failing OpenAI-compatible client; the
   `JSON parsing failed: ... event: pin...` error must not recur.
