"""
Endpoint classifier for LiteLLM streaming-iterator callbacks.

LiteLLM stamps the full incoming URL into
`request_data["proxy_server_request"]["url"]` (see
litellm/proxy/litellm_pre_call_utils.py:1028) before any streaming hook
runs. We classify by URL path suffix instead of by body shape because
Anthropic Messages and OpenAI Chat Completions both carry a `messages`
field — body shape alone cannot distinguish them.

`endswith` on the path is intentional: LiteLLM also exposes prefixed
mounts like `/anthropic/v1/messages`, and a suffix match keeps the
classifier robust to that.

Leading underscore marks this module as internal — it is not a
registered callback dotted path.
"""

ANTHROPIC_MESSAGES = "anthropic_messages"
CHAT_COMPLETIONS = "chat_completions"
RESPONSES = "responses"
OTHER = "other"


def classify_endpoint(request_data: dict) -> str:
    psr = request_data.get("proxy_server_request") if isinstance(request_data, dict) else None
    url = (psr or {}).get("url") or ""
    if url.endswith("/v1/responses"):
        return RESPONSES
    if url.endswith("/v1/messages"):
        return ANTHROPIC_MESSAGES
    if url.endswith("/v1/chat/completions"):
        return CHAT_COMPLETIONS
    return OTHER
